package video

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"feedsystem_video_go/internal/apierror"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"gorm.io/gorm"
)

type VideoService struct {
	repo         *VideoRepository
	cache        *rediscache.Client
	cacheTTL     time.Duration
	popularityMQ *rabbitmq.PopularityMQ
}

func NewVideoService(repo *VideoRepository, cache *rediscache.Client, popularityMQ *rabbitmq.PopularityMQ) *VideoService {
	return &VideoService{repo: repo, cache: cache, cacheTTL: 5 * time.Minute, popularityMQ: popularityMQ}
}

func (vs *VideoService) Publish(ctx context.Context, video *Video) error {
	if video == nil {
		return errors.New("video is nil")
	}
	video.Title = strings.TrimSpace(video.Title)
	video.PlayURL = strings.TrimSpace(video.PlayURL)
	video.CoverURL = strings.TrimSpace(video.CoverURL)

	if video.Title == "" {
		return errors.New("title is required")
	}
	if video.PlayURL == "" {
		return errors.New("play url is required")
	}
	if video.CoverURL == "" {
		return errors.New("cover url is required")
	}

	//事务保证视频写入库和消息写入本地消息表的一致性
	err := vs.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(video).Error; err != nil {
			return err
		}

		msg := OutboxMsg{
			VideoID:    video.ID,
			EventType:  "video_published",
			Status:     "pending",
			CreateTime: video.CreateTime,
		}

		if err := tx.Create(&msg).Error; err != nil {
			return err
		}

		tags := ExtractTags(video.Title + " " + video.Description)
		for _, tagName := range tags {
			var tag Tag
			if err := tx.Where("name = ?", tagName).FirstOrCreate(&tag, Tag{Name: tagName}).Error; err != nil {
				return err
			}
			if err := tx.Create(&VideoTag{VideoID: video.ID, TagID: tag.ID}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return err

}

func (vs *VideoService) Delete(ctx context.Context, id uint, authorID uint) error {
	video, err := vs.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if video == nil {
		return errors.New("video not found")
	}
	if video.AuthorID != authorID {
		return apierror.ErrUnauthorized
	}
	if err := vs.repo.DeleteVideo(ctx, id); err != nil {
		return err
	}
	if vs.cache != nil {
		cacheKey := vs.cache.Key("video:detail:id=%d", id)
		_ = vs.cache.Del(context.Background(), cacheKey)
	}
	return nil
}

func (vs *VideoService) ListByAuthorID(ctx context.Context, authorID uint) ([]Video, error) {
	videos, err := vs.repo.ListByAuthorID(ctx, int64(authorID))
	if err != nil {
		return nil, err
	}
	return videos, nil
}

func (vs *VideoService) GetDetail(ctx context.Context, id uint) (*Video, error) {
	cacheKey := vs.cache.Key("video:detail:id=%d", id)
	getCached := func() (*Video, bool) {
		opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		b, err := vs.cache.GetBytes(opCtx, cacheKey)
		if err != nil {
			return nil, false
		}
		var cached Video
		if json.Unmarshal(b, &cached) != nil {
			return nil, false
		}
		return &cached, true
	}
	load := func() (*Video, error) {
		versionCtx, versionCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		generation, versionErr := vs.cache.VideoGeneration(versionCtx, id)
		versionCancel()
		v, err := vs.repo.GetByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if versionErr == nil {
			if b, err := json.Marshal(v); err == nil {
				setCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
				_, _ = vs.cache.SetVideoBytes(setCtx, id, cacheKey, generation, b, vs.cacheTTL)
			}
		}
		return v, nil
	}
	if vs.cache == nil {
		return vs.repo.GetByID(ctx, id)
	}
	if v, ok := getCached(); ok {
		return v, nil
	}
	lockCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	token, locked, err := vs.cache.Lock(lockCtx, "lock:"+cacheKey, 2*time.Second)
	cancel()
	if err == nil && locked {
		defer func() {
			unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer unlockCancel()
			_ = vs.cache.Unlock(unlockCtx, "lock:"+cacheKey, token)
		}()
		if v, ok := getCached(); ok {
			return v, nil
		}
		return load()
	}
	if err == nil {
		for i := 0; i < 5; i++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			if v, ok := getCached(); ok {
				return v, nil
			}
		}
	}
	return load()
}

func (vs *VideoService) UpdateLikesCount(ctx context.Context, id uint, likesCount int64) error {
	if err := vs.repo.UpdateLikesCount(ctx, id, likesCount); err != nil {
		return err
	}
	return nil
}

func (vs *VideoService) UpdatePopularity(ctx context.Context, id uint, change int64) error {
	eventID, err := rabbitmq.NewEventID()
	if err != nil {
		return err
	}
	return vs.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&Video{}).Where("id = ?", id).
			UpdateColumn("popularity", gorm.Expr("popularity + ?", change))
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return errors.New("video not found")
		}
		if change == 0 {
			return nil
		}
		return enqueuePopularity(tx, "video", eventID, id, change, time.Now().UTC())
	})
}
