package video

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"gorm.io/gorm/clause"
	"strings"

	"gorm.io/gorm"
)

type CommentRepository struct {
	db *gorm.DB
}

func NewCommentRepository(db *gorm.DB) *CommentRepository {
	return &CommentRepository{db: db}
}

func (r *CommentRepository) CreateComment(ctx context.Context, comment *Comment) error {
	return r.db.WithContext(ctx).Create(comment).Error
}

func (r *CommentRepository) DeleteComment(ctx context.Context, comment *Comment) error {
	return r.db.WithContext(ctx).Delete(comment).Error
}

func (r *CommentRepository) GetAllComments(ctx context.Context, videoID uint) ([]Comment, error) {
	var comments []Comment
	err := r.db.WithContext(ctx).
		Where("video_id = ?", videoID).
		Order("created_at asc").
		Limit(200).
		Find(&comments).Error
	return comments, err
}

func (r *CommentRepository) IsExist(ctx context.Context, id uint) (bool, error) {
	var comment Comment
	if err := r.db.WithContext(ctx).First(&comment, id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *CommentRepository) GetByID(ctx context.Context, id uint) (*Comment, error) {
	var comment Comment
	if err := r.db.WithContext(ctx).First(&comment, id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &comment, nil
}

func (r *CommentRepository) ApplyCommentEvent(ctx context.Context, evt rabbitmq.CommentEvent) error {
	if evt.Action != "publish" && evt.Action != "delete" {
		return errors.New("invalid comment action")
	}
	return consumeEvent(r.db, ctx, "comment", evt.EventID, func(tx *gorm.DB) error {
		if evt.Action == "delete" {
			if evt.CommentID == 0 {
				return errors.New("comment_id is required")
			}
			return tx.Delete(&Comment{}, evt.CommentID).Error
		}
		if evt.VideoID == 0 || evt.AuthorID == 0 || strings.TrimSpace(evt.Content) == "" {
			return errors.New("invalid comment event")
		}
		var v Video
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&v, evt.VideoID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		c := Comment{Username: strings.TrimSpace(evt.Username), VideoID: evt.VideoID,
			AuthorID: evt.AuthorID, Content: strings.TrimSpace(evt.Content), CreatedAt: evt.OccurredAt}
		if err := tx.Create(&c).Error; err != nil {
			return err
		}
		if err := tx.Model(&Video{}).Where("id = ?", evt.VideoID).
			UpdateColumn("popularity", gorm.Expr("popularity + 1")).Error; err != nil {
			return err
		}
		return enqueuePopularity(tx, "comment", evt.EventID, evt.VideoID, 1, evt.OccurredAt)
	})
}
