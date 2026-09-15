package followfeed

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"golang.org/x/sync/singleflight"
	"gorm.io/gorm"
)

type Options struct {
	CelebrityThreshold                    int64
	Capacity, FanoutBatch, MaxPullSources int
}

func DefaultOptions() Options {
	o := Options{100000, 1000, 256, 32}
	if n, err := strconv.ParseInt(os.Getenv("FEED_CELEBRITY_THRESHOLD"), 10, 64); err == nil && n > 0 {
		o.CelebrityThreshold = n
	}
	return o
}

type Service struct {
	db      *gorm.DB
	cache   *rediscache.Client
	opts    Options
	rebuild singleflight.Group
}

func New(db *gorm.DB, cache *rediscache.Client, opts Options) *Service {
	defaults := DefaultOptions()
	if opts.CelebrityThreshold <= 0 {
		opts.CelebrityThreshold = defaults.CelebrityThreshold
	}
	if opts.Capacity <= 0 {
		opts.Capacity = defaults.Capacity
	}
	if opts.FanoutBatch <= 0 {
		opts.FanoutBatch = defaults.FanoutBatch
	}
	if opts.MaxPullSources <= 0 {
		opts.MaxPullSources = defaults.MaxPullSources
	}
	return &Service{db: db, cache: cache, opts: opts}
}
func (s *Service) InboxKey(user uint) string    { return s.cache.Key("feed:inbox:%d", user) }
func (s *Service) AuthorKey(author uint) string { return s.cache.Key("feed:user_videos:%d", author) }

func applyCursor(q *gorm.DB, c Cursor) *gorm.DB {
	if c.Time.IsZero() {
		return q
	}
	return q.Where("create_time < ? OR (create_time = ? AND id < ?)", c.Time, c.Time, c.ID)
}
func (s *Service) query(ctx context.Context, authors []uint, c Cursor, limit int) ([]*video.Video, error) {
	if len(authors) == 0 {
		return []*video.Video{}, nil
	}
	var rows []*video.Video
	err := applyCursor(s.db.WithContext(ctx).Model(&video.Video{}).Where("author_id IN ?", authors), c).
		Order("create_time DESC, id DESC").Limit(limit).Find(&rows).Error
	return rows, err
}
func (s *Service) fromDB(ctx context.Context, user uint, c Cursor, limit int) ([]*video.Video, error) {
	follows := s.db.Model(&social.Social{}).Select("vlogger_id").Where("follower_id = ?", user)
	var rows []*video.Video
	err := applyCursor(s.db.WithContext(ctx).Model(&video.Video{}).Where("author_id IN (?)", follows), c).
		Order("create_time DESC, id DESC").Limit(limit).Find(&rows).Error
	return rows, err
}

// Read classifies using current DB relationships, so unfollows and threshold
// crossings invalidate the inbox signature without trusting stale social caches.
func (s *Service) Read(ctx context.Context, user uint, c Cursor, limit int) ([]*video.Video, error) {
	if user == 0 {
		return []*video.Video{}, nil
	}
	if limit < 1 || limit > 51 {
		return nil, errors.New("invalid following limit")
	}
	if s.cache == nil {
		return s.fromDB(ctx, user, c, limit)
	}
	var authors []uint
	if err := s.db.WithContext(ctx).Model(&social.Social{}).Where("follower_id = ?", user).Order("vlogger_id").Pluck("vlogger_id", &authors).Error; err != nil {
		return nil, err
	}
	if len(authors) == 0 {
		return []*video.Video{}, nil
	}
	type authorCount struct {
		VloggerID uint
		Total     int64
	}
	var counts []authorCount
	if err := s.db.WithContext(ctx).Model(&social.Social{}).Select("vlogger_id, COUNT(*) AS total").Where("vlogger_id IN ?", authors).Group("vlogger_id").Scan(&counts).Error; err != nil {
		return nil, err
	}
	big := make(map[uint]bool)
	for _, row := range counts {
		big[row.VloggerID] = row.Total >= s.opts.CelebrityThreshold
	}
	var normal, celebrities []uint
	for _, a := range authors {
		if big[a] {
			celebrities = append(celebrities, a)
		} else {
			normal = append(normal, a)
		}
	}
	// Bound per-request read amplification instead of starting unbounded goroutines.
	if len(celebrities) > s.opts.MaxPullSources {
		return s.fromDB(ctx, user, c, limit)
	}
	sources := make([][]*video.Video, 0, len(celebrities)+1)
	if len(normal) > 0 {
		rows, err := s.source(ctx, s.InboxKey(user), normal, c, limit)
		if err != nil {
			return nil, err
		}
		sources = append(sources, rows)
	}
	for _, a := range celebrities {
		rows, err := s.source(ctx, s.AuthorKey(a), []uint{a}, c, limit)
		if err != nil {
			return nil, err
		}
		sources = append(sources, rows)
	}
	return Merge(sources, limit), nil
}

func (s *Service) source(ctx context.Context, key string, authors []uint, c Cursor, limit int) ([]*video.Video, error) {
	signature := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprint(authors))))
	opCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	members, ready, err := s.cache.ReadFollowing(opCtx, key, signature, c.Member(), limit)
	cancel()
	if err != nil {
		return s.query(ctx, authors, c, limit)
	}
	if !ready {
		// Rebuild recent history independent of the requested page. Singleflight
		// only merges overlapping rebuilds in this process; writes are idempotent.
		_, err, _ = s.rebuild.Do(key+signature, func() (interface{}, error) {
			rows, err := s.query(ctx, authors, Cursor{}, s.opts.Capacity)
			if err != nil {
				return nil, err
			}
			items := make([]string, 0, len(rows))
			for _, v := range rows {
				items = append(items, member(v.CreateTime, v.ID))
			}
			writeCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
			defer stop()
			// Failed cache writes never prevent serving the database result.
			_ = s.cache.AppendFollowing(writeCtx, key, items, s.opts.Capacity, 24*time.Hour, signature)
			return nil, nil
		})
		if err != nil {
			return nil, err
		}
		return s.query(ctx, authors, c, limit)
	}
	// A short cached source may cross the retained history boundary. Query the
	// entire source page from DB, not merely append after a potentially stale tail.
	if len(members) < limit {
		return s.query(ctx, authors, c, limit)
	}
	ids := make([]uint, 0, len(members))
	for _, m := range members {
		parsed, err := parseMember(m)
		if err != nil {
			return s.query(ctx, authors, c, limit)
		}
		ids = append(ids, parsed.ID)
	}
	// Validate deletion/ownership against DB. Never expose stale inbox entries
	// from an unfollowed author or trust a stale video-entity cache for filtering.
	var rows []*video.Video
	err = applyCursor(s.db.WithContext(ctx).Model(&video.Video{}).Where("id IN ? AND author_id IN ?", ids, authors), c).
		Order("create_time DESC, id DESC").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	if len(rows) < limit {
		return s.query(ctx, authors, c, limit)
	}
	return rows, nil
}

// Publish is at-least-once and safe to restart from the first follower batch.
// No goroutine per follower; each Redis operation has a bounded timeout.
func (s *Service) Publish(ctx context.Context, videoID uint) error {
	var v video.Video
	if err := s.db.WithContext(ctx).First(&v, videoID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	appendTo := func(key string) error {
		opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		return s.cache.AppendFollowing(opCtx, key, []string{member(v.CreateTime, v.ID)}, s.opts.Capacity, 24*time.Hour, "")
	}
	if err := appendTo(s.AuthorKey(v.AuthorID)); err != nil {
		return err
	}
	var count int64
	if err := s.db.WithContext(ctx).Model(&social.Social{}).Where("vlogger_id = ?", v.AuthorID).Count(&count).Error; err != nil {
		return err
	}
	if count >= s.opts.CelebrityThreshold {
		return nil
	}
	var after uint
	for {
		var followers []uint
		if err := s.db.WithContext(ctx).Model(&social.Social{}).Where("vlogger_id = ? AND follower_id > ?", v.AuthorID, after).
			Order("follower_id").Limit(s.opts.FanoutBatch).Pluck("follower_id", &followers).Error; err != nil {
			return err
		}
		if len(followers) == 0 {
			return nil
		}
		keys := make([]string, len(followers))
		for i, user := range followers {
			keys[i] = s.InboxKey(user)
		}
		opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		err := s.cache.AppendFollowingBatch(opCtx, keys, member(v.CreateTime, v.ID), s.opts.Capacity, 24*time.Hour)
		cancel()
		if err != nil {
			return err
		}
		after = followers[len(followers)-1]
	}
}

// Keep sorting logic explicit for callers/tests which construct source slices.
func Sort(videos []*video.Video) {
	sort.Slice(videos, func(i, j int) bool { return newer(videos[i], videos[j]) })
}
