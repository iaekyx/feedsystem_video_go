package worker

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	amqp "github.com/rabbitmq/amqp091-go"
	goredis "github.com/redis/go-redis/v9"
)

func TestInteractionOutboxEndToEnd(t *testing.T) {
	url, addr := os.Getenv("TEST_AMQP_URL"), os.Getenv("TEST_REDIS_ADDR")
	if url == "" || addr == "" {
		t.Skip("TEST_AMQP_URL and TEST_REDIS_ADDR required")
	}
	db := testDB(t)
	v := newInteractionVideo(t, db)
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	defer rdb.Close()
	prefix := "interaction-e2e:" + strconv.FormatInt(time.Now().UnixNano(), 10) + ":"
	cache := rediscache.NewClient(rdb, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tasks := NewTasks(ctx)
	defer tasks.Stop()
	start := func(name string, run func(context.Context, *amqp.Channel) error) {
		tasks.Go(name, func(ctx context.Context) error {
			return WithChannel(ctx, url, func(ch *amqp.Channel) error { return run(ctx, ch) })
		})
	}
	start("test-outbox", func(ctx context.Context, ch *amqp.Channel) error { return RunOutboxPoller(ctx, db, ch) })
	likes := video.NewLikeRepository(db)
	videos := video.NewVideoRepository(db)
	start("test-like", func(ctx context.Context, ch *amqp.Channel) error {
		return NewLikeWorker(ch, likes, videos, "like.events").Run(ctx)
	})
	start("test-comment", func(ctx context.Context, ch *amqp.Channel) error {
		return NewCommentWorker(ch, video.NewCommentRepository(db), videos, "comment.events").Run(ctx)
	})
	start("test-heat", func(ctx context.Context, ch *amqp.Channel) error {
		return NewPopularityWorker(ch, cache, "video.popularity.events").Run(ctx)
	})
	likeService := video.NewLikeService(likes, videos, cache, nil, nil)
	commentService := video.NewCommentService(video.NewCommentRepository(db), videos, cache, nil, nil)
	if err := likeService.Like(ctx, &video.Like{VideoID: v.ID, AccountID: 2}); err != nil {
		t.Fatal(err)
	}
	if err := commentService.Publish(ctx, &video.Comment{VideoID: v.ID, AuthorID: 2, Username: "test", Content: "end to end"}); err != nil {
		t.Fatal(err)
	}
	waitFor := func(expectedLikes int64, expectedHeat float64) {
		t.Helper()
		for ctx.Err() == nil {
			var current video.Video
			if err := db.First(&current, v.ID).Error; err != nil {
				t.Fatal(err)
			}
			var comments, pending int64
			db.Model(&video.Comment{}).Where("video_id = ?", v.ID).Count(&comments)
			db.Model(&video.OutboxMsg{}).Where("video_id = ?", v.ID).Count(&pending)
			keys, err := rdb.Keys(ctx, prefix+"hot:video:1m:*").Result()
			var heat float64
			if err == nil {
				for _, key := range keys {
					score, _ := rdb.ZScore(ctx, key, strconv.FormatUint(uint64(v.ID), 10)).Result()
					heat += score
				}
			}
			if current.LikesCount == expectedLikes && current.Popularity == int64(expectedHeat) && comments == 1 && heat == expectedHeat && pending == 0 {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("interaction pipeline did not converge", ctx.Err())
	}
	waitFor(1, 2)
	if err := likeService.Unlike(ctx, &video.Like{VideoID: v.ID, AccountID: 2}); err != nil {
		t.Fatal(err)
	}
	waitFor(0, 1)
	cancel()
	tasks.Stop()
}
