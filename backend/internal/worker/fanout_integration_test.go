package worker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"feedsystem_video_go/internal/followfeed"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	amqp "github.com/rabbitmq/amqp091-go"
	redis "github.com/redis/go-redis/v9"
)

func TestOutboxReachesTimelineAndFollowing(t *testing.T) {
	url, addr := os.Getenv("TEST_AMQP_URL"), os.Getenv("TEST_REDIS_ADDR")
	if url == "" || addr == "" {
		t.Skip("isolated RabbitMQ/Redis not configured")
	}
	db := testDB(t)
	if err := db.AutoMigrate(&social.Social{}); err != nil {
		t.Fatal(err)
	}
	user := uint(time.Now().UnixNano() / 1000)
	if err := db.Create(&social.Social{FollowerID: user, VloggerID: user + 1}).Error; err != nil {
		t.Fatal(err)
	}
	v := video.Video{AuthorID: user + 1, Username: "test", Title: "fanout", PlayURL: "test", CoverURL: "test", CreateTime: time.Now().UTC().Truncate(time.Millisecond)}
	if err := db.Create(&v).Error; err != nil {
		t.Fatal(err)
	}
	// Simulate at-least-once duplicate publication from a retried outbox dispatch.
	for i := 0; i < 2; i++ {
		if err := db.Create(&video.OutboxMsg{VideoID: v.ID, Status: "pending", CreateTime: v.CreateTime}).Error; err != nil {
			t.Fatal(err)
		}
	}
	raw := redis.NewClient(&redis.Options{Addr: addr})
	defer raw.Close()
	cache := rediscache.NewClient(raw, fmt.Sprintf("test:outbox:%d:", user))
	service := followfeed.New(db, cache, followfeed.Options{CelebrityThreshold: 2})
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	channel := func() *amqp.Channel {
		t.Helper()
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		return ch
	}
	publisher, timeline, following := channel(), channel(), channel()
	defer publisher.Close()
	defer timeline.Close()
	defer following.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 3)
	go func() { done <- RunTimelineConsumer(ctx, timeline, cache) }()
	go func() { done <- RunFollowingConsumer(ctx, following, service) }()
	go func() { done <- RunOutboxPoller(ctx, db, publisher) }()
	t.Cleanup(func() {
		cancel()
		for i := 0; i < 3; i++ {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("worker failed to stop")
				return
			}
		}
	})
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("outbox did not reach both projections", ctx.Err())
		case <-ticker.C:
			var pending int64
			if err := db.Model(&video.OutboxMsg{}).Where("video_id = ?", v.ID).Count(&pending).Error; err != nil {
				t.Fatal(err)
			}
			if pending != 0 {
				continue
			}
			authorCount, e1 := raw.ZCard(ctx, service.AuthorKey(v.AuthorID)).Result()
			inboxCount, e2 := raw.ZCard(ctx, service.InboxKey(user)).Result()
			score, e3 := raw.ZScore(ctx, cache.Key("feed:global_timeline"), fmt.Sprint(v.ID)).Result()
			if e1 == nil && e2 == nil && e3 == nil && authorCount == 1 && inboxCount == 1 && score == float64(v.CreateTime.UnixMilli()) {
				return
			}
		}
	}
}
