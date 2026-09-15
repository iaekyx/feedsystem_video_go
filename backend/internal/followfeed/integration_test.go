package followfeed

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	redis "github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func integration(t *testing.T) (*gorm.DB, *redis.Client, *rediscache.Client) {
	t.Helper()
	dsn, addr := os.Getenv("TEST_MYSQL_DSN"), os.Getenv("TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("isolated MySQL/Redis not configured")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sql, _ := db.DB()
	t.Cleanup(func() { sql.Close() })
	if err := db.AutoMigrate(&video.Video{}, &social.Social{}); err != nil {
		t.Fatal(err)
	}
	r := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { r.Close() })
	return db, r, rediscache.NewClient(r, fmt.Sprintf("test:following:%d:", time.Now().UnixNano()))
}

func TestHybridPublishReadTransitionsAndFallback(t *testing.T) {
	db, raw, cache := integration(t)
	ctx := context.Background()
	user := uint(time.Now().UnixNano() / 1000)
	a, b, extra, other := user+1, user+2, user+3, user+4
	follow := func(u, author uint) {
		t.Helper()
		if err := db.Create(&social.Social{FollowerID: u, VloggerID: author}).Error; err != nil {
			t.Fatal(err)
		}
	}
	follow(user, a)
	follow(user, b)
	follow(other, b) // b is a celebrity at threshold 2
	s := New(db, cache, Options{CelebrityThreshold: 2, Capacity: 4, FanoutBatch: 1})
	now := time.Now().UTC().Truncate(time.Second)
	create := func(author uint, when time.Time) *video.Video {
		t.Helper()
		v := &video.Video{AuthorID: author, Username: "integration", Title: "following", PlayURL: "test", CoverURL: "test", CreateTime: when}
		if err := db.Create(v).Error; err != nil {
			t.Fatal(err)
		}
		return v
	}
	var all []*video.Video
	for i := 0; i < 10; i++ {
		author := a
		if i%2 == 1 {
			author = b
		}
		v := create(author, now.Add(time.Duration(i/3)*time.Second)) // equal-time ties
		all = append(all, v)
		if err := s.Publish(ctx, v.ID); err != nil {
			t.Fatal(err)
		}
		if err := s.Publish(ctx, v.ID); err != nil {
			t.Fatal("retry", err)
		}
	}
	Sort(all)
	if n := raw.ZCard(ctx, s.AuthorKey(b)).Val(); n != 4 {
		t.Fatalf("author capacity/idempotence: %d", n)
	}
	for _, m := range raw.ZRange(ctx, s.InboxKey(user), 0, -1).Val() {
		c, err := parseMember(m)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range all {
			if v.ID == c.ID && v.AuthorID == b {
				t.Fatal("celebrity was pushed")
			}
		}
	}
	assertPage := func(reader *Service, cursor Cursor, want []*video.Video, n int) []*video.Video {
		t.Helper()
		got, err := reader.Read(ctx, user, cursor, n)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("length %d != %d", len(got), len(want))
		}
		for i := range want {
			if got[i].ID != want[i].ID {
				t.Fatalf("position %d: %d != %d", i, got[i].ID, want[i].ID)
			}
		}
		return got
	}
	assertPage(s, Cursor{}, all[:3], 3) // DB rebuild, including historical posts
	assertPage(s, Cursor{}, all[:3], 3) // ready Redis indexes + merge
	var cursor Cursor
	for offset := 0; offset < len(all); offset += 3 {
		end := offset + 3
		if end > len(all) {
			end = len(all)
		}
		page := assertPage(s, cursor, all[offset:end], 3)
		cursor, _ = DecodeCursor(EncodeCursor(page[len(page)-1]))
	}
	assertPage(s, cursor, nil, 3)
	assertPage(New(db, nil, Options{}), Cursor{}, all[:3], 3)
	// A new ordinary author must be backfilled despite an already-ready inbox.
	newVideo := create(extra, now.Add(time.Hour))
	follow(user, extra)
	withNew := append([]*video.Video{newVideo}, all...)
	assertPage(s, Cursor{}, withNew[:3], 3)
	// Unfollow filters previously pushed IDs; deleted videos must also disappear.
	if err := db.Where("follower_id = ? AND vlogger_id = ?", user, a).Delete(&social.Social{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(newVideo).Error; err != nil {
		t.Fatal(err)
	}
	var onlyB []*video.Video
	for _, v := range all {
		if v.AuthorID == b {
			onlyB = append(onlyB, v)
		}
	}
	assertPage(s, Cursor{}, onlyB[:3], 3)
	// b crosses from pull to push; history is recovered without a migration job.
	if err := db.Where("follower_id = ? AND vlogger_id = ?", other, b).Delete(&social.Social{}).Error; err != nil {
		t.Fatal(err)
	}
	assertPage(s, Cursor{}, onlyB[:3], 3)
	pushed := create(b, now.Add(2*time.Hour))
	if err := s.Publish(ctx, pushed.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ZScore(ctx, s.InboxKey(user), member(pushed.CreateTime, pushed.ID)).Result(); err != nil {
		t.Fatal("ordinary author not pushed", err)
	}
	onlyB = append([]*video.Video{pushed}, onlyB...)
	assertPage(s, Cursor{}, onlyB[:3], 3)
	// Switch back to pull. The DB-owned source classification prevents duplicates.
	follow(other, b)
	assertPage(s, Cursor{}, onlyB[:3], 3)
	// Redis failure is not a failed Feed request; use the same SQL cursor/order.
	brokenRaw := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 10 * time.Millisecond})
	defer brokenRaw.Close()
	broken := New(db, rediscache.NewClient(brokenRaw, "broken:"), Options{CelebrityThreshold: 2})
	assertPage(broken, Cursor{}, onlyB[:3], 3)
	if err := broken.Publish(ctx, pushed.ID); err == nil {
		t.Fatal("failed fanout must be retried, not acked")
	}
	// Eviction of index alone must not let the remaining ready marker hide data.
	if err := raw.Del(ctx, s.AuthorKey(b)).Err(); err != nil {
		t.Fatal(err)
	}
	assertPage(s, Cursor{}, onlyB[:3], 3)
	got, err := s.Read(ctx, 0, Cursor{}, 3)
	if err != nil || len(got) != 0 {
		t.Fatal("anonymous feed must not expose global videos")
	}
}

func TestFanoutPartialBatchRetry(t *testing.T) {
	db, raw, cache := integration(t)
	ctx := context.Background()
	user := uint(time.Now().UnixNano() / 1000)
	author := user + 10
	for i := uint(0); i < 3; i++ {
		if err := db.Create(&social.Social{FollowerID: user + i, VloggerID: author}).Error; err != nil {
			t.Fatal(err)
		}
	}
	v := video.Video{AuthorID: author, Title: "batch", PlayURL: "test", CoverURL: "test", CreateTime: time.Now().UTC().Truncate(time.Millisecond)}
	if err := db.Create(&v).Error; err != nil {
		t.Fatal(err)
	}
	s := New(db, cache, Options{CelebrityThreshold: 10, FanoutBatch: 2})
	if err := raw.Set(ctx, s.InboxKey(user+1), "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(ctx, v.ID); err == nil {
		t.Fatal("partial pipeline failure must fail delivery")
	}
	if n := raw.ZCard(ctx, s.InboxKey(user)).Val(); n != 1 {
		t.Fatal("expected first recipient before partial failure")
	}
	if err := raw.Del(ctx, s.InboxKey(user+1)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.Publish(ctx, v.ID); err != nil {
		t.Fatal(err)
	}
	for i := uint(0); i < 3; i++ {
		if n := raw.ZCard(ctx, s.InboxKey(user+i)).Val(); n != 1 {
			t.Fatalf("recipient %d count %d", i, n)
		}
	}
}

func TestRebuildPreservesConcurrentPublication(t *testing.T) {
	_, _, cache := integration(t)
	ctx := context.Background()
	key := cache.Key("feed:inbox:concurrent")
	now := time.Now().UTC().Truncate(time.Millisecond)
	newest, older := member(now, 2), member(now.Add(-time.Second), 1)
	if err := cache.AppendFollowing(ctx, key, []string{newest}, 1000, time.Hour, ""); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := cache.ReadFollowing(ctx, key, "snapshot", "", 10); err != nil || ready {
		t.Fatal("a single publication is not a ready snapshot", err)
	}
	// The DB query started before newest was published, so its snapshot omits it.
	if err := cache.AppendFollowing(ctx, key, []string{older}, 1000, time.Hour, "snapshot"); err != nil {
		t.Fatal(err)
	}
	members, ready, err := cache.ReadFollowing(ctx, key, "snapshot", "", 10)
	if err != nil || !ready || len(members) != 2 || members[0] != newest || members[1] != older {
		t.Fatalf("rebuild lost concurrent event: %v %v %v", members, ready, err)
	}
}
