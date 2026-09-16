package redis

import (
	"context"
	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"sync"
	"testing"
	"time"
)

func interactionCache(t *testing.T) (*miniredis.Miniredis, *Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := NewClient(goredis.NewClient(&goredis.Options{Addr: mr.Addr()}), "test:")
	t.Cleanup(func() { _ = c.Close() })
	return mr, c
}

func TestPopularityConcurrentRedeliveryAndExpiry(t *testing.T) {
	mr, c := interactionCache(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Minute)
	mr.SetTime(at)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.ApplyPopularity(ctx, "event-1", 42, 1, at); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	key := c.Key("hot:video:1m:%s", at.Format("200601021504"))
	score, err := c.rdb.ZScore(ctx, key, "42").Result()
	if err != nil || score != 1 {
		t.Fatalf("duplicate heat: score=%v err=%v", score, err)
	}
	// Opposite action has a different ID and must apply once, not be suppressed.
	for i := 0; i < 2; i++ {
		if err := c.ApplyPopularity(ctx, "event-2", 42, -1, at); err != nil {
			t.Fatal(err)
		}
	}
	if score, _ := c.rdb.ZScore(ctx, key, "42").Result(); score != 0 {
		t.Fatal(score)
	}
	mr.FastForward(3 * time.Hour)
	mr.SetTime(at.Add(3 * time.Hour))
	if err := c.ApplyPopularity(ctx, "event-1", 42, 1, at); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(key) {
		t.Fatal("expired replay resurrected a heat bucket")
	}
}

func TestCacheFillCannotOverwriteInvalidation(t *testing.T) {
	_, c := interactionCache(t)
	ctx := context.Background()
	oldGeneration, err := c.VideoGeneration(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{c.Key("video:entity:%d", 42), c.Key("video:detail:id=%d", 42)}
	for _, key := range keys {
		if ok, err := c.SetVideoBytes(ctx, 42, key, oldGeneration, []byte("old"), time.Hour); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	if err := c.ApplyPopularity(ctx, "commit-1", 42, 1, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if _, err := c.GetBytes(ctx, key); !IsMiss(err) {
			t.Fatalf("not invalidated: %s %v", key, err)
		}
		if ok, err := c.SetVideoBytes(ctx, 42, key, oldGeneration, []byte("stale-in-flight"), time.Hour); err != nil || ok {
			t.Fatalf("stale fill accepted: %v %v", ok, err)
		}
	}
	generation, err := c.VideoGeneration(ctx, 42)
	if err != nil {
		t.Fatal(err)
	}
	if generation == oldGeneration {
		t.Fatal("invalidation did not change token")
	}
	for _, key := range keys {
		if ok, err := c.SetVideoBytes(ctx, 42, key, generation, []byte("fresh"), time.Hour); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	if err := c.ApplyPopularity(ctx, "commit-1", 42, 1, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if b, err := c.GetBytes(ctx, key); err != nil || string(b) != "fresh" {
			t.Fatalf("redelivery invalidated fresh cache: %s %v", b, err)
		}
	}
}

func TestPopularityFailureIsRetryableWithoutPartialMutation(t *testing.T) {
	mr, c := interactionCache(t)
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Minute)
	key := c.Key("hot:video:1m:%s", at.Format("200601021504"))
	mr.Set(key, "wrong-type")
	if err := c.ApplyPopularity(ctx, "retry-me", 42, 1, at); err == nil {
		t.Fatal("Redis error swallowed")
	}
	if generation, _ := c.VideoGeneration(ctx, 42); generation != "" {
		t.Fatal("failed script partially invalidated")
	}
	mr.Del(key)
	if err := c.ApplyPopularity(ctx, "retry-me", 42, 1, at); err != nil {
		t.Fatal(err)
	}
	if score, _ := c.rdb.ZScore(ctx, key, "42").Result(); score != 1 {
		t.Fatal(score)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := c.ApplyPopularity(cancelled, "cancelled", 42, 1, at); err == nil {
		t.Fatal("cancelled write treated as success")
	}
}
