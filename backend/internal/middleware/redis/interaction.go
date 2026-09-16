package redis

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func (c *Client) VideoGeneration(ctx context.Context, id uint) (string, error) {
	if c == nil || c.rdb == nil {
		return "", errors.New("redis client not initialized")
	}
	version, err := c.rdb.Get(ctx, c.Key("video:generation:%d", id)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return version, err
}

var fillVideoScript = redis.NewScript(`
if (redis.call('GET', KEYS[1]) or '') ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
return 1
`)

// A reader must capture generation BEFORE reading MySQL. Invalidation changes
// that token atomically with DEL, so an in-flight old read cannot refill Redis.
func (c *Client) SetVideoBytes(ctx context.Context, id uint, key, generation string, body []byte, ttl time.Duration) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, errors.New("redis client not initialized")
	}
	n, err := fillVideoScript.Run(ctx, c.rdb, []string{c.Key("video:generation:%d", id), key}, generation, body, ttl.Milliseconds()).Int()
	return n == 1, err
}

var popularityScript = redis.NewScript(`
-- Validate types before changing anything: Lua errors do not roll back writes.
local seenType = redis.call('TYPE', KEYS[1]).ok
if seenType ~= 'none' and seenType ~= 'string' then return redis.error_reply('invalid dedup key type') end
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local clock = redis.call('TIME')
local active = tonumber(ARGV[3]) > tonumber(clock[1])
if active then
    local bucketType = redis.call('TYPE', KEYS[2]).ok
    if bucketType ~= 'none' and bucketType ~= 'zset' then return redis.error_reply('invalid popularity bucket type') end
end
redis.call('SET', KEYS[3], ARGV[4])
redis.call('DEL', KEYS[4], KEYS[5])
-- Expired events still invalidate details, but must never resurrect old heat.
if active then
    redis.call('ZINCRBY', KEYS[2], ARGV[2], ARGV[1])
    redis.call('EXPIREAT', KEYS[2], ARGV[3])
    redis.call('SET', KEYS[1], '1', 'EXAT', ARGV[3])
end
return 1
`)

// The dedup marker and minute bucket share a fixed expiry derived from event
// time. After that deadline replays cannot add heat, even after marker expiry.
func (c *Client) ApplyPopularity(ctx context.Context, eventID string, id uint, change int64, occurredAt time.Time) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	if eventID == "" || id == 0 || change == 0 || occurredAt.IsZero() {
		return errors.New("invalid popularity event")
	}
	minute := occurredAt.UTC().Truncate(time.Minute)
	token, err := randToken(16)
	if err != nil {
		return err
	}
	seen := fmt.Sprintf("%x", sha256.Sum256([]byte(eventID)))
	return popularityScript.Run(ctx, c.rdb, []string{
		c.Key("hot:processed:%s", seen), c.Key("hot:video:1m:%s", minute.Format("200601021504")),
		c.Key("video:generation:%d", id), c.Key("video:detail:id=%d", id), c.Key("video:entity:%d", id),
	}, fmt.Sprint(id), change, minute.Add(2*time.Hour).Unix(), token).Err()
}
