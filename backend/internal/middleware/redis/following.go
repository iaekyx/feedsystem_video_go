package redis

import (
	"context"
	"errors"
	"time"

	rdb "github.com/redis/go-redis/v9"
)

// All members have score zero; fixed-width timestamp:ID members provide an
// exact composite order without packing integers into floating-point scores.
const appendFollowingSource = `
for i = 4, #ARGV do redis.call('ZADD', KEYS[1], 0, ARGV[i]) end
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -tonumber(ARGV[1])-1)
redis.call('EXPIRE', KEYS[1], ARGV[2])
if ARGV[3] ~= '' then redis.call('SET', KEYS[2], ARGV[3], 'EX', 60) end
return 1
`

var appendFollowing = rdb.NewScript(appendFollowingSource)

// Union, rather than replace, preserves publications concurrent with rebuilds.
// Only a DB rebuild may mark a source ready; one pushed event is not a snapshot.
func (c *Client) AppendFollowing(ctx context.Context, key string, members []string, capacity int, ttl time.Duration, signature string) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis unavailable")
	}
	args := []interface{}{capacity, int64(ttl.Seconds()), signature}
	for _, m := range members {
		args = append(args, m)
	}
	return appendFollowing.Run(ctx, c.rdb, []string{key, key + ":ready"}, args...).Err()
}

// Each inbox update is atomic; the batch is pipelined (not a cross-inbox
// transaction). On an uncertain partial failure the consumer retries all items.
func (c *Client) AppendFollowingBatch(ctx context.Context, keys []string, member string, capacity int, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis unavailable")
	}
	_, err := c.rdb.Pipelined(ctx, func(p rdb.Pipeliner) error {
		for _, key := range keys {
			// EVAL avoids NOSCRIPT recovery after a queued EVALSHA pipeline.
			p.Eval(ctx, appendFollowingSource, []string{key, key + ":ready"}, capacity, int64(ttl.Seconds()), "", member)
		}
		return nil
	})
	return err
}

var readFollowing = rdb.NewScript(`
if redis.call('GET', KEYS[2]) ~= ARGV[1] or redis.call('EXISTS', KEYS[1]) == 0 then
  return {0}
end
local result = {1}
local members = redis.call('ZREVRANGEBYLEX', KEYS[1], ARGV[2], '-', 'LIMIT', 0, ARGV[3])
for _, m in ipairs(members) do table.insert(result, m) end
return result
`)

func (c *Client) ReadFollowing(ctx context.Context, key, signature, before string, limit int) ([]string, bool, error) {
	if c == nil || c.rdb == nil {
		return nil, false, errors.New("redis unavailable")
	}
	max := "+"
	if before != "" {
		max = "(" + before
	}
	result, err := readFollowing.Run(ctx, c.rdb, []string{key, key + ":ready"}, signature, max, limit).Slice()
	if err != nil {
		return nil, false, err
	}
	if len(result) == 0 || result[0] != int64(1) {
		return nil, false, nil
	}
	members := make([]string, 0, len(result)-1)
	for _, v := range result[1:] {
		m, ok := v.(string)
		if !ok {
			return nil, false, errors.New("invalid following index")
		}
		members = append(members, m)
	}
	return members, true, nil
}
