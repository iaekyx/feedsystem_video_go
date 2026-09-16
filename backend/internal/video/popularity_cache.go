package video

import (
	"context"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"time"
)

func UpdatePopularityCache(ctx context.Context, cache *rediscache.Client, evt rabbitmq.PopularityEvent) error {
	opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	return cache.ApplyPopularity(opCtx, evt.EventID, evt.VideoID, evt.Change, evt.OccurredAt)
}
