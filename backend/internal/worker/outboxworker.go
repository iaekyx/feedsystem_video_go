package worker

import (
	"context"
	"encoding/json"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	redis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

const timelineExchange = "video.timeline.events"
const timelineQueue = "video.timeline.update.queue"

func DeclareTimeline(ch *amqp.Channel) error {
	return rabbitmq.DeclareTopic(ch, timelineExchange, timelineQueue, "video.timeline.*")
}

// Claim one row in a short transaction. Other instances skip locked rows.
// Failed/uncertain publishes roll back, retaining pending records for retry.
func dispatchOutbox(ctx context.Context, db *gorm.DB, publish func(context.Context, *video.OutboxMsg) error) (bool, error) {
	found := false
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var msg video.OutboxMsg
		result := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("status = ?", "pending").Order("id ASC").Limit(1).Find(&msg)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		found = true
		if err := publish(ctx, &msg); err != nil {
			return err
		}
		return tx.Delete(&msg).Error
	})
	return found, err
}
func RunOutboxPoller(ctx context.Context, db *gorm.DB, ch *amqp.Channel) error {
	if err := DeclareTimeline(ch); err != nil {
		return err
	}
	// Bind both queues before publishing, even if a consumer is not running yet.
	if err := DeclareFollowing(ch); err != nil {
		return err
	}
	for _, topology := range []struct{ exchange, queue, binding string }{
		{"like.events", "like.events", "like.*"},
		{"comment.events", "comment.events", "comment.*"},
		{"video.popularity.events", "video.popularity.events", "video.popularity.*"},
	} {
		if err := rabbitmq.DeclareTopic(ch, topology.exchange, topology.queue, topology.binding); err != nil {
			return err
		}
	}
	if err := DeclareNotificationQueues(ch); err != nil {
		return err
	}
	publisher, err := newConfirmedPublisher(ch)
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		found, err := dispatchOutbox(ctx, db, func(ctx context.Context, msg *video.OutboxMsg) error {
			if msg.Exchange != "" {
				if !json.Valid(msg.Payload) || msg.RoutingKey == "" {
					return fmt.Errorf("invalid outbox event %d", msg.ID)
				}
				return publisher.Publish(ctx, msg.Exchange, msg.RoutingKey, true, json.RawMessage(msg.Payload))
			}
			event := rabbitmq.TimelineEvent{EventID: fmt.Sprintf("outbox:%d", msg.ID), VideoID: msg.VideoID, CreateTime: msg.CreateTime.UnixMilli(), OccurredAt: msg.CreateTime}
			return publisher.Publish(ctx, timelineExchange, "video.timeline.publish", true, event)
		})
		if err != nil {
			return err
		}
		if !found && !pause(ctx, time.Second) {
			break
		}
	}
	return ctx.Err()
}
func RunTimelineConsumer(ctx context.Context, ch *amqp.Channel, cache *rediscache.Client) error {
	if err := DeclareTimeline(ch); err != nil {
		return err
	}
	msgs, err := ch.Consume(timelineQueue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("timeline deliveries closed")
			}
			var event rabbitmq.TimelineEvent
			if json.Unmarshal(msg.Body, &event) != nil || event.VideoID == 0 {
				_ = msg.Nack(false, false)
				continue
			}
			opCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			key := cache.Key("feed:global_timeline")
			err := cache.ZAdd(opCtx, key, redis.Z{Member: fmt.Sprint(event.VideoID), Score: float64(event.CreateTime)})
			if err == nil {
				err = cache.ZRemRangeByRank(opCtx, key, 0, -1001)
			}
			cancel()
			if err != nil {
				_ = msg.Nack(false, true)
				return err
			}
			if err := msg.Ack(false); err != nil {
				return err
			}
		}
	}
}
