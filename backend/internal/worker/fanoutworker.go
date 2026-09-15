package worker

import (
	"context"
	"encoding/json"
	"feedsystem_video_go/internal/followfeed"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

const followingQueue = "video.following.fanout.queue"

func DeclareFollowing(ch *amqp.Channel) error {
	return rabbitmq.DeclareTopic(ch, timelineExchange, followingQueue, "video.timeline.publish")
}

// A separate durable subscription to the SAME confirmed publication: neither
// consumer steals the other's events and there is no two-publish atomicity gap.
func RunFollowingConsumer(ctx context.Context, ch *amqp.Channel, service *followfeed.Service) error {
	if err := DeclareFollowing(ch); err != nil {
		return err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}
	msgs, err := ch.Consume(followingQueue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("following deliveries closed")
			}
			var event rabbitmq.TimelineEvent
			if json.Unmarshal(msg.Body, &event) != nil || event.VideoID == 0 {
				if err := msg.Nack(false, false); err != nil {
					return err
				}
				continue
			}
			if err := service.Publish(ctx, event.VideoID); err != nil {
				_ = msg.Nack(false, true)
				return err // supervised reconnect/backoff; pending work is retried
			}
			if err := msg.Ack(false); err != nil {
				return err
			}
		}
	}
}
