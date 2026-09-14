package worker

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

const notificationExchange = "notification.broadcast"

func declareNotificationBroadcast(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(notificationExchange, "fanout", true, false, false, false, nil)
}

type NotificationPublisher struct{ publisher *confirmedPublisher }

func NewNotificationPublisher(ch *amqp.Channel) (*NotificationPublisher, error) {
	if err := declareNotificationBroadcast(ch); err != nil {
		return nil, err
	}
	p, err := newConfirmedPublisher(ch)
	return &NotificationPublisher{publisher: p}, err
}
func (p *NotificationPublisher) Publish(ctx context.Context, n *Notification) error {
	// No online API is valid: the durable notification remains queryable in MySQL.
	return p.publisher.Publish(ctx, notificationExchange, "", false, n)
}

// Every API gets its own ephemeral queue, so instances never compete for pushes.
// Offline clients recover via the persisted notification list, not this queue.
func notificationDeliveries(ch *amqp.Channel) (<-chan amqp.Delivery, error) {
	if err := declareNotificationBroadcast(ch); err != nil {
		return nil, err
	}
	q, err := ch.QueueDeclare("", false, true, true, false, amqp.Table{"x-message-ttl": int32(60000), "x-max-length": int32(1000)})
	if err != nil {
		return nil, err
	}
	if err := ch.QueueBind(q.Name, "", notificationExchange, false, nil); err != nil {
		return nil, err
	}
	return ch.Consume(q.Name, "", false, true, false, false, nil)
}

func RunNotificationBridge(ctx context.Context, ch *amqp.Channel, hub *SSEHub) error {
	msgs, err := notificationDeliveries(ch)
	if err != nil {
		return err
	}
	seen := make(map[uint]time.Time)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("notification bridge closed")
			}
			var n Notification
			if json.Unmarshal(d.Body, &n) == nil && n.ID != 0 {
				now := time.Now()
				for id, expires := range seen {
					if now.After(expires) {
						delete(seen, id)
					}
				}
				if _, exists := seen[n.ID]; !exists {
					hub.Push(n.RecipientID, &n)
					seen[n.ID] = now.Add(time.Minute)
				}
			}
			if err := d.Ack(false); err != nil {
				return err
			}
		}
	}
}
