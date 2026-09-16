package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

// One publisher per channel. Publish serializes sends and their confirms/returns.
type ConfirmedPublisher struct {
	mu      sync.Mutex
	failed  bool
	ch      *amqp.Channel
	returns chan amqp.Return
}

func NewConfirmedPublisher(ch *amqp.Channel) (*ConfirmedPublisher, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, err
	}
	return &ConfirmedPublisher{ch: ch, returns: ch.NotifyReturn(make(chan amqp.Return, 1))}, nil
}
func (p *ConfirmedPublisher) Publish(ctx context.Context, exchange, key string, mandatory bool, value interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed {
		return fmt.Errorf("publisher channel has an uncertain delivery; recreate it")
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(opCtx, exchange, key, mandatory, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: body,
	})
	if err != nil {
		p.failed = true
		return err
	}
	if confirmation == nil {
		p.failed = true
		return fmt.Errorf("publisher confirms not enabled")
	}
	ack, err := confirmation.WaitContext(opCtx)
	if err != nil {
		p.failed = true
		return err
	}
	if !ack {
		return fmt.Errorf("broker rejected publish")
	}
	select {
	case returned := <-p.returns:
		return fmt.Errorf("unroutable message: %s", returned.ReplyText)
	default:
	}
	return nil
}
