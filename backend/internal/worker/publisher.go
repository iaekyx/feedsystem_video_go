package worker

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

// One publisher per task/channel; callers serialize Publish calls.
type confirmedPublisher struct {
	ch      *amqp.Channel
	returns chan amqp.Return
}

func newConfirmedPublisher(ch *amqp.Channel) (*confirmedPublisher, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, err
	}
	return &confirmedPublisher{ch: ch, returns: ch.NotifyReturn(make(chan amqp.Return, 1))}, nil
}
func (p *confirmedPublisher) Publish(ctx context.Context, exchange, key string, mandatory bool, value interface{}) error {
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
		return err
	}
	if confirmation == nil {
		return fmt.Errorf("publisher confirms not enabled")
	}
	ack, err := confirmation.WaitContext(opCtx)
	if err != nil {
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
