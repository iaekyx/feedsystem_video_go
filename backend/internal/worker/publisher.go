package worker

import (
	"feedsystem_video_go/internal/middleware/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type confirmedPublisher = rabbitmq.ConfirmedPublisher

func newConfirmedPublisher(ch *amqp.Channel) (*confirmedPublisher, error) {
	return rabbitmq.NewConfirmedPublisher(ch)
}
