package worker

import (
	"context"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/followfeed"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"
	"net"
	"net/url"
	"strconv"
)

func MQURL(cfg config.RabbitMQConfig) string {
	u := url.URL{Scheme: "amqp", Host: net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)), User: url.UserPassword(cfg.Username, cfg.Password), Path: "/"}
	return u.String()
}
func DeclareNotificationQueues(ch *amqp.Channel) error {
	for _, v := range []struct{ exchange, queue, key string }{
		{"like.events", "notification.like", "like.like"},
		{"comment.events", "notification.comment", "comment.publish"},
		{"social.events", "notification.social", "social.follow"},
	} {
		if err := rabbitmq.DeclareTopic(ch, v.exchange, v.queue, v.key); err != nil {
			return err
		}
	}
	return nil
}

func StartBusinessTasks(tasks *Tasks, db *gorm.DB, cache *rediscache.Client, url string) {
	start := func(name string, fn func(context.Context, *amqp.Channel) error) {
		tasks.Go(name, func(ctx context.Context) error {
			return WithChannel(ctx, url, func(ch *amqp.Channel) error { return fn(ctx, ch) })
		})
	}
	likes, videos := video.NewLikeRepository(db), video.NewVideoRepository(db)
	start("LikeWorker", func(ctx context.Context, ch *amqp.Channel) error {
		if err := rabbitmq.DeclareTopic(ch, "like.events", "like.events", "like.*"); err != nil {
			return err
		}
		return NewLikeWorker(ch, likes, videos, "like.events").Run(ctx)
	})
	start("CommentWorker", func(ctx context.Context, ch *amqp.Channel) error {
		if err := rabbitmq.DeclareTopic(ch, "comment.events", "comment.events", "comment.*"); err != nil {
			return err
		}
		return NewCommentWorker(ch, video.NewCommentRepository(db), videos, "comment.events").Run(ctx)
	})
	start("SocialWorker", func(ctx context.Context, ch *amqp.Channel) error {
		if err := rabbitmq.DeclareTopic(ch, "social.events", "social.events", "social.*"); err != nil {
			return err
		}
		return NewSocialWorker(ch, social.NewSocialRepository(db), "social.events").Run(ctx)
	})
	if cache != nil {
		following := followfeed.New(db, cache, followfeed.DefaultOptions())
		start("FollowingFanoutWorker", func(ctx context.Context, ch *amqp.Channel) error { return RunFollowingConsumer(ctx, ch, following) })
		start("PopularityWorker", func(ctx context.Context, ch *amqp.Channel) error {
			if err := rabbitmq.DeclareTopic(ch, "video.popularity.events", "video.popularity.events", "video.popularity.*"); err != nil {
				return err
			}
			return NewPopularityWorker(ch, cache, "video.popularity.events").Run(ctx)
		})
		start("TimelineWorker", func(ctx context.Context, ch *amqp.Channel) error { return RunTimelineConsumer(ctx, ch, cache) })
	}
	start("OutboxPoller", func(ctx context.Context, ch *amqp.Channel) error { return RunOutboxPoller(ctx, db, ch) })
	for _, queue := range []string{"notification.like", "notification.comment", "notification.social"} {
		start(queue, func(ctx context.Context, ch *amqp.Channel) error {
			if err := DeclareNotificationQueues(ch); err != nil {
				return err
			}
			publisher, err := NewNotificationPublisher(ch)
			if err != nil {
				return err
			}
			return NewNotificationWorker(ch, db, queue, publisher).Run(ctx)
		})
	}
}
