package video

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	"gorm.io/gorm"
)

// EnqueueEvent persists the exact event body (including its ID) for every retry.
// When called with a transaction, the business mutation and event commit together.
func EnqueueEvent(tx *gorm.DB, videoID uint, exchange, key string, event any) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return tx.Create(&OutboxMsg{VideoID: videoID, Exchange: exchange,
		RoutingKey: key, Payload: body, EventType: key, Status: "pending"}).Error
}

// Keep inbox records permanently: deleting them would make old replays effective again.
func consumeEvent(db *gorm.DB, ctx context.Context, consumer, id string, apply func(*gorm.DB) error) error {
	if id == "" {
		return errors.New("event_id is required")
	}
	key := fmt.Sprintf("%x", sha256.Sum256([]byte(consumer+":"+id)))
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&ConsumedEvent{EventKey: key}).Error; err != nil {
			if isDupKey(err) {
				return nil
			}
			return err
		}
		return apply(tx)
	})
}

func enqueuePopularity(tx *gorm.DB, source, id string, videoID uint, delta int64, at time.Time) error {
	if at.IsZero() {
		return errors.New("occurred_at is required")
	}
	return EnqueueEvent(tx, videoID, "video.popularity.events", "video.popularity.update",
		rabbitmq.PopularityEvent{EventID: source + ":" + id, VideoID: videoID, Change: delta, OccurredAt: at})
}
