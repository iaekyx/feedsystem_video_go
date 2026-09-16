package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func newInteractionVideo(t *testing.T, db *gorm.DB) video.Video {
	t.Helper()
	v := video.Video{AuthorID: 1, Username: "test", Title: "interaction", PlayURL: "test", CoverURL: "test"}
	if err := db.Create(&v).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Where("video_id = ?", v.ID).Delete(&video.OutboxMsg{})
		db.Where("video_id = ?", v.ID).Delete(&video.Like{})
		db.Where("video_id = ?", v.ID).Delete(&video.Comment{})
		db.Delete(&video.Video{}, v.ID)
	})
	return v
}

func TestLikeReplayAfterUnlikeAndHeatOnlyForChanges(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	v := newInteractionVideo(t, db)
	repo := video.NewLikeRepository(db)
	like := rabbitmq.LikeEvent{EventID: fmt.Sprintf("like-%d", v.ID), Action: "like", UserID: 2, VideoID: v.ID, OccurredAt: time.Now().UTC()}
	if err := repo.ApplyLikeEvent(ctx, like); err != nil {
		t.Fatal(err)
	}
	duplicateRequest := like
	duplicateRequest.EventID += "-another-request"
	if err := repo.ApplyLikeEvent(ctx, duplicateRequest); err != nil {
		t.Fatal(err)
	}
	unlike := like
	unlike.EventID += "-unlike"
	unlike.Action = "unlike"
	if err := repo.ApplyLikeEvent(ctx, unlike); err != nil {
		t.Fatal(err)
	}
	// ACK loss / old message replay after a later state transition.
	if err := repo.ApplyLikeEvent(ctx, like); err != nil {
		t.Fatal(err)
	}
	if err := repo.ApplyLikeEvent(ctx, duplicateRequest); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&v, v.ID).Error; err != nil {
		t.Fatal(err)
	}
	if v.LikesCount != 0 || v.Popularity != 0 {
		t.Fatalf("replay restored a cancelled like: %+v", v)
	}
	if liked, err := repo.IsLiked(ctx, v.ID, 2); err != nil || liked {
		t.Fatal(liked, err)
	}
	var events []video.OutboxMsg
	if err := db.Where("video_id = ?", v.ID).Find(&events).Error; err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected heat for only two actual changes, got %d", len(events))
	}
	for _, evt := range events {
		var heat rabbitmq.PopularityEvent
		if json.Unmarshal(evt.Payload, &heat) != nil || heat.EventID == "" || heat.VideoID != v.ID {
			t.Fatalf("bad event: %+v", evt)
		}
	}
}

func TestCommentTransactionRollbackRetryAndReplay(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	v := newInteractionVideo(t, db)
	repo := video.NewCommentRepository(db)
	evt := rabbitmq.CommentEvent{EventID: fmt.Sprintf("comment-%d", v.ID), Action: "publish", VideoID: v.ID, AuthorID: 2, Username: "test", Content: "hello", OccurredAt: time.Now().UTC()}
	trigger := fmt.Sprintf("fail_comment_%d", v.ID)
	if err := db.Exec("CREATE TRIGGER " + trigger + " BEFORE UPDATE ON videos FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected failure'").Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP TRIGGER IF EXISTS " + trigger) })
	if err := repo.ApplyCommentEvent(ctx, evt); err == nil {
		t.Fatal("expected injected failure")
	}
	var comments, events int64
	db.Model(&video.Comment{}).Where("video_id = ?", v.ID).Count(&comments)
	db.Model(&video.OutboxMsg{}).Where("video_id = ?", v.ID).Count(&events)
	if comments != 0 || events != 0 {
		t.Fatalf("partial commit: comments=%d events=%d", comments, events)
	}
	if err := db.Exec("DROP TRIGGER " + trigger).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := repo.ApplyCommentEvent(ctx, evt); err != nil {
			t.Fatal(err)
		}
	}
	db.Model(&video.Comment{}).Where("video_id = ?", v.ID).Count(&comments)
	db.Model(&video.OutboxMsg{}).Where("video_id = ?", v.ID).Count(&events)
	db.First(&v, v.ID)
	if comments != 1 || events != 1 || v.Popularity != 1 {
		t.Fatalf("retry duplicated work: comments=%d events=%d heat=%d", comments, events, v.Popularity)
	}
	var c video.Comment
	db.Where("video_id = ?", v.ID).First(&c)
	deletion := rabbitmq.CommentEvent{EventID: evt.EventID + "-delete", Action: "delete", CommentID: c.ID}
	if err := repo.ApplyCommentEvent(ctx, deletion); err != nil {
		t.Fatal(err)
	}
	if err := repo.ApplyCommentEvent(ctx, evt); err != nil {
		t.Fatal(err)
	}
	db.Model(&video.Comment{}).Where("video_id = ?", v.ID).Count(&comments)
	if comments != 0 {
		t.Fatal("old publish recreated a deleted comment")
	}
}

func TestHeatOutboxFailureRollsBackLikeAndInbox(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	v := newInteractionVideo(t, db)
	evt := rabbitmq.LikeEvent{EventID: fmt.Sprintf("outbox-failure-%d", v.ID), Action: "like", VideoID: v.ID, UserID: 2, OccurredAt: time.Now()}
	trigger := fmt.Sprintf("fail_heat_%d", v.ID)
	if err := db.Exec("CREATE TRIGGER " + trigger + " BEFORE INSERT ON outbox_msgs FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='outbox unavailable'").Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP TRIGGER IF EXISTS " + trigger) })
	repo := video.NewLikeRepository(db)
	if err := repo.ApplyLikeEvent(ctx, evt); err == nil {
		t.Fatal("expected outbox failure")
	}
	if liked, _ := repo.IsLiked(ctx, v.ID, 2); liked {
		t.Fatal("business commit survived missing outbox")
	}
	db.First(&v, v.ID)
	if v.LikesCount != 0 || v.Popularity != 0 {
		t.Fatal("counters survived rollback")
	}
	if err := db.Exec("DROP TRIGGER " + trigger).Error; err != nil {
		t.Fatal(err)
	}
	if err := repo.ApplyLikeEvent(ctx, evt); err != nil {
		t.Fatal(err)
	}
	if liked, _ := repo.IsLiked(ctx, v.ID, 2); !liked {
		t.Fatal("inbox was not rolled back; retry skipped")
	}
}

func TestServicePersistsCommandWithoutMQOrEarlyHeat(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	v := newInteractionVideo(t, db)
	repo := video.NewLikeRepository(db)
	service := video.NewLikeService(repo, video.NewVideoRepository(db), nil, nil, nil)
	if err := service.Like(ctx, &video.Like{VideoID: v.ID, AccountID: 2}); err != nil {
		t.Fatal(err)
	}
	var commands []video.OutboxMsg
	if err := db.Where("video_id = ?", v.ID).Find(&commands).Error; err != nil {
		t.Fatal(err)
	}
	if len(commands) != 1 || commands[0].RoutingKey != "like.like" {
		t.Fatalf("expected just a durable command: %+v", commands)
	}
	if liked, _ := repo.IsLiked(ctx, v.ID, 2); liked {
		t.Fatal("unexpected direct-write fallback")
	}
	w := NewLikeWorker(nil, repo, video.NewVideoRepository(db), "like.events")
	if err := w.process(ctx, commands[0].Payload); err != nil {
		t.Fatal(err)
	}
	if err := w.process(ctx, commands[0].Payload); err != nil {
		t.Fatal(err)
	}
	if liked, _ := repo.IsLiked(ctx, v.ID, 2); !liked {
		t.Fatal("persisted command did not apply")
	}
}

func TestPopularityWorkerReturnsRedisFailure(t *testing.T) {
	mr := miniredis.RunT(t)
	cache := rediscache.NewClient(goredis.NewClient(&goredis.Options{Addr: mr.Addr()}), "worker:")
	defer cache.Close()
	at := time.Now().UTC().Truncate(time.Minute)
	key := cache.Key("hot:video:1m:%s", at.Format("200601021504"))
	mr.Set(key, "wrong-type")
	body, _ := json.Marshal(rabbitmq.PopularityEvent{EventID: "retry", VideoID: 42, Change: 1, OccurredAt: at})
	w := NewPopularityWorker(nil, cache, "test")
	if err := w.process(context.Background(), body); err == nil {
		t.Fatal("failed Redis update would be ACKed")
	}
	mr.Del(key)
	if err := w.process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxRetainsExactBodyOnUncertainPublish(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	v := newInteractionVideo(t, db)
	// Isolated test database only; other tests must have dispatched their rows.
	event := rabbitmq.LikeEvent{EventID: fmt.Sprintf("stable-%d", v.ID), Action: "like", UserID: 2, VideoID: v.ID, OccurredAt: time.Now()}
	if err := video.EnqueueEvent(db, v.ID, "like.events", "like.like", event); err != nil {
		t.Fatal(err)
	}
	var first []byte
	found, err := dispatchOutbox(ctx, db, func(_ context.Context, msg *video.OutboxMsg) error {
		first = append([]byte(nil), msg.Payload...)
		return errors.New("confirmation lost")
	})
	if !found || err == nil {
		t.Fatal(found, err)
	}
	found, err = dispatchOutbox(ctx, db, func(_ context.Context, msg *video.OutboxMsg) error {
		if string(first) != string(msg.Payload) {
			t.Fatal("retry changed message identity")
		}
		return nil
	})
	if !found || err != nil {
		t.Fatal(found, err)
	}
}
