package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"testing"
	"time"
)

func testDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN not set (isolated integration database required)")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sql.Close() })
	if err := db.AutoMigrate(&video.Video{}, &video.Like{}, &video.Comment{}, &video.OutboxMsg{}, &video.ConsumedEvent{}, &Notification{}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestLikeAtomicRollbackAndDuplicate(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	v := video.Video{AuthorID: 1, Username: "test", Title: "test", PlayURL: "test", CoverURL: "test"}
	if err := db.Create(&v).Error; err != nil {
		t.Fatal(err)
	}
	repo := video.NewLikeRepository(db)
	// Force the counter update to fail after the relationship insert.
	trigger := fmt.Sprintf("fail_like_%d", v.ID)
	if err := db.Exec("CREATE TRIGGER " + trigger + " BEFORE UPDATE ON videos FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected counter failure'").Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP TRIGGER IF EXISTS " + trigger) })
	if err := repo.ApplyLike(ctx, 2, v.ID, true); err == nil {
		t.Fatal("expected injected failure")
	}
	var count int64
	db.Model(&video.Like{}).Where("video_id = ?", v.ID).Count(&count)
	if count != 0 {
		t.Fatal("relationship survived counter rollback")
	}
	if err := db.Exec("DROP TRIGGER " + trigger).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := repo.ApplyLike(ctx, 2, v.ID, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.First(&v, v.ID).Error; err != nil {
		t.Fatal(err)
	}
	if v.LikesCount != 1 || v.Popularity != 1 {
		t.Fatalf("duplicate increment: %+v", v)
	}
	for i := 0; i < 2; i++ {
		if err := repo.ApplyLike(ctx, 2, v.ID, false); err != nil {
			t.Fatal(err)
		}
	}
	db.First(&v, v.ID)
	if v.LikesCount != 0 || v.Popularity != 0 {
		t.Fatalf("unlike counts: %+v", v)
	}
}

func TestOutboxClaimsAndFailure(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg := video.OutboxMsg{VideoID: 10, Status: "pending"}
	if err := db.Create(&msg).Error; err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	done := make(chan error, 1)
	go func() {
		_, err := dispatchOutbox(ctx, db, func(context.Context, *video.OutboxMsg) error {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return errors.New("publish failed")
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	found, err := dispatchOutbox(ctx, db, func(context.Context, *video.OutboxMsg) error { t.Error("claimed locked row"); return nil })
	if err != nil || found {
		t.Fatalf("skip locked: %v %v", found, err)
	}
	release <- struct{}{}
	if err := <-done; err == nil {
		t.Fatal("failure swallowed")
	}
	var stored video.OutboxMsg
	if err := db.First(&stored, msg.ID).Error; err != nil {
		t.Fatal("failed publish lost outbox", err)
	}
	if found, err := dispatchOutbox(ctx, db, func(context.Context, *video.OutboxMsg) error { return nil }); err != nil || !found {
		t.Fatal(found, err)
	}
	if err := db.First(&stored, msg.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal("outbox not removed after confirmed success", err)
	}
}

type failingNotificationPublisher struct{ ids []uint }

func (p *failingNotificationPublisher) Publish(ctx context.Context, n *Notification) error {
	p.ids = append(p.ids, n.ID)
	return errors.New("broadcast interrupted")
}
func TestNotificationRetryKeepsSingleRow(t *testing.T) {
	db := testDB(t)
	p := &failingNotificationPublisher{}
	w := NewNotificationWorker(nil, db, "notification.social", p)
	body, _ := json.Marshal(rabbitmq.SocialEvent{FollowerID: 20, VloggerID: 30, EventID: fmt.Sprint(time.Now().UnixNano())})
	d := amqp.Delivery{RoutingKey: "social.follow", Body: body}
	for i := 0; i < 2; i++ {
		if err := w.process(context.Background(), d); err == nil {
			t.Fatal("expected publisher failure")
		}
	}
	if len(p.ids) != 2 || p.ids[0] == 0 || p.ids[0] != p.ids[1] {
		t.Fatalf("retry created a second notification: %v", p.ids)
	}
}

func TestBroadcastReachesBothAPIQueues(t *testing.T) {
	url := os.Getenv("TEST_AMQP_URL")
	if url == "" {
		t.Skip("TEST_AMQP_URL not set")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	a, _ := conn.Channel()
	defer a.Close()
	b, _ := conn.Channel()
	defer b.Close()
	qa, err := notificationDeliveries(a)
	if err != nil {
		t.Fatal(err)
	}
	qb, err := notificationDeliveries(b)
	if err != nil {
		t.Fatal(err)
	}
	ch, _ := conn.Channel()
	defer ch.Close()
	p, err := NewNotificationPublisher(ch)
	if err != nil {
		t.Fatal(err)
	}
	n := &Notification{ID: 123, RecipientID: 42}
	if err := p.Publish(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	for _, q := range []<-chan amqp.Delivery{qa, qb} {
		select {
		case d := <-q:
			var got Notification
			if json.Unmarshal(d.Body, &got) != nil || got.ID != 123 {
				t.Fatal("bad notification")
			}
			d.Ack(false)
		case <-time.After(3 * time.Second):
			t.Fatal("an API instance missed the broadcast")
		}
	}
	// Mandatory unroutable publish must not count as successful delivery.
	if err := p.publisher.Publish(context.Background(), "", "nonexistent-test-queue", true, n); err == nil {
		t.Fatal("unroutable publish accepted")
	}
}

func TestTasksStopCancelsAndWaits(t *testing.T) {
	tasks := NewTasks(context.Background())
	started, finished := make(chan struct{}), make(chan struct{})
	tasks.Go("test", func(ctx context.Context) error { close(started); <-ctx.Done(); close(finished); return ctx.Err() })
	<-started
	tasks.Stop()
	select {
	case <-finished:
	default:
		t.Fatal("Stop returned before task exited")
	}
}

func TestWithChannelStopsConsumer(t *testing.T) {
	url := os.Getenv("TEST_AMQP_URL")
	if url == "" {
		t.Skip("TEST_AMQP_URL not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- WithChannel(ctx, url, func(ch *amqp.Channel) error {
			msgs, err := notificationDeliveries(ch)
			if err != nil {
				return err
			}
			close(ready)
			for range msgs {
			}
			return nil
		})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
}
