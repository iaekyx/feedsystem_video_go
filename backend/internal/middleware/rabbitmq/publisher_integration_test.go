package rabbitmq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"sync"
	"testing"
	"time"
)

func TestConfirmedPublisherConcurrentAndUnroutable(t *testing.T) {
	url := os.Getenv("TEST_AMQP_URL")
	if url == "" {
		t.Skip("TEST_AMQP_URL not set")
	}
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewConfirmedPublisher(ch)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := publisher.Publish(ctx, "", q.Name, true, id); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 20; i++ {
		if _, ok, err := ch.Get(q.Name, true); err != nil || !ok {
			t.Fatalf("confirmed message missing: %v %v", ok, err)
		}
	}
	if err := publisher.Publish(ctx, "", fmt.Sprintf("missing-%d", time.Now().UnixNano()), true, "unroutable"); err == nil {
		t.Fatal("unroutable publish reported success")
	}
	if err := publisher.Publish(ctx, "", q.Name, true, "after-return"); err != nil {
		t.Fatal(err)
	}
	ch.Close()
	if err := publisher.Publish(ctx, "", q.Name, true, "closed"); err == nil {
		t.Fatal("closed channel reported success")
	}
}
