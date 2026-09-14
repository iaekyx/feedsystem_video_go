package worker

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"net"
	"sync"
	"time"
)

// Tasks owns goroutines. Stop must run before closing shared DB/cache.
type Tasks struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewTasks(parent context.Context) *Tasks {
	ctx, cancel := context.WithCancel(parent)
	return &Tasks{ctx: ctx, cancel: cancel}
}
func (g *Tasks) Go(name string, fn func(context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		for g.ctx.Err() == nil {
			if err := fn(g.ctx); err != nil && g.ctx.Err() == nil {
				log.Printf("%s: %v", name, err)
			}
			if !pause(g.ctx, time.Second) {
				return
			}
		}
	}()
}
func (g *Tasks) Stop() { g.cancel(); g.wg.Wait() }
func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Each task owns a connection: reconnects restore both connection and channel.
func WithChannel(ctx context.Context, url string, fn func(*amqp.Channel) error) error {
	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial: func(network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			// amqp091 clears this deadline after a successful protocol handshake.
			if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		},
	})
	if err != nil {
		return err
	}
	done := make(chan struct{})
	defer close(done)
	defer conn.Close()
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.Qos(50, 0, false); err != nil {
		return err
	}
	return fn(ch)
}
