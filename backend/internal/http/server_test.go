package http

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

type shutdownListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *shutdownListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

func TestServeUntilShutdown(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "drains_active_request"
		if force {
			name = "forces_close_on_timeout"
		}
		t.Run(name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			listener := &shutdownListener{Listener: ln, closed: make(chan struct{})}
			started, release, handlerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer close(release)
			srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(handlerDone)
				close(started)
				select {
				case <-release:
					_, _ = io.WriteString(w, "finished")
				case <-r.Context().Done():
				}
			})}
			defer srv.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			timeout := 2 * time.Second
			if force {
				timeout = 30 * time.Millisecond
			}
			done := make(chan error, 1)
			go func() { done <- ServeUntilShutdown(ctx, srv, listener, timeout) }()
			response := make(chan string, 1)
			go func() {
				client := &http.Client{Timeout: 3 * time.Second}
				resp, err := client.Get("http://" + ln.Addr().String())
				if err != nil {
					response <- "request failed"
					return
				}
				defer resp.Body.Close()
				body, _ := io.ReadAll(resp.Body)
				response <- string(body)
			}()
			select {
			case <-started:
			case <-time.After(4 * time.Second):
				t.Fatal("request did not start")
			}
			cancel()
			select {
			case <-listener.closed:
			case <-time.After(4 * time.Second):
				t.Fatal("listener not closed")
			}
			if !force {
				select {
				case err := <-done:
					t.Fatalf("returned before request completed: %v", err)
				default:
				}
				release <- struct{}{}
			}
			select {
			case err := <-done:
				if force && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected timeout, got %v", err)
				}
				if !force && err != nil {
					t.Fatal(err)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("shutdown did not return")
			}
			select {
			case <-handlerDone:
			case <-time.After(4 * time.Second):
				t.Fatal("handler did not finish")
			}
			if body := <-response; !force && body != "finished" {
				t.Fatalf("response was interrupted: %q", body)
			}
		})
	}
}
