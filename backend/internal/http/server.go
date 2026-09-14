package http

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

// ServeUntilShutdown drains HTTP requests before returning to the caller's cleanup.
func ServeUntilShutdown(ctx context.Context, srv *http.Server, ln net.Listener, timeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		_ = srv.Close()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
	}

	log.Printf("API shutting down: waiting up to %s for active requests", timeout)
	// The signal context is already cancelled; draining needs a fresh context.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := srv.Shutdown(shutdownCtx)
	if err != nil {
		_ = srv.Close() // Also closes long-lived SSE connections on timeout.
	}
	<-serveErr
	if err != nil {
		return fmt.Errorf("HTTP shutdown (connections forcibly closed): %w", err)
	}
	log.Print("API HTTP shutdown complete")
	return nil
}
