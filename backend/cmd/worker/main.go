package main

import (
	"context"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
	"feedsystem_video_go/internal/worker"
	"fmt"
	"github.com/joho/godotenv"
	"gorm.io/gorm"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Worker stopped: %v", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	_ = godotenv.Load()
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "configs/config.yaml"
	}
	cfg, _, err := config.LoadLocalDev(path)
	if err != nil {
		return err
	}
	var sqlDB *gorm.DB
	for attempt := 0; attempt < 10; attempt++ {
		sqlDB, err = db.NewDB(cfg.Database)
		if err == nil {
			break
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if err != nil {
		return fmt.Errorf("connect MySQL: %w", err)
	}
	defer db.CloseDB(sqlDB)
	// API owns schema migration. Compose waits for its health check before Worker.
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		return err
	}
	defer cache.Close()
	pprof, err := observability.NewPprofServer("Worker", cfg.ObservabilityConfig.Pprof.Enabled, cfg.ObservabilityConfig.Pprof.WorkerAddr)
	if err != nil {
		log.Printf("pprof: %v", err)
	}
	if pprof != nil {
		defer pprof.Close()
	}
	tasks := worker.NewTasks(ctx)
	defer tasks.Stop()
	worker.StartBusinessTasks(tasks, sqlDB, cache, worker.MQURL(cfg.RabbitMQ))
	log.Print("Worker started: business consumers, outbox, timeline and notifications")
	<-ctx.Done()
	log.Print("Worker stopping: cancelling tasks and waiting before resource cleanup")
	return nil
}
