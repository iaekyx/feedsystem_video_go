package main

import (
	"context"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"
	apphttp "feedsystem_video_go/internal/http"
	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
	"feedsystem_video_go/internal/worker"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		log.Printf("API stopped with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// 加载 .env（本地开发）
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found; continuing")
	}

	// 加载配置
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/config.yaml"
	}
	log.Printf("Loading config from %s", configPath)
	cfg, usedDefault, err := config.LoadLocalDev(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if usedDefault {
		log.Printf("Config File %s not found, using default local config", configPath)
	} else {
		log.Printf("Config loaded from file: %s", configPath)
	}

	// 连接数据库
	//log.Printf("Database config: %v", cfg.Database)
	sqlDB, err := db.NewDB(cfg.Database)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer db.CloseDB(sqlDB)
	if err := db.AutoMigrate(sqlDB); err != nil {
		return fmt.Errorf("auto migrate database: %w", err)
	}

	// 连接 Redis (可选，用于缓存)
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		log.Printf("Redis config error (cache disabled): %v", err)
		cache = nil
	} else {
		//创建客户端之后，代码还会执行一次带 300ms 超时的 Ping，确认 Redis 是否能连接。
		pingCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		if err := cache.Ping(pingCtx); err != nil {
			log.Printf("Redis not available (cache disabled): %v", err)
			_ = cache.Close()
			cache = nil
		} else {
			defer cache.Close()
			log.Printf("Redis connected (cache enabled)")
		}
	}

	// 连接 RabbitMQ (可选，用于消息队列)
	rmq, err := rabbitmq.NewRabbitMQ(&cfg.RabbitMQ)
	if err != nil {
		log.Printf("RabbitMQ config error (disabled): %v", err)
		rmq = nil
	} else {
		defer rmq.Close()
		log.Printf("RabbitMQ connected")
	}
	// Pprof
	pprofServer, err := observability.NewPprofServer(
		"API",
		cfg.ObservabilityConfig.Pprof.Enabled,
		cfg.ObservabilityConfig.Pprof.ApiAddr,
	)
	if err != nil {
		log.Printf("Failed to start API pprof server: %v", err)
	}
	if pprofServer != nil {
		defer pprofServer.Close()
	}

	// 设置路由
	hub := worker.NewSSEHub(sqlDB)
	// Declare durable notification bindings before accepting business requests.
	if rmq != nil {
		ch, err := rmq.NewChannel()
		if err != nil {
			return fmt.Errorf("notification topology channel: %w", err)
		}
		err = worker.DeclareNotificationQueues(ch)
		_ = ch.Close()
		if err != nil {
			return fmt.Errorf("notification topology: %w", err)
		}
	}
	tasks := worker.NewTasks(ctx)
	defer tasks.Stop()
	// Closing streams on the signal lets HTTP Shutdown finish without waiting on SSE.
	tasks.Go("SSE lifecycle", func(ctx context.Context) error { <-ctx.Done(); hub.Close(); return ctx.Err() })
	tasks.Go("Notification bridge", func(ctx context.Context) error {
		return worker.WithChannel(ctx, worker.MQURL(cfg.RabbitMQ), func(ch *amqp.Channel) error {
			if err := worker.DeclareNotificationQueues(ch); err != nil {
				return err
			}
			return worker.RunNotificationBridge(ctx, ch, hub)
		})
	})
	r := apphttp.SetRouter(sqlDB, cache, rmq, hub)
	srv := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Server.Port),
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	log.Printf("Server is running on port %d", cfg.Server.Port)
	return apphttp.ServeUntilShutdown(ctx, srv, ln, 5*time.Second)
}
