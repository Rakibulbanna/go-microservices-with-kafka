package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"

	kafkapkg "github.com/banna/kafka-microservices/pkg/kafka"
	"github.com/banna/kafka-microservices/pkg/observability"
	"github.com/banna/kafka-microservices/services/notification-service/internal/config"
	"github.com/banna/kafka-microservices/services/notification-service/internal/consumer"
	"github.com/banna/kafka-microservices/services/notification-service/internal/repository"
	"github.com/banna/kafka-microservices/services/notification-service/internal/service"
)

func main() {
	cfg := config.Load()

	logger := observability.NewLogger("notification-service", cfg.LogLevel)

	db, err := sql.Open("postgres", cfg.DSN())
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := repository.Migrate(ctx, db); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}
	logger.Info("database migrations completed")

	ordersReader := kafkapkg.NewReader(kafkapkg.ConsumerConfig{
		Brokers:       cfg.KafkaBrokers,
		GroupID:       kafkapkg.ConsumerGroupNotification,
		Topic:         kafkapkg.TopicOrders,
		MinBytes:      10e3,
		MaxBytes:      10e6,
		MaxWait:       5 * time.Second,
		StartOffset:   -1,
		RetentionTime: time.Hour * 24,
	})
	defer ordersReader.Close()

	paymentsReader := kafkapkg.NewReader(kafkapkg.ConsumerConfig{
		Brokers:       cfg.KafkaBrokers,
		GroupID:       kafkapkg.ConsumerGroupNotification,
		Topic:         kafkapkg.TopicPayments,
		MinBytes:      10e3,
		MaxBytes:      10e6,
		MaxWait:       5 * time.Second,
		StartOffset:   -1,
		RetentionTime: time.Hour * 24,
	})
	defer paymentsReader.Close()

	notificationRepo := repository.NewNotificationRepository(db)
	notificationSvc := service.NewNotificationService(notificationRepo, logger)

	ordersConsumer := consumer.NewEventConsumer(ordersReader, notificationSvc, logger, cfg)
	paymentsConsumer := consumer.NewEventConsumer(paymentsReader, notificationSvc, logger, cfg)

	r := mux.NewRouter()
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	}).Methods("GET")

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go ordersConsumer.Start(ctx)
	go paymentsConsumer.Start(ctx)

	go func() {
		logger.Info("notification service starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down notification service")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	if err := ordersReader.Close(); err != nil {
		logger.Error("orders reader close error", "error", err)
	}
	if err := paymentsReader.Close(); err != nil {
		logger.Error("payments reader close error", "error", err)
	}

	db.Close()

	fmt.Println("notification service stopped")
}
