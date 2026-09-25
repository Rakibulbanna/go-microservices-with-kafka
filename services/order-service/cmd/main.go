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

	"github.com/banna/kafka-microservices/pkg/kafka"
	"github.com/banna/kafka-microservices/pkg/observability"
	"github.com/banna/kafka-microservices/services/order-service/internal/config"
	"github.com/banna/kafka-microservices/services/order-service/internal/handler"
	"github.com/banna/kafka-microservices/services/order-service/internal/outbox"

	"github.com/banna/kafka-microservices/services/order-service/internal/repository"
	"github.com/banna/kafka-microservices/services/order-service/internal/service"
)

func main() {
	cfg := config.Load()

	logger := observability.NewLogger("order-service", cfg.LogLevel)

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

	producerCfg := kafka.DefaultProducerConfig(cfg.KafkaBrokers)
	writer := kafka.NewWriter(producerCfg)
	defer writer.Close()

	orderRepo := repository.NewOrderRepository(db)
	outboxRepo := repository.NewOutboxRepository(db)
	orderService := service.NewOrderService(orderRepo, outboxRepo, db, logger)

	outboxPublisher := outbox.NewPublisher(outboxRepo, writer, logger, 2*time.Second, 50)

	orderHandler := handler.NewOrderHandler(orderService, logger)

	r := mux.NewRouter()
	orderHandler.RegisterRoutes(r)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go outboxPublisher.Start(ctx)

	go func() {
		logger.Info("order service starting",
			"port", cfg.Port,
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down order service")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	if err := writer.Close(); err != nil {
		logger.Error("kafka writer close error", "error", err)
	}

	db.Close()

	fmt.Println("order service stopped")
}
