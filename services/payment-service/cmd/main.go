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
	"github.com/banna/kafka-microservices/services/payment-service/internal/config"
	"github.com/banna/kafka-microservices/services/payment-service/internal/consumer"
	"github.com/banna/kafka-microservices/services/payment-service/internal/repository"
	"github.com/banna/kafka-microservices/services/payment-service/internal/service"
)

func main() {
	cfg := config.Load()

	logger := observability.NewLogger("payment-service", cfg.LogLevel)

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

	producerCfg := kafkapkg.DefaultProducerConfig(cfg.KafkaBrokers)
	writer := kafkapkg.NewWriter(producerCfg)
	defer writer.Close()

	orderReader := kafkapkg.NewReader(kafkapkg.ConsumerConfig{
		Brokers:       cfg.KafkaBrokers,
		GroupID:       kafkapkg.ConsumerGroupPayment,
		Topic:         kafkapkg.TopicOrders,
		MinBytes:      10e3,
		MaxBytes:      10e6,
		MaxWait:       5 * time.Second,
		StartOffset:   -1,
		RetentionTime: time.Hour * 24,
	})
	defer orderReader.Close()

	retryReader := kafkapkg.NewReader(kafkapkg.ConsumerConfig{
		Brokers:       cfg.KafkaBrokers,
		GroupID:       kafkapkg.ConsumerGroupPayment + "-retry",
		Topic:         kafkapkg.TopicPaymentsRetry,
		MinBytes:      10e3,
		MaxBytes:      10e6,
		MaxWait:       5 * time.Second,
		StartOffset:   -1,
		RetentionTime: time.Hour * 24,
	})
	defer retryReader.Close()

	paymentRepo := repository.NewPaymentRepository(db)
	paymentSvc := service.NewPaymentService(paymentRepo, logger)

	orderConsumer := consumer.NewOrderConsumer(orderReader, writer, paymentSvc, logger, cfg)
	retryConsumer := consumer.NewRetryConsumer(retryReader, writer, paymentSvc, logger, cfg)

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

	go orderConsumer.Start(ctx)
	go retryConsumer.Start(ctx)

	go func() {
		logger.Info("payment service starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down payment service")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	if err := writer.Close(); err != nil {
		logger.Error("kafka writer close error", "error", err)
	}
	if err := orderReader.Close(); err != nil {
		logger.Error("order reader close error", "error", err)
	}
	if err := retryReader.Close(); err != nil {
		logger.Error("retry reader close error", "error", err)
	}

	db.Close()

	fmt.Println("payment service stopped")
}
