package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"

	kafkapkg "github.com/banna/kafka-microservices/pkg/kafka"
	"github.com/banna/kafka-microservices/pkg/observability"
	"github.com/banna/kafka-microservices/services/analytics-service/internal/config"
	"github.com/banna/kafka-microservices/services/analytics-service/internal/consumer"
)

func main() {
	cfg := config.Load()

	logger := observability.NewLogger("analytics-service", cfg.LogLevel)

	logger.Info("starting analytics service (Solution A: NO DATABASE - pure Kafka)",
		"port", cfg.Port,
	)

	producerCfg := kafkapkg.DefaultProducerConfig(cfg.KafkaBrokers)
	writer := kafkapkg.NewWriter(producerCfg)
	defer writer.Close()

	reader := kafkapkg.NewReader(kafkapkg.ConsumerConfig{
		Brokers:       cfg.KafkaBrokers,
		GroupID:       "analytics-service",
		Topic:         kafkapkg.TopicOrders,
		MinBytes:      10e3,
		MaxBytes:      10e6,
		MaxWait:       5 * time.Second,
		StartOffset:   -1,
		RetentionTime: time.Hour * 24,
	})
	defer reader.Close()

	orderConsumer := consumer.NewOrderConsumer(reader, writer, logger)

	r := mux.NewRouter()
	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","note":"no database - pure Kafka flow"}`))
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

	go func() {
		logger.Info("analytics service starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("failed to start server: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down analytics service")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	if err := writer.Close(); err != nil {
		logger.Error("kafka writer close error", "error", err)
	}
	if err := reader.Close(); err != nil {
		logger.Error("kafka reader close error", "error", err)
	}

	fmt.Println("analytics service stopped")
}
