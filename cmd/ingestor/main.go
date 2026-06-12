package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vamsi-arumalla/log-platform/internal/backpressure"
	"github.com/vamsi-arumalla/log-platform/internal/config"
	"github.com/vamsi-arumalla/log-platform/internal/kafka"
	"github.com/vamsi-arumalla/log-platform/internal/metrics"
	"github.com/vamsi-arumalla/log-platform/internal/model"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	if err := kafka.EnsureTopic(cfg.Kafka, logger); err != nil {
		logger.Warn("topic setup failed (may already exist)", zap.Error(err))
	}

	producer, err := kafka.NewProducer(cfg.Kafka, logger)
	if err != nil {
		logger.Fatal("failed to create kafka producer", zap.Error(err))
	}
	defer producer.Close()

	bp := backpressure.NewController(50000, 30*time.Second, logger)

	r := mux.NewRouter()
	r.HandleFunc("/ingest", handleIngest(producer, bp, logger)).Methods("POST")
	r.HandleFunc("/ingest/batch", handleBatchIngest(producer, bp, logger)).Methods("POST")
	r.HandleFunc("/health", handleHealth(producer)).Methods("GET")
	r.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         ":" + cfg.Server.IngestPort,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("ingestor started", zap.String("port", cfg.Server.IngestPort))
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("server error", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down ingestor")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

func handleIngest(producer *kafka.Producer, bp *backpressure.Controller, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !bp.ShouldAccept() {
			http.Error(w, `{"error":"backpressure: server overloaded"}`, http.StatusServiceUnavailable)
			return
		}

		var entry model.LogEntry
		if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
			metrics.IngestErrors.WithLabelValues("decode").Inc()
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
			return
		}

		if entry.Timestamp.IsZero() {
			entry.Timestamp = time.Now().UTC()
		}

		start := time.Now()
		bp.IncrementDepth(1)
		defer bp.DecrementDepth(1)

		if err := producer.Send(entry); err != nil {
			logger.Error("failed to produce message", zap.Error(err))
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}

		metrics.IngestTotal.WithLabelValues(entry.Source, string(entry.Level)).Inc()
		metrics.IngestLatency.WithLabelValues(entry.Source).Observe(time.Since(start).Seconds())

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
	}
}

func handleBatchIngest(producer *kafka.Producer, bp *backpressure.Controller, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !bp.ShouldAccept() {
			http.Error(w, `{"error":"backpressure: server overloaded"}`, http.StatusServiceUnavailable)
			return
		}

		var entries []model.LogEntry
		if err := json.NewDecoder(r.Body).Decode(&entries); err != nil {
			metrics.IngestErrors.WithLabelValues("decode").Inc()
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
			return
		}

		now := time.Now().UTC()
		for i := range entries {
			if entries[i].Timestamp.IsZero() {
				entries[i].Timestamp = now
			}
		}

		start := time.Now()
		bp.IncrementDepth(int64(len(entries)))
		defer bp.DecrementDepth(int64(len(entries)))

		if err := producer.SendBatch(entries); err != nil {
			logger.Error("failed to produce batch", zap.Error(err))
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}

		for _, entry := range entries {
			metrics.IngestTotal.WithLabelValues(entry.Source, string(entry.Level)).Inc()
		}
		metrics.IngestLatency.WithLabelValues("batch").Observe(time.Since(start).Seconds())

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "accepted",
			"ingested": len(entries),
		})
	}
}

func handleHealth(producer *kafka.Producer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "healthy",
			"service": "ingestor",
		})
	}
}
