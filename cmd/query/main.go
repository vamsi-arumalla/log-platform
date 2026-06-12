package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vamsi-arumalla/log-platform/internal/config"
	"github.com/vamsi-arumalla/log-platform/internal/kafka"
	"github.com/vamsi-arumalla/log-platform/internal/model"
	"github.com/vamsi-arumalla/log-platform/internal/query"
	"github.com/vamsi-arumalla/log-platform/internal/storage"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg := config.Load()

	hotStore := storage.NewHotStore(cfg.Storage.HotRetention, logger)

	coldStore, err := storage.NewColdStore(cfg.S3, logger)
	if err != nil {
		logger.Fatal("failed to create cold store", zap.Error(err))
	}

	engine := query.NewEngine(hotStore, coldStore, cfg.Storage.ColdThreshold, logger)

	// Compaction must run in-process: it drains this instance's hot store
	// into S3, so a separate compactor binary would only ever see an empty store.
	compactor := storage.NewCompactor(
		hotStore,
		coldStore,
		cfg.Storage.CompactInterval,
		cfg.Storage.ColdThreshold,
		logger,
	)

	handler := func(ctx context.Context, batch model.LogBatch) error {
		return hotStore.Store(ctx, batch)
	}

	consumer, err := kafka.NewConsumer(cfg.Kafka, handler, logger)
	if err != nil {
		logger.Fatal("failed to create kafka consumer", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := consumer.Start(ctx); err != nil {
			logger.Error("consumer error", zap.Error(err))
		}
	}()

	go compactor.Start(ctx)

	r := mux.NewRouter()
	r.HandleFunc("/query", handleQuery(engine, logger)).Methods("POST")
	r.HandleFunc("/health", handleHealth()).Methods("GET")
	r.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         ":" + cfg.Server.QueryPort,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logger.Info("query service started", zap.String("port", cfg.Server.QueryPort))
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("server error", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down query service")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)
}

func handleQuery(engine *query.Engine, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req model.QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
			return
		}

		if req.StartTime.IsZero() {
			req.StartTime = time.Now().Add(-1 * time.Hour)
		}
		if req.EndTime.IsZero() {
			req.EndTime = time.Now()
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()

		resp, err := engine.Execute(ctx, req)
		if err != nil {
			logger.Error("query failed", zap.Error(err))
			http.Error(w, `{"error":"query execution failed"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

func handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "healthy",
			"service": "query",
		})
	}
}
