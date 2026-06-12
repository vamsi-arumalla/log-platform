package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/vamsi-arumalla/log-platform/internal/config"
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

	compactor := storage.NewCompactor(
		hotStore,
		coldStore,
		cfg.Storage.CompactInterval,
		cfg.Storage.ColdThreshold,
		logger,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go compactor.Start(ctx)

	logger.Info("compactor service started")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down compactor")
	cancel()
}
