package storage

import (
	"context"
	"time"

	"github.com/vamsi-arumalla/log-platform/internal/metrics"
	"go.uber.org/zap"
)

type Compactor struct {
	hot      *HotStore
	cold     *ColdStore
	interval time.Duration
	coldAge  time.Duration
	logger   *zap.Logger
}

func NewCompactor(hot *HotStore, cold *ColdStore, interval, coldAge time.Duration, logger *zap.Logger) *Compactor {
	return &Compactor{
		hot:      hot,
		cold:     cold,
		interval: interval,
		coldAge:  coldAge,
		logger:   logger,
	}
}

func (c *Compactor) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	c.logger.Info("compactor started",
		zap.Duration("interval", c.interval),
		zap.Duration("cold_age", c.coldAge),
	)

	for {
		select {
		case <-ticker.C:
			c.compact(ctx)
		case <-ctx.Done():
			c.logger.Info("compactor stopped")
			return
		}
	}
}

func (c *Compactor) compact(ctx context.Context) {
	start := time.Now()
	cutoff := time.Now().Add(-c.coldAge)

	drained := c.hot.Drain(cutoff)
	if len(drained) == 0 {
		return
	}

	c.logger.Info("compacting hot → cold",
		zap.Int("entries", len(drained)),
		zap.Time("cutoff", cutoff),
	)

	batchSize := 10000
	for i := 0; i < len(drained); i += batchSize {
		end := i + batchSize
		if end > len(drained) {
			end = len(drained)
		}

		if err := c.cold.Archive(ctx, drained[i:end]); err != nil {
			c.logger.Error("compaction archive failed",
				zap.Int("batch_start", i),
				zap.Int("batch_end", end),
				zap.Error(err),
			)
			continue
		}
	}

	duration := time.Since(start)
	metrics.CompactionDuration.Observe(duration.Seconds())

	c.logger.Info("compaction complete",
		zap.Int("entries", len(drained)),
		zap.Duration("duration", duration),
	)
}
