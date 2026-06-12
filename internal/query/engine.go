package query

import (
	"context"
	"time"

	"github.com/vamsi-arumalla/log-platform/internal/metrics"
	"github.com/vamsi-arumalla/log-platform/internal/model"
	"github.com/vamsi-arumalla/log-platform/internal/storage"
	"go.uber.org/zap"
)

type Engine struct {
	hot           *storage.HotStore
	cold          *storage.ColdStore
	coldThreshold time.Duration
	logger        *zap.Logger
}

// coldThreshold must match the compactor's cold-age setting so the tier
// boundary the engine assumes is the same one the compactor enforces.
func NewEngine(hot *storage.HotStore, cold *storage.ColdStore, coldThreshold time.Duration, logger *zap.Logger) *Engine {
	return &Engine{
		hot:           hot,
		cold:          cold,
		coldThreshold: coldThreshold,
		logger:        logger,
	}
}

func (e *Engine) Execute(ctx context.Context, req model.QueryRequest) (*model.QueryResponse, error) {
	start := time.Now()

	hotCutoff := time.Now().Add(-e.coldThreshold)
	needsCold := req.StartTime.Before(hotCutoff)
	needsHot := req.EndTime.After(hotCutoff)

	var allEntries []model.LogEntry
	tier := "hot"

	if needsHot {
		hotStart := time.Now()
		hotEntries, err := e.hot.Query(ctx, req)
		if err != nil {
			metrics.QueryTotal.WithLabelValues("hot", "error").Inc()
			return nil, err
		}
		allEntries = append(allEntries, hotEntries...)
		e.logger.Debug("hot query complete",
			zap.Int("results", len(hotEntries)),
			zap.Duration("duration", time.Since(hotStart)),
		)
	}

	if needsCold {
		tier = "cold"
		coldStart := time.Now()
		coldReq := req
		if needsHot {
			coldReq.EndTime = hotCutoff
		}
		coldEntries, err := e.cold.Query(ctx, coldReq)
		if err != nil {
			metrics.QueryTotal.WithLabelValues("cold", "error").Inc()
			return nil, err
		}
		allEntries = append(allEntries, coldEntries...)
		e.logger.Debug("cold query complete",
			zap.Int("results", len(coldEntries)),
			zap.Duration("duration", time.Since(coldStart)),
		)
	}

	if needsHot && needsCold {
		tier = "tiered"
	}

	if req.Limit > 0 && len(allEntries) > req.Limit {
		allEntries = allEntries[:req.Limit]
	}

	duration := time.Since(start)
	metrics.QueryLatency.WithLabelValues(tier).Observe(duration.Seconds())
	metrics.QueryTotal.WithLabelValues(tier, "success").Inc()

	return &model.QueryResponse{
		Entries:    allEntries,
		TotalCount: len(allEntries),
		QueryTime:  float64(duration.Milliseconds()),
		Tier:       tier,
	}, nil
}
