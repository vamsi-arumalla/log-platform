package storage

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vamsi-arumalla/log-platform/internal/metrics"
	"github.com/vamsi-arumalla/log-platform/internal/model"
	"go.uber.org/zap"
)

type HotStore struct {
	mu        sync.RWMutex
	entries   []model.LogEntry
	index     map[string][]int // source -> entry indices
	retention time.Duration
	logger    *zap.Logger
}

func NewHotStore(retention time.Duration, logger *zap.Logger) *HotStore {
	h := &HotStore{
		entries:   make([]model.LogEntry, 0, 100000),
		index:     make(map[string][]int),
		retention: retention,
		logger:    logger,
	}
	go h.evictionLoop()
	return h
}

func (h *HotStore) Store(ctx context.Context, batch model.LogBatch) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, entry := range batch.Entries {
		idx := len(h.entries)
		h.entries = append(h.entries, entry)
		h.index[entry.Source] = append(h.index[entry.Source], idx)
	}

	metrics.StorageObjectCount.WithLabelValues("hot").Set(float64(len(h.entries)))
	return nil
}

func (h *HotStore) Query(ctx context.Context, req model.QueryRequest) ([]model.LogEntry, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var results []model.LogEntry
	limit := req.Limit
	if limit == 0 {
		limit = 1000
	}

	var candidates []model.LogEntry
	if req.Source != "" {
		indices, ok := h.index[req.Source]
		if !ok {
			return nil, nil
		}
		candidates = make([]model.LogEntry, 0, len(indices))
		for _, idx := range indices {
			if idx < len(h.entries) {
				candidates = append(candidates, h.entries[idx])
			}
		}
	} else {
		candidates = h.entries
	}

	for _, entry := range candidates {
		if entry.Timestamp.Before(req.StartTime) || entry.Timestamp.After(req.EndTime) {
			continue
		}
		if req.Level != "" && entry.Level != req.Level {
			continue
		}
		if req.Query != "" && !strings.Contains(entry.Message, req.Query) {
			continue
		}
		if !matchLabels(entry.Labels, req.Labels) {
			continue
		}
		results = append(results, entry)
		if len(results) >= limit {
			break
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp.After(results[j].Timestamp)
	})

	return results, nil
}

func (h *HotStore) Drain(before time.Time) []model.LogEntry {
	h.mu.Lock()
	defer h.mu.Unlock()

	var drained, remaining []model.LogEntry
	newIndex := make(map[string][]int)

	for _, entry := range h.entries {
		if entry.Timestamp.Before(before) {
			drained = append(drained, entry)
		} else {
			idx := len(remaining)
			remaining = append(remaining, entry)
			newIndex[entry.Source] = append(newIndex[entry.Source], idx)
		}
	}

	h.entries = remaining
	h.index = newIndex
	metrics.StorageObjectCount.WithLabelValues("hot").Set(float64(len(h.entries)))

	return drained
}

func (h *HotStore) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

func (h *HotStore) evictionLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cutoff := time.Now().Add(-h.retention)
		evicted := h.Drain(cutoff)
		if len(evicted) > 0 {
			h.logger.Info("evicted expired entries from hot store",
				zap.Int("count", len(evicted)),
			)
		}
	}
}

func matchLabels(entryLabels, queryLabels map[string]string) bool {
	for k, v := range queryLabels {
		if entryLabels[k] != v {
			return false
		}
	}
	return true
}
