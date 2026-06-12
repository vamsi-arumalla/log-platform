package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vamsi-arumalla/log-platform/internal/model"
	"go.uber.org/zap"
)

func newTestStore(t *testing.T) *HotStore {
	t.Helper()
	return NewHotStore(time.Hour, zap.NewNop())
}

func makeBatch(n int, source string, ts time.Time) model.LogBatch {
	entries := make([]model.LogEntry, n)
	for i := range entries {
		entries[i] = model.LogEntry{
			ID:        fmt.Sprintf("%s-%d", source, i),
			Timestamp: ts.Add(time.Duration(i) * time.Millisecond),
			Source:    source,
			Level:     model.LevelInfo,
			Message:   fmt.Sprintf("message %d from %s", i, source),
		}
	}
	return model.LogBatch{Entries: entries}
}

func TestHotStoreStoreAndQuery(t *testing.T) {
	store := newTestStore(t)
	now := time.Now()

	if err := store.Store(context.Background(), makeBatch(100, "api", now)); err != nil {
		t.Fatalf("store failed: %v", err)
	}

	results, err := store.Query(context.Background(), model.QueryRequest{
		StartTime: now.Add(-time.Minute),
		EndTime:   now.Add(time.Minute),
		Source:    "api",
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(results) != 100 {
		t.Errorf("expected 100 results, got %d", len(results))
	}
}

func TestHotStoreQueryFilters(t *testing.T) {
	store := newTestStore(t)
	now := time.Now()

	store.Store(context.Background(), makeBatch(50, "api", now))
	store.Store(context.Background(), makeBatch(50, "worker", now))

	results, _ := store.Query(context.Background(), model.QueryRequest{
		StartTime: now.Add(-time.Minute),
		EndTime:   now.Add(time.Minute),
		Source:    "worker",
	})
	if len(results) != 50 {
		t.Errorf("expected 50 worker results, got %d", len(results))
	}

	results, _ = store.Query(context.Background(), model.QueryRequest{
		StartTime: now.Add(-time.Minute),
		EndTime:   now.Add(time.Minute),
		Query:     "message 7 from api",
	})
	if len(results) != 1 {
		t.Errorf("expected 1 text-match result, got %d", len(results))
	}
}

func TestHotStoreQueryTimeWindow(t *testing.T) {
	store := newTestStore(t)
	now := time.Now()

	store.Store(context.Background(), makeBatch(10, "api", now.Add(-2*time.Hour)))
	store.Store(context.Background(), makeBatch(10, "api", now))

	results, _ := store.Query(context.Background(), model.QueryRequest{
		StartTime: now.Add(-time.Minute),
		EndTime:   now.Add(time.Minute),
	})
	if len(results) != 10 {
		t.Errorf("expected 10 recent results, got %d", len(results))
	}
}

func TestHotStoreDrain(t *testing.T) {
	store := newTestStore(t)
	now := time.Now()

	store.Store(context.Background(), makeBatch(30, "api", now.Add(-2*time.Hour)))
	store.Store(context.Background(), makeBatch(20, "api", now))

	drained := store.Drain(now.Add(-time.Hour))
	if len(drained) != 30 {
		t.Errorf("expected 30 drained entries, got %d", len(drained))
	}
	if store.Count() != 20 {
		t.Errorf("expected 20 remaining entries, got %d", store.Count())
	}

	// remaining entries must still be queryable after reindex
	results, _ := store.Query(context.Background(), model.QueryRequest{
		StartTime: now.Add(-time.Minute),
		EndTime:   now.Add(time.Minute),
		Source:    "api",
	})
	if len(results) != 20 {
		t.Errorf("expected 20 queryable results after drain, got %d", len(results))
	}
}

func TestHotStoreLimit(t *testing.T) {
	store := newTestStore(t)
	now := time.Now()

	store.Store(context.Background(), makeBatch(500, "api", now))

	results, _ := store.Query(context.Background(), model.QueryRequest{
		StartTime: now.Add(-time.Minute),
		EndTime:   now.Add(time.Minute),
		Limit:     25,
	})
	if len(results) != 25 {
		t.Errorf("expected limit of 25, got %d", len(results))
	}
}
