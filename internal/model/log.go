package model

import "time"

type LogEntry struct {
	ID        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	Source    string            `json:"source"`
	Level     LogLevel          `json:"level"`
	Message   string            `json:"message"`
	Labels    map[string]string `json:"labels,omitempty"`
	TraceID   string            `json:"trace_id,omitempty"`
	SpanID    string            `json:"span_id,omitempty"`
}

type LogLevel string

const (
	LevelDebug LogLevel = "DEBUG"
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
	LevelFatal LogLevel = "FATAL"
)

type LogBatch struct {
	Entries   []LogEntry `json:"entries"`
	Partition int32      `json:"partition"`
	Offset    int64      `json:"offset"`
}

type QueryRequest struct {
	StartTime time.Time         `json:"start_time"`
	EndTime   time.Time         `json:"end_time"`
	Source    string            `json:"source,omitempty"`
	Level     LogLevel          `json:"level,omitempty"`
	Query     string            `json:"query,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Limit     int               `json:"limit,omitempty"`
}

type QueryResponse struct {
	Entries    []LogEntry `json:"entries"`
	TotalCount int        `json:"total_count"`
	QueryTime  float64    `json:"query_time_ms"`
	Tier       string     `json:"tier"`
}
