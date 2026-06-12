package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	IngestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "log_ingest_total",
		Help: "Total number of log entries ingested",
	}, []string{"source", "level"})

	IngestErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "log_ingest_errors_total",
		Help: "Total ingestion errors by type",
	}, []string{"error_type"})

	IngestLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "log_ingest_duration_seconds",
		Help:    "Ingestion latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"source"})

	KafkaLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Consumer group lag per partition",
	}, []string{"partition", "consumer_group"})

	KafkaProduceLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_produce_duration_seconds",
		Help:    "Kafka produce latency",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
	})

	QueryLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "log_query_duration_seconds",
		Help:    "Query latency distribution",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14),
	}, []string{"tier"})

	QueryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "log_query_total",
		Help: "Total queries executed",
	}, []string{"tier", "status"})

	StorageBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "log_storage_bytes",
		Help: "Storage utilization by tier",
	}, []string{"tier"})

	StorageObjectCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "log_storage_objects",
		Help: "Number of stored objects by tier",
	}, []string{"tier"})

	CompactionDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "log_compaction_duration_seconds",
		Help:    "Hot-to-cold compaction duration",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})

	BackpressureActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "log_backpressure_active",
		Help: "Whether backpressure is currently engaged (1=active, 0=inactive)",
	})

	BackpressureDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "log_backpressure_dropped_total",
		Help: "Total entries dropped due to backpressure",
	})

	ActivePartitions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_active_partitions",
		Help: "Number of actively consumed partitions",
	})
)
