package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Kafka    KafkaConfig
	S3       S3Config
	Server   ServerConfig
	Metrics  MetricsConfig
	Storage  StorageConfig
}

type KafkaConfig struct {
	Brokers       []string
	Topic         string
	ConsumerGroup string
	Partitions    int32
	Replication   int16
	BatchSize     int
	FlushInterval time.Duration
}

type S3Config struct {
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
	UseSSL    bool
}

type ServerConfig struct {
	IngestPort string
	QueryPort  string
}

type MetricsConfig struct {
	Port string
}

type StorageConfig struct {
	HotRetention  time.Duration
	ColdThreshold time.Duration
	CompactInterval time.Duration
}

func Load() *Config {
	return &Config{
		Kafka: KafkaConfig{
			Brokers:       strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ","),
			Topic:         envOrDefault("KAFKA_TOPIC", "logs"),
			ConsumerGroup: envOrDefault("KAFKA_CONSUMER_GROUP", "log-processors"),
			Partitions:    int32(envOrDefaultInt("KAFKA_PARTITIONS", 12)),
			Replication:   int16(envOrDefaultInt("KAFKA_REPLICATION", 3)),
			BatchSize:     envOrDefaultInt("KAFKA_BATCH_SIZE", 500),
			FlushInterval: envOrDefaultDuration("KAFKA_FLUSH_INTERVAL", 5*time.Second),
		},
		S3: S3Config{
			Endpoint:  envOrDefault("S3_ENDPOINT", "http://localhost:9000"),
			Bucket:    envOrDefault("S3_BUCKET", "log-archive"),
			Region:    envOrDefault("S3_REGION", "us-east-1"),
			AccessKey: envOrDefault("S3_ACCESS_KEY", "minioadmin"),
			SecretKey: envOrDefault("S3_SECRET_KEY", "minioadmin"),
			UseSSL:    envOrDefault("S3_USE_SSL", "false") == "true",
		},
		Server: ServerConfig{
			IngestPort: envOrDefault("INGEST_PORT", "8080"),
			QueryPort:  envOrDefault("QUERY_PORT", "8081"),
		},
		Metrics: MetricsConfig{
			Port: envOrDefault("METRICS_PORT", "9090"),
		},
		Storage: StorageConfig{
			HotRetention:    envOrDefaultDuration("HOT_RETENTION", 24*time.Hour),
			ColdThreshold:   envOrDefaultDuration("COLD_THRESHOLD", 6*time.Hour),
			CompactInterval: envOrDefaultDuration("COMPACT_INTERVAL", 30*time.Minute),
		},
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func envOrDefaultDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
