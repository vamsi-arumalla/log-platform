package kafka

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/IBM/sarama"
	"github.com/vamsi-arumalla/log-platform/internal/config"
	"github.com/vamsi-arumalla/log-platform/internal/metrics"
	"github.com/vamsi-arumalla/log-platform/internal/model"
	"go.uber.org/zap"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
	logger   *zap.Logger
}

func NewProducer(cfg config.KafkaConfig, logger *zap.Logger) (*Producer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Retry.Max = 5
	saramaCfg.Producer.Retry.Backoff = 100 * time.Millisecond
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Idempotent = true
	saramaCfg.Net.MaxOpenRequests = 1
	saramaCfg.Producer.Flush.Messages = cfg.BatchSize
	saramaCfg.Producer.Flush.Frequency = cfg.FlushInterval

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("creating kafka producer: %w", err)
	}

	return &Producer{
		producer: producer,
		topic:    cfg.Topic,
		logger:   logger,
	}, nil
}

func (p *Producer) Send(entry model.LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		metrics.IngestErrors.WithLabelValues("marshal").Inc()
		return fmt.Errorf("marshaling log entry: %w", err)
	}

	partition := partitionKey(entry.Source, entry.TraceID)

	start := time.Now()
	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     p.topic,
		Key:       sarama.StringEncoder(partition),
		Value:     sarama.ByteEncoder(data),
		Timestamp: entry.Timestamp,
	})
	metrics.KafkaProduceLatency.Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.IngestErrors.WithLabelValues("kafka_produce").Inc()
		return fmt.Errorf("sending to kafka: %w", err)
	}

	return nil
}

func (p *Producer) SendBatch(entries []model.LogEntry) error {
	msgs := make([]*sarama.ProducerMessage, 0, len(entries))
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			metrics.IngestErrors.WithLabelValues("marshal").Inc()
			continue
		}
		key := partitionKey(entry.Source, entry.TraceID)
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic:     p.topic,
			Key:       sarama.StringEncoder(key),
			Value:     sarama.ByteEncoder(data),
			Timestamp: entry.Timestamp,
		})
	}

	start := time.Now()
	err := p.producer.SendMessages(msgs)
	metrics.KafkaProduceLatency.Observe(time.Since(start).Seconds())

	if err != nil {
		metrics.IngestErrors.WithLabelValues("kafka_batch_produce").Inc()
		return fmt.Errorf("batch send to kafka: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func partitionKey(source, traceID string) string {
	if traceID != "" {
		return traceID
	}
	h := fnv.New32a()
	h.Write([]byte(source))
	return fmt.Sprintf("%d", h.Sum32())
}
