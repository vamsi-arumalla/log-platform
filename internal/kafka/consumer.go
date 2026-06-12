package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/vamsi-arumalla/log-platform/internal/config"
	"github.com/vamsi-arumalla/log-platform/internal/metrics"
	"github.com/vamsi-arumalla/log-platform/internal/model"
	"go.uber.org/zap"
)

type MessageHandler func(ctx context.Context, batch model.LogBatch) error

type Consumer struct {
	group         sarama.ConsumerGroup
	topic         string
	consumerGroup string
	handler       MessageHandler
	logger        *zap.Logger
}

func NewConsumer(cfg config.KafkaConfig, handler MessageHandler, logger *zap.Logger) (*Consumer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = false

	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("creating consumer group: %w", err)
	}

	return &Consumer{
		group:         group,
		topic:         cfg.Topic,
		consumerGroup: cfg.ConsumerGroup,
		handler:       handler,
		logger:        logger,
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	handler := &consumerGroupHandler{
		topic:         c.topic,
		consumerGroup: c.consumerGroup,
		handler:       c.handler,
		logger:        c.logger,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Consume blocks through a full session; loop to rejoin after rebalances.
			if err := c.group.Consume(ctx, []string{c.topic}, handler); err != nil {
				c.logger.Error("consumer error", zap.Error(err))
				time.Sleep(time.Second)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-ctx.Done()
	wg.Wait()
	return c.group.Close()
}

type consumerGroupHandler struct {
	topic         string
	consumerGroup string
	handler       MessageHandler
	logger        *zap.Logger
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	partitions := session.Claims()[h.topic]
	metrics.ActivePartitions.Set(float64(len(partitions)))
	h.logger.Info("partition assignment",
		zap.Int32s("partitions", partitions),
		zap.String("member_id", session.MemberID()),
	)
	return nil
}

func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const batchSize = 100
	batch := make([]model.LogEntry, 0, batchSize)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	partition := claim.Partition()
	var lastOffset int64

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				if len(batch) > 0 {
					h.flushBatch(session, batch, partition, lastOffset)
				}
				return nil
			}

			var entry model.LogEntry
			if err := json.Unmarshal(msg.Value, &entry); err != nil {
				metrics.IngestErrors.WithLabelValues("unmarshal").Inc()
				h.logger.Warn("failed to unmarshal message", zap.Error(err))
				session.MarkMessage(msg, "")
				continue
			}

			batch = append(batch, entry)
			lastOffset = msg.Offset

			lag := claim.HighWaterMarkOffset() - msg.Offset - 1
			if lag < 0 {
				lag = 0
			}
			metrics.KafkaLag.WithLabelValues(
				strconv.Itoa(int(partition)),
				h.consumerGroup,
			).Set(float64(lag))

			if len(batch) >= batchSize {
				h.flushBatch(session, batch, partition, lastOffset)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				h.flushBatch(session, batch, partition, lastOffset)
				batch = batch[:0]
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

// flushBatch hands the batch to the handler and only then commits the offset:
// a crash before commit means redelivery, never loss (at-least-once).
func (h *consumerGroupHandler) flushBatch(session sarama.ConsumerGroupSession, entries []model.LogEntry, partition int32, offset int64) {
	logBatch := model.LogBatch{
		Entries:   append([]model.LogEntry(nil), entries...),
		Partition: partition,
		Offset:    offset,
	}

	ctx, cancel := context.WithTimeout(session.Context(), 10*time.Second)
	defer cancel()

	if err := h.handler(ctx, logBatch); err != nil {
		h.logger.Error("failed to process batch",
			zap.Int32("partition", partition),
			zap.Int64("offset", offset),
			zap.Int("size", len(entries)),
			zap.Error(err),
		)
		return
	}

	session.MarkOffset(h.topic, partition, offset+1, "")
	session.Commit()
}
