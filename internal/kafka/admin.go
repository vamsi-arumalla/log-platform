package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/vamsi-arumalla/log-platform/internal/config"
	"go.uber.org/zap"
)

func EnsureTopic(cfg config.KafkaConfig, logger *zap.Logger) error {
	admin, err := sarama.NewClusterAdmin(cfg.Brokers, sarama.NewConfig())
	if err != nil {
		return fmt.Errorf("creating cluster admin: %w", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("listing topics: %w", err)
	}

	if _, exists := topics[cfg.Topic]; exists {
		logger.Info("topic already exists", zap.String("topic", cfg.Topic))
		return nil
	}

	err = admin.CreateTopic(cfg.Topic, &sarama.TopicDetail{
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.Replication,
		ConfigEntries: map[string]*string{
			"retention.ms":          strPtr("86400000"),
			"cleanup.policy":        strPtr("delete"),
			"min.insync.replicas":   strPtr("2"),
			"compression.type":      strPtr("lz4"),
			"segment.bytes":         strPtr("536870912"),
			"max.message.bytes":     strPtr("10485760"),
		},
	}, false)
	if err != nil {
		return fmt.Errorf("creating topic %s: %w", cfg.Topic, err)
	}

	logger.Info("topic created",
		zap.String("topic", cfg.Topic),
		zap.Int32("partitions", cfg.Partitions),
		zap.Int16("replication", cfg.Replication),
	)
	return nil
}

func strPtr(s string) *string { return &s }
