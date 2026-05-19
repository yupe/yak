package config

import "time"

type BatchConfig struct {
	*ConsumerConfig
	BatchSize        int           `yaml:"batch_size" json:"batch_size"`
	BatchTimeout     time.Duration `yaml:"batch_timeout" json:"batch_timeout"`
	MaxBatchWaitTime time.Duration `yaml:"max_batch_wait" json:"max_batch_wait"`
	GroupID          string        `yaml:"group_id" json:"group_id"`
}

func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		ConsumerConfig:   DefaultConsumerConfig(),
		BatchSize:        10,
		BatchTimeout:     1 * time.Second,
		MaxBatchWaitTime: 5 * time.Second,
		GroupID:          "yandex-consumer-group-batch",
	}
}
