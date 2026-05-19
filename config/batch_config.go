package config

import "time"

type BatchConfig struct {
	*ConsumerConfig
	BatchSize        int           `yaml:"batch_size" json:"batch_size"`       // размер батча
	BatchTimeout     time.Duration `yaml:"batch_timeout" json:"batch_timeout"` // таймаут накопления
	MaxBatchWaitTime time.Duration `yaml:"max_batch_wait" json:"max_batch_wait"`
}

func DefaultBatchConfig() *BatchConfig {
	return &BatchConfig{
		ConsumerConfig:   DefaultConsumerConfig(),
		BatchSize:        100,             // 100 сообщений в батче
		BatchTimeout:     1 * time.Second, // ждём максимум 1 секунду
		MaxBatchWaitTime: 5 * time.Second,
	}
}
