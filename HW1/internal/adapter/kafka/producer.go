package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"yap/kafka/config"
)

type Producer struct {
	producer *kafka.Producer
	config   *config.Config
}

func NewProducer(config *config.Config) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":   config.BootstrapServers,
		"client.id":           config.ClientID,
		"acks":                config.Acks,
		"retries":             config.Retries,
		"retry.backoff.ms":    100,
		"delivery.timeout.ms": 120000,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	producer := &Producer{
		producer: p,
		config:   config,
	}

	return producer, nil
}

func (p *Producer) Send(data []byte) error {
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.config.Topic,
			Partition: kafka.PartitionAny,
		},
		Value: data,
	}, nil)

	if err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	p.producer.Flush(15)

	return nil
}

func (p *Producer) Close() {
	p.producer.Close()
}
