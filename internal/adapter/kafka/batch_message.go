package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"yap/kafka/config"
	"yap/kafka/internal/entity"
)

const timeoutMsBatch = 100

type BatchMessageConsumer struct {
	config   *config.BatchConfig
	consumer *kafka.Consumer
}

func NewBatchMessageConsumer(config *config.BatchConfig) (*BatchMessageConsumer, error) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":         config.BootstrapServers,
		"group.id":                  config.GroupID,
		"client.id":                 config.ClientID,
		"enable.auto.commit":        false,
		"auto.offset.reset":         config.AutoOffsetReset,
		"fetch.min.bytes":           1024 * 100,
		"max.partition.fetch.bytes": 1024 * 1024,
	})

	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics([]string{config.Topic}, nil)
	if err != nil {
		c.Close()
		return nil, err
	}

	return &BatchMessageConsumer{
		config:   config,
		consumer: c,
	}, err
}

func (s *BatchMessageConsumer) ConsumeMessages() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	batchSize := s.config.BatchSize
	batchTimeout := s.config.BatchTimeout

	batch := make([]*kafka.Message, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	processBatch := func(messages []*kafka.Message) {
		if len(messages) == 0 {
			return
		}

		fmt.Println("processing batch", len(messages))

		var lastOffset int64
		var lastPartition int32
		var lastTopic string

		for _, message := range messages {
			value := entity.Order{}
			err := json.Unmarshal(message.Value, &value)
			if err != nil {
				fmt.Printf("Error unmarshal: %v\n", err)
				continue
			}
			fmt.Printf("Order: %+v\n", value)

			lastOffset = int64(message.TopicPartition.Offset)
			lastPartition = message.TopicPartition.Partition
			if message.TopicPartition.Topic != nil {
				lastTopic = *message.TopicPartition.Topic
			}
		}

		if lastTopic != "" {
			_, err := s.consumer.CommitOffsets([]kafka.TopicPartition{{
				Topic:     &lastTopic,
				Partition: lastPartition,
				Offset:    kafka.Offset(lastOffset + 1),
			}})

			if err != nil {
				log.Printf("Failed to commit offsets: %v", err)
			} else {
				fmt.Printf("Committed batch up to offset %d on partition %d\n", lastOffset, lastPartition)
			}
		}

		fmt.Printf("batch processing stop\n")
	}

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		case <-ticker.C:
			if len(batch) > 0 {
				fmt.Printf("Batch timeout reached, processing %d messages\n", len(batch))
				processBatch(batch)
				batch = make([]*kafka.Message, 0, batchSize)
			}
		default:
			ev := s.consumer.Poll(timeoutMsBatch)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case kafka.OffsetsCommitted:
				if e.Error != nil {
					log.Printf("Failed to commit offsets: %v", e.Error)
				} else {
					log.Printf("Offsets committed successfully: %v", e.Offsets)
				}
			case *kafka.Message:
				batch = append(batch, e)
				if len(batch) >= batchSize {
					fmt.Printf("Batch size reached (%d), processing...\n", batchSize)
					processBatch(batch)
					batch = make([]*kafka.Message, 0, batchSize)
					ticker.Reset(batchTimeout)
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

}

func (s *BatchMessageConsumer) Close() {
	s.consumer.Close()
}
