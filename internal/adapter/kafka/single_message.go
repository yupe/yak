package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"yap/kafka/config"
	"yap/kafka/internal/entity"
)

const timeoutMs = 100

type SingleMessageConsumer struct {
	config   *config.ConsumerConfig
	consumer *kafka.Consumer
}

func NewSingleMessageConsumer(config *config.ConsumerConfig) (*SingleMessageConsumer, error) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  config.BootstrapServers,
		"group.id":           config.GroupID,
		"client.id":          config.ClientID,
		"enable.auto.commit": config.EnableAutoCommit,
		"auto.offset.reset":  config.AutoOffsetReset,
	})

	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics([]string{config.Topic}, nil)
	if err != nil {
		c.Close()
		return nil, err
	}

	return &SingleMessageConsumer{
		config:   config,
		consumer: c,
	}, err
}

func (s *SingleMessageConsumer) ConsumeMessages() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := s.consumer.Poll(timeoutMs)
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
				value := entity.Order{}
				err := json.Unmarshal(e.Value, &value)
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Printf("Message on %s: %s\n", e.TopicPartition, e.Value)
					order := entity.Order{}
					err = json.Unmarshal(e.Value, &order)
					if err != nil {
						fmt.Printf("Error unmarshal %v\n", err)
					}
					fmt.Printf("Order: %+v\n", order)
				}

				if e.Headers != nil {
					fmt.Printf("Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

}

func (s *SingleMessageConsumer) Close() {
	s.consumer.Close()
}
