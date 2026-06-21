package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9094",
		"group.id":          "go-hw4-consumer",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "create consumer: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	topics := []string{"customers.public.users", "customers.public.orders"}
	if err := consumer.SubscribeTopics(topics, nil); err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Waiting for messages (Ctrl+C to stop)...")

run:
	for {
		select {
		case <-sigchan:
			break run
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				fmt.Fprintf(os.Stderr, "read: %v\n", err)
				continue
			}

			fmt.Printf("topic=%s partition=%d offset=%d\n",
				*msg.TopicPartition.Topic,
				msg.TopicPartition.Partition,
				msg.TopicPartition.Offset)
			fmt.Printf("key=%s\n", string(msg.Key))
			fmt.Printf("value=%s\n", string(msg.Value))
			fmt.Println("---")
		}
	}
}
