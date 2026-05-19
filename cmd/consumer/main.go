package main

import (
	"log"
	"yap/kafka/config"
	"yap/kafka/internal/adapter/kafka"
)

func main() {
	cfg := config.DefaultConsumerConfig()

	c, err := kafka.NewSingleMessageConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	c.ConsumeMessages()
}
