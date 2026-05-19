package main

import (
	"log"
	"yap/kafka/config"
	"yap/kafka/internal/adapter/kafka"
)

func main() {
	cfg := config.DefaultBatchConfig()

	c, err := kafka.NewBatchMessageConsumer(cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	c.ConsumeMessages()
}
