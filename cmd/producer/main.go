package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"yap/kafka/config"
	"yap/kafka/internal/adapter/kafka"
	"yap/kafka/internal/entity"
)

func main() {
	cfg := config.DefaultConfig()
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}
	defer producer.Close()

	for i := 0; i < 50; i++ {

		data, err := json.Marshal(entity.Order{
			ID:    strconv.Itoa(i),
			Price: 20000 + float64(i*100),
		})

		if err != nil {
			fmt.Printf("Failed to marshal json: %s\n", err)
			return
		}

		err = producer.Send(data)
	}

	if err != nil {
		fmt.Printf("Failed to send json: %s\n", err)
		return
	}

}
