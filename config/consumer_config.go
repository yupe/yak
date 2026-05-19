package config

type ConsumerConfig struct {
	BootstrapServers string `yaml:"bootstrap_servers" json:"bootstrap_servers"`
	Topic            string `yaml:"topic" json:"topic"`
	GroupID          string `yaml:"group_id" json:"group_id"`
	ClientID         string `yaml:"client_id" json:"client_id"`
	AutoOffsetReset  string `yaml:"auto_offset_reset" json:"auto_offset_reset"`
	EnableAutoCommit bool   `yaml:"enable_auto_commit" json:"enable_auto_commit"`
}

func DefaultConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		BootstrapServers: "localhost:9094,localhost:9095",
		Topic:            "yandex-hw-1",
		GroupID:          "yandex-consumer-group",
		ClientID:         "ya-hw-1-go-consumer",
		AutoOffsetReset:  "earliest",
		EnableAutoCommit: true,
	}
}
