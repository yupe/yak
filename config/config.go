package config

type Config struct {
	BootstrapServers string `yaml:"bootstrap_servers" json:"bootstrap_servers"`
	Topic            string `yaml:"topic" json:"topic"`
	ClientID         string `yaml:"client_id" json:"client_id"`
	Acks             string `yaml:"acks" json:"acks"`
	Retries          string `yaml:"retries" json:"retries"`
}

func DefaultConfig() *Config {
	return &Config{
		BootstrapServers: "localhost:9094",
		Topic:            "yandex-hw-1",
		ClientID:         "ya-hw-1-go-producer",
		Acks:             "all",
		Retries:          "3",
	}
}
