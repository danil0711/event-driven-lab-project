package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	KafkaBootstrapServers string `env:"KAFKA_BOOTSTRAP_SERVERS,required"`

	KafkaPaymentsTopic  string `env:"KAFKA_PAYMENTS_TOPIC" envDefault:"payments"`
	KafkaInventoryTopic string `env:"KAFKA_INVENTORY_TOPIC" envDefault:"inventory"`

	KafkaGroupID string `env:"KAFKA_GROUP_ID" envDefault:"inventory-service"`

	PostgreUser     string `env:"POSTGRES_USER,required"`
	PostgrePassword string `env:"POSTGRES_PASSWORD,required"`
	PostgreDB       string `env:"POSTGRES_DB,required"`
	PostgreHost     string `env:"POSTGRES_HOST,required"`
	PostgrePort     int    `env:"POSTGRES_PORT,required"`

	Production bool `env:"PRODUCTION" envDefault:"false"`
}

func Load() (Config, error) {
	cfg := Config{}
	err := env.Parse(&cfg)
	return cfg, err
}

func (c Config) PostgresDSN() string {
	return fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		c.PostgreUser,
		c.PostgrePassword,
		c.PostgreHost,
		c.PostgrePort,
		c.PostgreDB,
	)
}
