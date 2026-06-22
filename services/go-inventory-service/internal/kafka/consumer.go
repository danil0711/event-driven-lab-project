package kafka

import (
	"context"
	"inventory/internal/config"
	"log"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(cfg config.Config) *Consumer {
	log.Println("creating kafka consumer...")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBootstrapServers},
		Topic:   cfg.KafkaPaymentsTopic,
		GroupID: cfg.KafkaGroupID,
	})

	return &Consumer{
		reader: reader,
	}
}

func (c *Consumer) Read(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}
