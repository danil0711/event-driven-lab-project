package kafka

import (
	"context"
	"inventory/internal/config"
	"log"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(cfg config.Config) *Producer {
	log.Println("creating kafka producer...")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{cfg.KafkaBootstrapServers},
		Topic:   cfg.KafkaInventoryTopic,
	})

	return &Producer{
		writer: writer,
	}

}

func (c Producer) Write(ctx context.Context, value []byte) error {
	return c.writer.WriteMessages(ctx, kafka.Message{
		Key:   nil,
		Value: value,
	})
}
