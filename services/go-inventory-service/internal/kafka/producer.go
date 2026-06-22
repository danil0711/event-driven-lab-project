package kafka

import (
	"context"
	"log"
	"orders/internal/inventory"

	"github.com/segmentio/kafka-go"
)

func startConsumer(service *inventory.Service) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:29092"},
		Topic:   "payments",
		GroupID: "inventory-service",
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("message: %s", string(m.Value))

		// дальше будем парсить JSON → PaymentEvent
	}
}
