package main

import (
	"context"
	"log"
	"time"

	"inventory/internal/config"
	"inventory/internal/db"
	"inventory/internal/kafka"
	"inventory/internal/outbox"

	"github.com/joho/godotenv"
)

func main() {
	log.Println("Запуск outbox воркера...")
	// === Контекст ===
	ctx := context.Background()

	_ = godotenv.Load()

	// === Конфиг ===
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	// === Пул БД ===
	pool, err := db.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	producer := kafka.NewProducer(cfg)

	for {
		events, err := outbox.GetPendingEvents(ctx, pool)

		if err != nil {
			log.Println(err)

			time.Sleep(time.Second)
			continue
		}

		for _, event := range events {
			err := outbox.ProcessEvent(ctx, pool, producer, event)

			if err != nil {
				log.Println(err)
			}
		}

		time.Sleep(time.Second)
	}
}
