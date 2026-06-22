package main

import (
	"context"
	"encoding/json"
	"log"

	"inventory/internal/config"
	"inventory/internal/db"
	"inventory/internal/inventory"
	"inventory/internal/kafka"

	"github.com/joho/godotenv"
)

func main() {
	ctx := context.Background()

	err := godotenv.Load()
	if err != nil {
		log.Println(".env not found, using system env")
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("connecting to postgres...")

	pool, err := db.New(ctx, cfg)
	if err != nil {
		log.Fatal("failed to connect postgres:", err)
	}
	log.Println("postgres connected")

	log.Println("starting inventory service...")
	consumer := kafka.NewConsumer(cfg)

	log.Println("consumer initialized, starting loop")
	service := inventory.NewService(pool)

	for {
		msg, err := consumer.Read(ctx)
		if err != nil {
			log.Println("kafka read error:", err)
			continue
		}

		log.Println("raw message:", string(msg.Value))

		var event inventory.PaymentEvent

		err = json.Unmarshal(msg.Value, &event)
		if err != nil {
			log.Println("bad json:", err)
			continue
		}

		result, err := service.HandleBusiness(event)
		if err != nil {
			log.Println("handle error:", err)
			continue
		}

		if result == nil {
			continue
		}

		log.Printf("result: %+v", result)

	}
}
