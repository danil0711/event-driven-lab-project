package outbox

import (
	"context"
	"inventory/internal/kafka"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ProcessEvent(ctx context.Context, pool *pgxpool.Pool, producer *kafka.Producer, event OutboxEvent) error {

	log.Printf("sending event=%s", event.EventID)

	err := producer.Write(ctx, event.Payload)

	if err != nil {
		log.Printf(
			"failed to publish event=%s: %v",
			event.EventID,
			err,
		)

		return HandlePublishError(
			ctx,
			pool,
			event,
			err,
		)
	}

	_, err = pool.Exec(
		ctx,
		`
			UPDATE outbox_events
			SET status = 'SENT'
			WHERE id = $1
		`,
		event.ID,
	)

	if err != nil {
		return err
	}

	log.Printf(
		"event=%s marked as SENT",
		event.EventID,
	)

	return nil

}
