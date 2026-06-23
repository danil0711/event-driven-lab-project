package outbox

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func getBackoff(
	retryCount int,
) time.Duration {

	switch retryCount {

	case 1:
		return 3 * time.Second

	case 2:
		return 10 * time.Second

	case 3:
		return 30 * time.Second

	case 4:
		return 60 * time.Second

	default:
		return 60 * time.Second

	}

}

func HandlePublishError(
	ctx context.Context,
	pool *pgxpool.Pool,
	event OutboxEvent,
	publishErr error,
) error {
	retryCount := event.RetryCount + 1

	if retryCount >= MaxRetries {

		_, err := pool.Exec(
			ctx,
			`
			UPDATE outbox_events
			SET
				status = 'FAILED',
				retry_count = $1,
				last_error = $2
			WHERE id = $3
			`,
			retryCount,
			publishErr.Error(),
			event.ID,
		)

		log.Println("Не удалось выполнить event.", event.EventID)

		return err
	}

	nextAttempt := time.Now().UTC().Add(getBackoff(retryCount))

	log.Println("Повторная попытка выполнения event, номер попытки:", event.RetryCount+1, event.EventID)

	_, err := pool.Exec(
		ctx,
		`
		UPDATE outbox_events
		SET
			retry_count = $1,
			last_error = $2,
			next_attempt_at = $3
		WHERE id = $4
		`,
		retryCount,
		publishErr.Error(),
		nextAttempt,
		event.ID,
	)

	return err
}
