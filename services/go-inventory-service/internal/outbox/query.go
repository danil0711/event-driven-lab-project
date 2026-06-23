package outbox

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func GetPendingEvent(ctx context.Context, pool *pgxpool.Pool) ([]OutboxEvent, error) {
	const query = `
		SELECT
			id,
			event_id,
			type,
			payload,
			retry_count
		FROM outbox_events
		WHERE status = 'PENDING'
		AND (
			next_attempt_at IS NULL
			OR next_attempt_at <= NOW()
		)
		ORDER BY next_attempt_at ASC
		LIMIT 100
	`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []OutboxEvent

	for rows.Next() {
		var event OutboxEvent

		err := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.Type,
			&event.Payload,
			&event.RetryCount,
		)

		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}
	return events, nil
}
