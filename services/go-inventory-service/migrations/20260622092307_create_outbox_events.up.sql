CREATE TABLE outbox_events (
    id SERIAL PRIMARY KEY,

    event_id TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,

    payload JSONB NOT NULL,

    status TEXT NOT NULL DEFAULT 'PENDING',

    retry_count INTEGER NOT NULL DEFAULT 0,

    next_attempt_at TIMESTAMPTZ NULL,

    last_error TEXT NULL,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);