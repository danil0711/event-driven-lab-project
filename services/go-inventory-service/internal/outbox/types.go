package outbox

type OutboxEvent struct {
	ID         int64
	EventID    string
	Type       string
	Payload    []byte
	RetryCount int
}
