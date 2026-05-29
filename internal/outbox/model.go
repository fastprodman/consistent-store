package outbox

import (
	"encoding/json"

	"github.com/google/uuid"
)

type Event struct {
	ID            uuid.UUID
	AggregateType string
	AggregateID   uuid.UUID
	EventType     string
	Payload       json.RawMessage
}
