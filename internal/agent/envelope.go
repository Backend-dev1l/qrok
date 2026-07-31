package agent

import (
	"crypto/rand"

	"github.com/oklog/ulid/v2"

	"qrok/internal/agent/source"
	qrokv1 "qrok/internal/proto/qrok/v1"
)

// NewEventID генерирует ULID для event_id.
func NewEventID() string {
	return ulid.MustNew(ulid.Now(), rand.Reader).String()
}

// ToEnvelope конвертирует событие брокера в protobuf-envelope для gateway.
func ToEnvelope(tunnelID, sourceType, eventID string, ev *source.Event) *qrokv1.EventEnvelope {
	var brokerTS int64
	if !ev.BrokerTS.IsZero() {
		brokerTS = ev.BrokerTS.UTC().UnixMilli()
	}
	return &qrokv1.EventEnvelope{
		EventId:    eventID,
		TunnelId:   tunnelID,
		SourceType: sourceType,
		Topic:      ev.Topic,
		Key:        ev.Key,
		Headers:    ev.Headers,
		Payload:    ev.Payload,
		Partition:  int32(ev.Partition),
		Offset:     ev.Offset,
		BrokerTsMs: brokerTS,
	}
}
