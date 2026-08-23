package agent

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"hash"

	"github.com/oklog/ulid/v2"

	"qrok/internal/agent/source"
	qrokv1 "qrok/internal/proto/qrok/v1"
)

// NewEventID генерирует ULID для event_id.
func NewEventID() string {
	return ulid.MustNew(ulid.Now(), rand.Reader).String()
}

// SourceEventID returns a stable event ID for the same broker position.
// A retry after an ACK or offset-commit failure must reuse the same ID so the
// cloud can deduplicate the ingest.
func SourceEventID(tunnelID, sourceType string, ev *source.Event) string {
	h := sha256.New()
	writeHashString(h, tunnelID)
	writeHashString(h, sourceType)
	writeHashString(h, ev.Topic)
	_ = binary.Write(h, binary.BigEndian, int64(ev.Partition))
	_ = binary.Write(h, binary.BigEndian, ev.Offset)

	var id ulid.ULID
	copy(id[:], h.Sum(nil))
	return id.String()
}

func writeHashString(h hash.Hash, value string) {
	_ = binary.Write(h, binary.BigEndian, uint64(len(value)))
	_, _ = h.Write([]byte(value))
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
