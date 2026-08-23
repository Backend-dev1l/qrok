package gateway

import (
	"time"

	"qrok/internal/controlplane/infrastructure/models"
	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/pkg/fault"
)

func validateEventEnvelope(op string, envelope *qrokv1.EventEnvelope) error {
	if envelope == nil {
		return fault.ErrValidation.New("empty envelope").WithOp(op)
	}
	if envelope.GetPayloadRef() != "" {
		return fault.ErrValidation.
			New("payload_ref from agent is not supported").
			WithOp(op)
	}
	if envelope.GetEventId() == "" {
		return fault.ErrValidation.New("event_id is required").WithOp(op)
	}
	if envelope.GetTunnelId() == "" {
		return fault.ErrValidation.New("tunnel_id is required").WithOp(op)
	}
	if envelope.GetTopic() == "" {
		return fault.ErrValidation.New("topic is required").WithOp(op)
	}
	return nil
}

func eventFromProto(envelope *qrokv1.EventEnvelope) (*models.Event, error) {
	var brokerTS *time.Time
	if ms := envelope.GetBrokerTsMs(); ms > 0 {
		t := time.UnixMilli(ms).UTC()
		brokerTS = &t
	}

	partition := envelope.GetPartition()
	offset := envelope.GetOffset()

	return &models.Event{
		ID:           envelope.GetEventId(),
		TunnelID:     envelope.GetTunnelId(),
		SourceType:   envelope.GetSourceType(),
		Topic:        envelope.GetTopic(),
		Partition:    &partition,
		BrokerOffset: &offset,
		Key:          envelope.GetKey(),
		Headers:      envelope.GetHeaders(),
		Payload:      envelope.GetPayload(),
		IsReplay:     envelope.GetIsReplay(),
		BrokerTS:     brokerTS,
	}, nil
}
