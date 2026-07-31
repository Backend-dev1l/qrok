package service

import (
	"time"

	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/internal/controlplane/model"
	"qrok/internal/tunnel"
	"qrok/pkg/fault"
)

// EventFromProto converts a protobuf envelope into a domain event.
func EventFromProto(envelope *qrokv1.EventEnvelope) (*model.Event, error) {
	const op = "event.from_proto"
	if envelope == nil {
		return nil, fault.ErrValidation.New("пустой envelope").WithOp(op)
	}
	if envelope.GetPayloadRef() != "" {
		return nil, fault.ErrValidation.
			New("payload_ref на стороне агента не поддерживается").
			WithOp(op)
	}
	if len(envelope.GetPayload()) == 0 {
		return nil, fault.ErrValidation.New("пустой payload").WithOp(op)
	}

	var brokerTS *time.Time
	if ms := envelope.GetBrokerTsMs(); ms > 0 {
		t := time.UnixMilli(ms).UTC()
		brokerTS = &t
	}

	partition := envelope.GetPartition()
	offset := envelope.GetOffset()

	return &model.Event{
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

// ToReplayEnvelope builds a protobuf envelope for replay delivery.
func ToReplayEnvelope(ev *model.Event, payload []byte, deliveryID string) *qrokv1.EventEnvelope {
	return toReplayEnvelope(ev, payload, deliveryID)
}

func toReplayEnvelope(ev *model.Event, payload []byte, deliveryID string) *qrokv1.EventEnvelope {
	headers := make(map[string]string, len(ev.Headers)+1)
	for k, v := range ev.Headers {
		headers[k] = v
	}
	headers[tunnel.DeliveryIDHeader] = deliveryID

	var partition int32
	var offset int64
	if ev.Partition != nil {
		partition = *ev.Partition
	}
	if ev.BrokerOffset != nil {
		offset = *ev.BrokerOffset
	}
	var brokerTS int64
	if ev.BrokerTS != nil {
		brokerTS = ev.BrokerTS.UnixMilli()
	}

	sourceType := ev.SourceType
	if sourceType == "" {
		sourceType = "kafka"
	}

	return &qrokv1.EventEnvelope{
		EventId:    ev.ID,
		TunnelId:   ev.TunnelID,
		SourceType: sourceType,
		Topic:      ev.Topic,
		Key:        ev.Key,
		Headers:    headers,
		Payload:    payload,
		Partition:  partition,
		Offset:     offset,
		BrokerTsMs: brokerTS,
		IsReplay:   true,
	}
}
