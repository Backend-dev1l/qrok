package service

import (
	"qrok/internal/controlplane/infrastructure/models"
	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/internal/tunnel"
)

// ToReplayEnvelope builds a protobuf envelope for replay delivery.
func ToReplayEnvelope(ev *models.Event, payload []byte, deliveryID string) *qrokv1.EventEnvelope {
	return toReplayEnvelope(ev, payload, deliveryID)
}

func toReplayEnvelope(ev *models.Event, payload []byte, deliveryID string) *qrokv1.EventEnvelope {
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
