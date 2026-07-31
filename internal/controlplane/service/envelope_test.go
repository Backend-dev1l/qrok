package service_test

import (
	"testing"

	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/internal/controlplane/service"
)

func TestEventFromProto(t *testing.T) {
	t.Parallel()

	ev, err := service.EventFromProto(&qrokv1.EventEnvelope{
		EventId:    "01ABC",
		TunnelId:   "tunnel-1",
		Topic:      "orders",
		Partition:  2,
		Offset:     42,
		Key:        []byte("k"),
		Headers:    map[string]string{"trace": "1"},
		Payload:    []byte(`{"ok":true}`),
		BrokerTsMs: 1_700_000_000_000,
	})
	if err != nil {
		t.Fatalf("EventFromProto() error = %v", err)
	}
	if ev.ID != "01ABC" || ev.TunnelID != "tunnel-1" || ev.Topic != "orders" {
		t.Fatalf("unexpected event: %+v", ev)
	}
	if ev.BrokerTS == nil {
		t.Fatal("broker_ts is nil")
	}
}

func TestEventFromProtoRejectsPayloadRef(t *testing.T) {
	t.Parallel()

	_, err := service.EventFromProto(&qrokv1.EventEnvelope{
		EventId:    "01ABC",
		TunnelId:   "tunnel-1",
		Topic:      "orders",
		PayloadRef: "s3/key",
	})
	if err == nil {
		t.Fatal("expected error for payload_ref from agent")
	}
}
