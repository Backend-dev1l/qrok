package agent_test

import (
	"testing"
	"time"

	"qrok/internal/agent"
	"qrok/internal/agent/source"
)

func TestToEnvelope(t *testing.T) {
	t.Parallel()

	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	ev := &source.Event{
		Topic:     "orders",
		Partition: 1,
		Offset:    99,
		Key:       []byte("k1"),
		Headers:   map[string]string{"h": "v"},
		Payload:   []byte(`{}`),
		BrokerTS:  ts,
	}

	out := agent.ToEnvelope("tunnel-1", "kafka", "01EVENT", ev)
	if out.GetEventId() != "01EVENT" || out.GetTunnelId() != "tunnel-1" {
		t.Fatalf("unexpected envelope: %+v", out)
	}
	if out.GetBrokerTsMs() != ts.UnixMilli() {
		t.Fatalf("broker_ts_ms = %d", out.GetBrokerTsMs())
	}
}

func TestNewEventIDUnique(t *testing.T) {
	t.Parallel()
	a := agent.NewEventID()
	b := agent.NewEventID()
	if a == b {
		t.Fatal("expected unique ULIDs")
	}
}

func TestSourceEventIDStableForBrokerPosition(t *testing.T) {
	t.Parallel()

	ev := &source.Event{Topic: "orders", Partition: 2, Offset: 42}
	first := agent.SourceEventID("tunnel-1", "kafka", ev)
	second := agent.SourceEventID("tunnel-1", "kafka", ev)

	if first != second {
		t.Fatalf("source event ID changed: %q != %q", first, second)
	}

	ev.Offset++
	if first == agent.SourceEventID("tunnel-1", "kafka", ev) {
		t.Fatal("different broker positions must have different IDs")
	}
}
