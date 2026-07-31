package inproc_test

import (
	"context"
	"testing"

	"qrok/internal/bus/inproc"
	qrokv1 "qrok/internal/proto/qrok/v1"
)

func TestBusPublishSubscribe(t *testing.T) {
	t.Parallel()

	b := inproc.New(4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, unsub, err := b.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsub()

	ev := &qrokv1.EventEnvelope{EventId: "01TEST", TunnelId: "t1", Topic: "demo", Payload: []byte("hi")}
	if err := b.Publish(ctx, ev); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	got := <-ch
	if got.GetEventId() != ev.GetEventId() {
		t.Fatalf("event_id = %q, want %q", got.GetEventId(), ev.GetEventId())
	}
}
