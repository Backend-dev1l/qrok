package inproc_test

import (
	"context"
	"testing"
	"time"

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

func TestPublishDoesNotBlockOnSlowSubscriber(t *testing.T) {
	t.Parallel()

	b := inproc.New(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, unsub, err := b.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer unsub()

	ev := &qrokv1.EventEnvelope{EventId: "01TEST", Payload: []byte("hi")}
	if err := b.Publish(ctx, ev); err != nil {
		t.Fatalf("first Publish() error = %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- b.Publish(ctx, ev) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second Publish() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full subscriber buffer")
	}
}
