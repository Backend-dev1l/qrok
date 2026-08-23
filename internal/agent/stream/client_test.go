package stream

import (
	"context"
	"testing"
	"time"
)

func TestRegisterAckDoesNotLoseImmediateAck(t *testing.T) {
	client := &AgentClient{acks: make(map[string]chan struct{})}
	ack, unregister := client.registerAck("event-1")
	defer unregister()

	client.mu.Lock()
	client.acks["event-1"] <- struct{}{}
	client.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitAck(ctx, ack); err != nil {
		t.Fatalf("waitAck returned error: %v", err)
	}
}

func TestWaitAckHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitAck(ctx, make(chan struct{}))
	if err == nil {
		t.Fatal("expected timeout error")
	}
}
