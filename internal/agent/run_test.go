package agent

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"qrok/internal/agent/source"
	"qrok/internal/config"
	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/pkg/fault"
)

type fakeSource struct {
	events  []*source.Event
	ackErr  error
	onAck   func()
	mu      sync.Mutex
	offsets []int64
}

func (f *fakeSource) Subscribe(ctx context.Context, _ []string, out chan<- *source.Event) error {
	for _, ev := range f.events {
		select {
		case out <- ev:
		case <-ctx.Done():
			return nil
		}
	}
	<-ctx.Done()
	return nil
}

func (f *fakeSource) Ack(_ context.Context, ev *source.Event) error {
	if f.ackErr != nil {
		return f.ackErr
	}
	f.mu.Lock()
	f.offsets = append(f.offsets, ev.Offset)
	f.mu.Unlock()
	if f.onAck != nil {
		f.onAck()
	}
	return nil
}

func (f *fakeSource) Close() error { return nil }

type fakeAgentStream struct {
	mu       sync.Mutex
	sendIDs  []string
	failSend int
}

func (f *fakeAgentStream) RunSession(
	ctx context.Context,
	_ string,
	_ *qrokv1.AgentHello,
	handle func(context.Context, func(context.Context, *qrokv1.EventEnvelope) error) error,
) error {
	return handle(ctx, func(_ context.Context, ev *qrokv1.EventEnvelope) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.sendIDs = append(f.sendIDs, ev.GetEventId())
		if f.failSend > 0 {
			f.failSend--
			return fault.ErrServiceUnavail.New("temporary send failure")
		}
		return nil
	})
}

func (f *fakeAgentStream) Close() error { return nil }

func TestRunRetriesPendingEventBeforeLaterOffsets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	src := &fakeSource{events: []*source.Event{
		{Topic: "orders", Partition: 0, Offset: 1, Payload: []byte(`{"n":1}`)},
		{Topic: "orders", Partition: 0, Offset: 2, Payload: []byte(`{"n":2}`)},
	}}
	src.onAck = func() {
		src.mu.Lock()
		done := len(src.offsets) == 2
		src.mu.Unlock()
		if done {
			cancel()
		}
	}
	client := &fakeAgentStream{failSend: 1}

	err := run(ctx, testAgentConfig(1<<20), slog.New(slog.NewTextHandler(io.Discard, nil)), src, client)
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	client.mu.Lock()
	sendIDs := append([]string(nil), client.sendIDs...)
	client.mu.Unlock()
	if len(sendIDs) != 3 {
		t.Fatalf("send count = %d, want 3", len(sendIDs))
	}
	if sendIDs[0] != sendIDs[1] {
		t.Fatalf("retry changed event ID: %q != %q", sendIDs[0], sendIDs[1])
	}
	if sendIDs[1] == sendIDs[2] {
		t.Fatal("different offsets received the same event ID")
	}

	src.mu.Lock()
	offsets := append([]int64(nil), src.offsets...)
	src.mu.Unlock()
	if len(offsets) != 2 || offsets[0] != 1 || offsets[1] != 2 {
		t.Fatalf("committed offsets = %v, want [1 2]", offsets)
	}
}

func TestRunRejectsOversizedEventWithoutSendOrAck(t *testing.T) {
	src := &fakeSource{events: []*source.Event{{
		Topic: "orders", Partition: 0, Offset: 1, Payload: []byte("too large"),
	}}}
	client := &fakeAgentStream{}

	err := run(context.Background(), testAgentConfig(3), slog.New(slog.NewTextHandler(io.Discard, nil)), src, client)
	if fault.CodeOf(err) != fault.ErrValidation {
		t.Fatalf("error = %v, want validation", err)
	}
	if len(client.sendIDs) != 0 {
		t.Fatalf("oversized event was sent: %v", client.sendIDs)
	}
	if len(src.offsets) != 0 {
		t.Fatalf("oversized event was committed: %v", src.offsets)
	}
}

func testAgentConfig(maxPayload int64) *config.Agent {
	return &config.Agent{
		Agent: config.AgentConn{
			Token:      "token",
			TunnelID:   "tunnel-1",
			SourceType: "kafka",
			Topics:     []string{"orders"},
		},
		Events: config.Events{MaxPayloadBytes: maxPayload},
	}
}
