package kafka

import (
	"context"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"qrok/internal/agent/source"
	"qrok/pkg/fault"
)

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{TunnelID: "t1"}); fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("без брокеров ожидался VALIDATION_ERROR: %v", err)
	}
	if _, err := New(Config{Brokers: []string{"localhost:9092"}}); fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("без tunnel_id/group_id ожидался VALIDATION_ERROR: %v", err)
	}
	if _, err := New(Config{Brokers: []string{"localhost:9092"}, TunnelID: "t1"}); err != nil {
		t.Errorf("валидный конфиг не должен давать ошибку: %v", err)
	}
}

func TestGroupIDNaming(t *testing.T) {
	if got := (Config{TunnelID: "orders-tunnel"}).groupID(); got != "qrok-agent-orders-tunnel" {
		t.Errorf("groupID = %q", got)
	}
	if got := (Config{TunnelID: "orders", GroupID: "custom"}).groupID(); got != "custom" {
		t.Errorf("явный GroupID должен побеждать: %q", got)
	}
}

func TestSubscribeRequiresTopics(t *testing.T) {
	s, err := New(Config{Brokers: []string{"localhost:9092"}, TunnelID: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Subscribe(context.Background(), nil, make(chan *source.Event))
	if fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("пустые топики: ожидался VALIDATION_ERROR, получено %v", err)
	}
}

func TestAckBeforeSubscribe(t *testing.T) {
	s, err := New(Config{Brokers: []string{"localhost:9092"}, TunnelID: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Ack(context.Background(), &source.Event{Topic: "x"})
	if fault.CodeOf(err) != fault.ErrConflict {
		t.Errorf("Ack до подписки: ожидался CONFLICT, получено %v", err)
	}
}

func TestDoubleSubscribe(t *testing.T) {
	// Брокер недостижим (порт 1) — первый Subscribe висит в ретраях,
	// второй должен сразу вернуть CONFLICT.
	s, err := New(Config{Brokers: []string{"127.0.0.1:1"}, TunnelID: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.Subscribe(ctx, []string{"topic"}, make(chan *source.Event)) }()
	time.Sleep(100 * time.Millisecond)

	err = s.Subscribe(ctx, []string{"topic"}, make(chan *source.Event))
	if fault.CodeOf(err) != fault.ErrConflict {
		t.Errorf("вторая подписка: ожидался CONFLICT, получено %v", err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("отмена контекста должна завершать Subscribe без ошибки: %v", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	s, err := New(Config{Brokers: []string{"localhost:9092"}, TunnelID: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close без подписки: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("повторный Close: %v", err)
	}
}

func TestToEvent(t *testing.T) {
	ts := time.Now()
	ev := toEvent(kafkago.Message{
		Topic:     "orders",
		Partition: 2,
		Offset:    41,
		Key:       []byte("k1"),
		Value:     []byte(`{"order_id":42}`),
		Time:      ts,
		Headers: []kafkago.Header{
			{Key: "traceparent", Value: []byte("00-abc")},
			{Key: "dup", Value: []byte("first")},
			{Key: "dup", Value: []byte("last")},
		},
	})

	if ev.Topic != "orders" || ev.Partition != 2 || ev.Offset != 41 {
		t.Errorf("метаданные потерялись: %+v", ev)
	}
	if string(ev.Key) != "k1" || string(ev.Payload) != `{"order_id":42}` {
		t.Errorf("key/payload потерялись: %+v", ev)
	}
	if !ev.BrokerTS.Equal(ts) {
		t.Errorf("BrokerTS = %v, want %v", ev.BrokerTS, ts)
	}
	if ev.Headers["traceparent"] != "00-abc" {
		t.Errorf("headers = %v", ev.Headers)
	}
	if ev.Headers["dup"] != "last" {
		t.Errorf("при дублях ключей должен побеждать последний: %v", ev.Headers)
	}
}

func TestToEventNoHeaders(t *testing.T) {
	if ev := toEvent(kafkago.Message{Topic: "t"}); ev.Headers != nil {
		t.Errorf("без заголовков Headers должен быть nil: %v", ev.Headers)
	}
}
