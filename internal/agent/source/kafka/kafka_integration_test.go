//go:build integration

package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/agent/source"
	"qrok/internal/testutil/integ"
)

func requireBroker(t *testing.T) string {
	t.Helper()

	brokerAddr := integ.KafkaBroker()
	conn, err := kafka.Dial("tcp", brokerAddr)
	if err != nil {
		t.Skipf("kafka недоступен (%v); запустите make compose-up", err)
	}
	_ = conn.Close()
	return brokerAddr
}

func createTopic(t *testing.T, brokerAddr, topic string) {
	t.Helper()

	conn, err := kafka.Dial("tcp", brokerAddr)
	require.NoError(t, err, "dial broker")
	t.Cleanup(func() { _ = conn.Close() })

	controller, err := conn.Controller()
	require.NoError(t, err, "controller")

	ctrlConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	require.NoError(t, err, "dial controller")
	t.Cleanup(func() { _ = ctrlConn.Close() })

	require.NoError(t, ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}), "create topic")

	// CreateTopics возвращается до того, как метаданные топика доедут до
	// брокера — ждём, пока топик станет видимым.
	require.Eventually(t, func() bool {
		partitions, err := conn.ReadPartitions(topic)
		return err == nil && len(partitions) > 0
	}, 30*time.Second, 200*time.Millisecond, "топик %s не стал видимым", topic)
}

func produce(t *testing.T, brokerAddr, topic string, msgs ...kafka.Message) {
	t.Helper()

	// Собственный Transport: глобальный kafka.DefaultTransport кэширует
	// метаданные кластера (~6s) и не видит только что созданный топик.
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topic,
		BatchTimeout: 50 * time.Millisecond,
		Transport:    &kafka.Transport{MetadataTTL: 100 * time.Millisecond},
	}
	t.Cleanup(func() { _ = w.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.Eventually(t, func() bool {
		return w.WriteMessages(ctx, msgs...) == nil
	}, 20*time.Second, 300*time.Millisecond, "produce messages")
}

func receive(t *testing.T, out <-chan *source.Event, timeout time.Duration) *source.Event {
	t.Helper()

	var ev *source.Event
	require.Eventually(t, func() bool {
		select {
		case ev = <-out:
			return true
		default:
			return false
		}
	}, timeout, 100*time.Millisecond, "событие не пришло за отведённое время")
	return ev
}

func newSource(t *testing.T, brokerAddr, tunnelID string) Source {
	t.Helper()

	s, err := New(Config{
		Brokers:         []string{brokerAddr},
		TunnelID:        tunnelID,
		StartFromOldest: true,
	})
	require.NoError(t, err)
	return s
}

func subscribe(t *testing.T, s Source, topic string) (context.Context, context.CancelFunc, <-chan *source.Event, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *source.Event, 16)
	done := make(chan error, 1)
	go func() { done <- s.Subscribe(ctx, []string{topic}, out) }()
	return ctx, cancel, out, done
}

func stopSubscribe(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()

	cancel()
	require.NoError(t, <-done, "subscribe завершился с ошибкой")
}

// Сквозной сценарий: продюс → чтение своей consumer group → ack →
// после пересоздания Source закоммиченное сообщение не перечитывается.
func TestSubscribeAckCommit(t *testing.T) {
	brokerAddr := requireBroker(t)
	topic := fmt.Sprintf("qrok-it-%d", time.Now().UnixNano())
	tunnelID := fmt.Sprintf("it-%d", time.Now().UnixNano())
	createTopic(t, brokerAddr, topic)

	produce(t, brokerAddr, topic, kafka.Message{
		Key:     []byte("k1"),
		Value:   []byte(`{"n":1}`),
		Headers: []kafka.Header{{Key: "traceparent", Value: []byte("00-abc")}},
	})

	// Первая сессия: читаем первое сообщение и ack'аем его.
	s1 := newSource(t, brokerAddr, tunnelID)
	t.Cleanup(func() { _ = s1.Close() })

	_, cancel1, out1, done1 := subscribe(t, s1, topic)
	ev := receive(t, out1, 60*time.Second)
	assert.Equal(t, `{"n":1}`, string(ev.Payload))
	assert.Equal(t, "k1", string(ev.Key))
	assert.Equal(t, "00-abc", ev.Headers["traceparent"])
	require.NoError(t, s1.Ack(context.Background(), ev))
	stopSubscribe(t, cancel1, done1)

	// Вторая сессия той же группы: закоммиченное сообщение не должно прийти,
	// новое — должно.
	produce(t, brokerAddr, topic, kafka.Message{Value: []byte(`{"n":2}`)})

	s2 := newSource(t, brokerAddr, tunnelID)
	t.Cleanup(func() { _ = s2.Close() })

	_, cancel2, out2, done2 := subscribe(t, s2, topic)
	ev2 := receive(t, out2, 60*time.Second)
	assert.Equal(t, `{"n":2}`, string(ev2.Payload))
	stopSubscribe(t, cancel2, done2)
}

// Без ack оффсет не двигается: после рестарта той же группы событие приходит снова.
func TestNoAckMeansRedelivery(t *testing.T) {
	brokerAddr := requireBroker(t)
	topic := fmt.Sprintf("qrok-redeliver-%d", time.Now().UnixNano())
	tunnelID := fmt.Sprintf("rd-%d", time.Now().UnixNano())
	createTopic(t, brokerAddr, topic)

	produce(t, brokerAddr, topic, kafka.Message{Value: []byte(`{"n":1}`)})

	for attempt := 1; attempt <= 2; attempt++ {
		t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
			s := newSource(t, brokerAddr, tunnelID)
			t.Cleanup(func() { _ = s.Close() })

			_, cancel, out, done := subscribe(t, s, topic)
			ev := receive(t, out, 60*time.Second)
			assert.Equal(t, `{"n":1}`, string(ev.Payload))
			// Намеренно без Ack: событие должно прийти и на второй попытке.
			stopSubscribe(t, cancel, done)
		})
	}
}
