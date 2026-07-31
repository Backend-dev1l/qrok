//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"qrok/internal/agent"
	"qrok/internal/config"
	"qrok/internal/devcli"
)

// Критерий MVP: событие из Kafka доходит до localhost через agent → gateway → listen.
// Требует make compose-up && make migrate-up.
func TestKafkaAgentGatewayListenLocalhost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, objects := connectStores(t, ctx)
	defer pool.Close()

	broker := requireKafkaBroker(t)
	topic := fmt.Sprintf("qrok-e2e-%d", time.Now().UnixNano())
	tunnelID, agentToken := seedTunnel(t, ctx, pool, []string{topic})
	createKafkaTopic(t, broker, topic)

	stack := startTestStack(t, ctx, pool, objects)

	payload := `{"happy_path":true}`
	received := make(chan string, 1)
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()

	listenDone := make(chan error, 1)
	go func() {
		listenDone <- devcli.Run(ctx, &config.Listen{
			Listen: config.ListenConn{
				Gateway:  stack.GRPCAddr,
				TunnelID: tunnelID,
				Topics:   []string{topic},
				Forward:  stub.URL,
			},
		}, nil)
	}()

	agentDone := make(chan error, 1)
	go func() {
		agentDone <- agent.Run(ctx, &config.Agent{
			Agent: config.AgentConn{
				Token:      agentToken,
				TunnelID:   tunnelID,
				Gateway:    stack.GRPCAddr,
				Topics:     []string{topic},
				SourceType: "kafka",
			},
			Kafka: config.AgentKafka{
				Brokers:         []string{broker},
				StartFromOldest: true,
			},
			Events: config.Events{MaxPayloadBytes: 1 << 20},
		}, nil)
	}()

	time.Sleep(500 * time.Millisecond)
	produceKafka(t, broker, topic, kafka.Message{Value: []byte(payload)})

	require.Eventually(t, func() bool {
		select {
		case body := <-received:
			return body == payload
		default:
			return false
		}
	}, 60*time.Second, 200*time.Millisecond, "событие не дошло до localhost")

	require.Eventually(t, func() bool {
		events, err := stack.EventRepo.ListByTunnel(ctx, tunnelID, 10)
		return err == nil && len(events) >= 1
	}, 10*time.Second, 200*time.Millisecond, "событие не сохранилось в eventstore")

	cancel()
	<-listenDone
	<-agentDone
}

func createKafkaTopic(t *testing.T, brokerAddr, topic string) {
	t.Helper()

	conn, err := kafka.Dial("tcp", brokerAddr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	controller, err := conn.Controller()
	require.NoError(t, err)

	ctrlConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ctrlConn.Close() })

	require.NoError(t, ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic: topic, NumPartitions: 1, ReplicationFactor: 1,
	}))

	require.Eventually(t, func() bool {
		partitions, err := conn.ReadPartitions(topic)
		return err == nil && len(partitions) > 0
	}, 30*time.Second, 200*time.Millisecond)
}

func produceKafka(t *testing.T, brokerAddr, topic string, msg kafka.Message) {
	t.Helper()

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topic,
		BatchTimeout: 50 * time.Millisecond,
		Transport:    &kafka.Transport{MetadataTTL: 100 * time.Millisecond},
	}
	t.Cleanup(func() { _ = w.Close() })

	writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.Eventually(t, func() bool {
		return w.WriteMessages(writeCtx, msg) == nil
	}, 20*time.Second, 300*time.Millisecond)
}
