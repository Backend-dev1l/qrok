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
	"qrok/internal/controlplane/infrastructure/models"
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
	type localDelivery struct {
		body    string
		eventID string
	}
	received := make(chan localDelivery, 3)
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- localDelivery{body: string(body), eventID: r.Header.Get("X-Qrok-Event-Id")}
		if string(body) == `{"fail":true}` {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
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

	waitListenReady(t, ctx, stack, tunnelID, topic, func(probe string) bool {
		select {
		case d := <-received:
			return d.body == probe
		default:
			return false
		}
	})
	assertListenRunning(t, listenDone)

	produceKafka(t, broker, topic, kafka.Message{Value: []byte(`{"warmup":true}`)})
	select {
	case <-received:
	case <-time.After(60 * time.Second):
		t.Fatal("warmup event did not reach localhost")
	}

	produceKafka(t, broker, topic, kafka.Message{Value: []byte(payload)})
	require.Eventually(t, func() bool {
		select {
		case delivery := <-received:
			return delivery.body == payload
		default:
			return false
		}
	}, 10*time.Second, 100*time.Millisecond, "event did not reach localhost")

	produceKafka(t, broker, topic, kafka.Message{Value: []byte(`{"fail":true}`)})
	var failed localDelivery
	select {
	case failed = <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("failed localhost delivery was not attempted")
	}
	require.Eventually(t, func() bool {
		rec, err := stack.Delivery.GetByID(ctx, failed.eventID+"-live")
		return err == nil && rec.Status == models.DeliveryStatusFailed && rec.StatusCode != nil &&
			*rec.StatusCode == http.StatusInternalServerError
	}, 5*time.Second, 100*time.Millisecond, "localhost failure was not recorded")

	require.Eventually(t, func() bool {
		events, err := stack.EventRepo.ListByTunnel(ctx, tunnelID, 10)
		return err == nil && len(events) >= 1
	}, 10*time.Second, 200*time.Millisecond, "event was not saved to eventstore")

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
