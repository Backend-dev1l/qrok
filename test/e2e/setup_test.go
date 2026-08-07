//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"qrok/internal/bus/inproc"
	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/internal/controlplane/infrastructure/delivery"
	"qrok/internal/controlplane/infrastructure/eventstore"
	"qrok/internal/controlplane/service"
	"qrok/internal/transport/grpc"
	qrokv1 "qrok/internal/proto/qrok/v1"
	"qrok/internal/testutil/integ"
	"qrok/pkg/objectstore"
	"qrok/pkg/postgres"
)

const (
	e2eOrgID     = "01E2EORG000000000000000000"
	e2eProjectID = "01E2EPRJ000000000000000000"
)

type testStack struct {
	Pool       *pgxpool.Pool
	EventRepo  eventstore.Repository
	Replay     service.ReplayService
	Delivery   delivery.Repository
	Bus        *inproc.Bus
	GRPCAddr   string
	grpcServer *grpc.Server
	cancel     context.CancelFunc
}

func connectStores(t *testing.T, ctx context.Context) (*pgxpool.Pool, *objectstore.Client) {
	t.Helper()

	pool, err := postgres.New(ctx, postgres.Config{DSN: integ.PostgresDSN()})
	if err != nil {
		t.Skipf("postgres unavailable (%v); run make compose-up && make migrate-up", err)
	}

	objects, err := objectstore.New(ctx, objectstore.Config{
		Endpoint:  integ.MinIOEndpoint(),
		Bucket:    "qrok-payloads",
		AccessKey: "qrok",
		SecretKey: "qrok-secret",
		UseSSL:    false,
	})
	if err != nil {
		pool.Close()
		t.Skipf("minio unavailable (%v); run make compose-up", err)
	}

	return pool, objects
}

func requireKafkaBroker(t *testing.T) string {
	t.Helper()

	addr := integ.KafkaBroker()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Skipf("kafka unavailable (%v); run make compose-up", err)
	}
	_ = conn.Close()
	return addr
}

func seedTunnel(t *testing.T, ctx context.Context, pool *pgxpool.Pool, topics []string) (tunnelID, agentToken string) {
	t.Helper()

	plaintext, hash, err := auth.GenerateAgentToken()
	require.NoError(t, err)

	tunnelID = fmt.Sprintf("e2e-%d", time.Now().UnixNano())

	_, err = pool.Exec(ctx, `
		INSERT INTO orgs (id, name) VALUES ($1, 'E2E Org')
		ON CONFLICT (id) DO NOTHING
	`, e2eOrgID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO projects (id, org_id, name) VALUES ($1, $2, 'e2e')
		ON CONFLICT (id) DO NOTHING
	`, e2eProjectID, e2eOrgID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO tunnels (id, project_id, name, source_type, topics)
		VALUES ($1, $2, 'e2e', 'kafka', $3)
	`, tunnelID, e2eProjectID, topics)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO agent_tokens (id, project_id, token_hash, name)
		VALUES ($1, $2, $3, 'e2e-token')
	`, fmt.Sprintf("tok-%s", tunnelID), e2eProjectID, hash)
	require.NoError(t, err)

	return tunnelID, plaintext
}

func startTestStack(t *testing.T, ctx context.Context, pool *pgxpool.Pool, objects *objectstore.Client) *testStack {
	t.Helper()

	eventBus := inproc.New(256)
	eventRepo := eventstore.New(pool, objects, eventstore.Config{
		PayloadThresholdBytes: 262144,
	})
	authRepo := auth.NewRepository(pool)
	deliveryRepo := delivery.NewRepository(pool)
	authSvc := service.NewAuthService(authRepo)
	eventSvc := service.NewEventService(eventRepo, deliveryRepo, authRepo)
	deliverySvc := service.NewDeliveryService(deliveryRepo)
	replaySvc := service.NewReplayService(eventRepo, deliveryRepo, authRepo, eventBus)

	_, cancel := context.WithCancel(ctx)

	gw := gateway.NewServer(gateway.Config{
		AllowInsecureListen: true,
		MaxPayloadBytes:     1048576,
	}, eventBus, eventSvc, authSvc, deliverySvc, nil)
	grpcServer := grpc.NewServer()
	qrokv1.RegisterTunnelServiceServer(grpcServer, gw)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go grpcServer.Serve(lis)

	t.Cleanup(func() {
		cancel()
		grpcServer.Stop()
		_ = lis.Close()
	})

	return &testStack{
		Pool:       pool,
		EventRepo:  eventRepo,
		Replay:     replaySvc,
		Delivery:   deliveryRepo,
		Bus:        eventBus,
		GRPCAddr:   lis.Addr().String(),
		grpcServer: grpcServer,
		cancel:     cancel,
	}
}
