// seed — наполняет dev-БД org/project/tunnel/agent_token для локального тестирования.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"qrok/internal/config"
	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/pkg/fault"
	"qrok/pkg/postgres"
)

const (
	devOrgID     = "01DEVORG000000000000000000"
	devProjectID = "01DEVPRJ000000000000000000"
	devTunnelID  = "dev-tunnel"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, fault.RenderCLI(err))
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	cfg, err := config.LoadServer("")
	if err != nil {
		return err
	}

	pool, err := postgres.New(ctx, cfg.Postgres.Pool())
	if err != nil {
		return err
	}
	defer pool.Close()

	plaintext, hash, err := auth.GenerateAgentToken()
	if err != nil {
		return err
	}

	if err := seed(ctx, pool, hash); err != nil {
		return err
	}

	fmt.Println("Dev-данные созданы:")
	fmt.Println("  org_id:     ", devOrgID)
	fmt.Println("  project_id: ", devProjectID)
	fmt.Println("  tunnel_id:  ", devTunnelID)
	fmt.Println("  agent_token:", plaintext)
	fmt.Println()
	fmt.Println("Dev-клиент (OAuth device flow):")
	fmt.Println("  qrok login --api http://127.0.0.1:8080 --project", devProjectID)
	fmt.Println("  qrok listen --config deploy/listen.example.yaml")
	fmt.Println()
	fmt.Println("Агент:")
	fmt.Println("  qrok agent start --config deploy/agent.example.yaml")
	return nil
}

func seed(ctx context.Context, pool *pgxpool.Pool, tokenHash string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO orgs (id, name) VALUES ($1, 'Dev Org')
		ON CONFLICT (id) DO NOTHING
	`, devOrgID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO projects (id, org_id, name) VALUES ($1, $2, 'dev')
		ON CONFLICT (id) DO NOTHING
	`, devProjectID, devOrgID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO tunnels (id, project_id, name, source_type, topics)
		VALUES ($1, $2, 'dev', 'kafka', ARRAY['demo'])
		ON CONFLICT (id) DO NOTHING
	`, devTunnelID, devProjectID)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO agent_tokens (id, project_id, token_hash, name)
		VALUES ('01DEVTOK000000000000000000', $1, $2, 'dev-token')
		ON CONFLICT (id) DO UPDATE SET token_hash = EXCLUDED.token_hash, revoked_at = NULL
	`, devProjectID, tokenHash)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
