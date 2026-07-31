package postgres

import (
	"context"
	"testing"
	"time"

	"qrok/pkg/fault"
)

func TestPoolConfigZeroValuesKeepPgxDefaults(t *testing.T) {
	// Дефолты пула теперь живут в internal/config; нулевые поля Config
	// не должны затирать собственные умолчания pgxpool.
	parsed, err := Config{DSN: "postgres://qrok:qrok@localhost:5432/qrok"}.poolConfig()
	if err != nil {
		t.Fatal(err)
	}

	if parsed.MaxConns <= 0 {
		t.Errorf("pgxpool-дефолт MaxConns затёрт нулём: %d", parsed.MaxConns)
	}
}

func TestPoolConfigOverrides(t *testing.T) {
	pc, err := Config{
		DSN:            "postgres://qrok:qrok@localhost:5432/qrok",
		MaxConns:       50,
		MinConns:       5,
		ConnectTimeout: 2 * time.Second,
	}.poolConfig()
	if err != nil {
		t.Fatal(err)
	}

	if pc.MaxConns != 50 || pc.MinConns != 5 {
		t.Errorf("переопределения не применились: max=%d min=%d", pc.MaxConns, pc.MinConns)
	}
	if pc.ConnConfig.ConnectTimeout != 2*time.Second {
		t.Errorf("ConnectTimeout = %v", pc.ConnConfig.ConnectTimeout)
	}
}

func TestInvalidDSN(t *testing.T) {
	_, err := New(context.Background(), Config{DSN: "не dsn вообще"})
	if fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("ожидался VALIDATION_ERROR, получено: %v", err)
	}
}

func TestUnreachableHost(t *testing.T) {
	// Порт 1 на loopback закрыт — connection refused приходит мгновенно.
	_, err := New(context.Background(), Config{
		DSN:            "postgres://u:p@127.0.0.1:1/db",
		ConnectTimeout: 2 * time.Second,
	})

	if fault.CodeOf(err) != fault.ErrServiceUnavail {
		t.Errorf("ожидался SERVICE_UNAVAILABLE, получено: %v", err)
	}
	if !fault.IsRetryable(err) {
		t.Error("недоступность базы должна быть retryable")
	}
}
