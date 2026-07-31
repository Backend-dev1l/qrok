//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"qrok/internal/testutil/integ"
)

func TestNewAgainstRealPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := New(ctx, Config{DSN: integ.PostgresDSN()})
	if err != nil {
		t.Skipf("postgres недоступен (%v); запустите make compose-up", err)
	}
	t.Cleanup(func() { pool.Close() })

	var one int
	require.NoError(t, pool.QueryRow(ctx, "SELECT 1").Scan(&one))
	require.Equal(t, 1, one)
}
