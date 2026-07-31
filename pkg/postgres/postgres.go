// Package postgres — подключение к PostgreSQL через pgxpool
// с ошибками в формате fault. Значения пула задаёт вызывающий
// (дефолты живут в internal/config); нулевые поля не переопределяют
// собственные умолчания pgxpool.
package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"qrok/pkg/fault"
)

type Config struct {
	DSN             string // postgres://user:pass@host:5432/db?sslmode=disable
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	ConnectTimeout  time.Duration // на установку соединения и стартовый ping
}

// New создаёт пул соединений и проверяет его ping'ом — ошибка конфигурации
// или недоступности базы видна сразу на старте, а не при первом запросе.
func New(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := cfg.poolConfig()
	if err != nil {
		return nil, err
	}

	if timeout := pc.ConnConfig.ConnectTimeout; timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, connectFault(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, connectFault(err)
	}
	return pool, nil
}

func connectFault(err error) error {
	return fault.ErrServiceUnavail.
		Wrap(err, "PostgreSQL недоступен").
		WithOp("postgres.connect").
		WithHint("проверьте DSN и что база запущена (make compose-up)")
}

func (c Config) poolConfig() (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(c.DSN)
	if err != nil {
		return nil, fault.ErrValidation.
			Wrap(err, "невалидный DSN PostgreSQL").
			WithOp("postgres.parse_dsn").
			WithHint("формат: postgres://user:pass@host:5432/db?sslmode=disable")
	}

	setIfPositive(&pc.MaxConns, c.MaxConns)
	setIfPositive(&pc.MinConns, c.MinConns)
	setIfPositive(&pc.MaxConnLifetime, c.MaxConnLifetime)
	setIfPositive(&pc.MaxConnIdleTime, c.MaxConnIdleTime)
	setIfPositive(&pc.ConnConfig.ConnectTimeout, c.ConnectTimeout)

	return pc, nil
}

func setIfPositive[T int32 | time.Duration](dst *T, v T) {
	if v > 0 {
		*dst = v
	}
}
