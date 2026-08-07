// Package eventstore persists control-plane events with hybrid payload storage.
package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"qrok/internal/controlplane/infrastructure/models"
	"qrok/pkg/objectstore"
)

var ErrCorruptEvent = errors.New("corrupt event row")

// ObjectStore stores large event payloads outside Postgres.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

// Repository persists and reads events.
type Repository interface {
	Insert(ctx context.Context, ev *models.Event) (bool, error)
	GetByID(ctx context.Context, id string) (*models.Event, error)
	GetByIDForProject(ctx context.Context, projectID, eventID string) (*models.Event, error)
	GetPayload(ctx context.Context, ev *models.Event) ([]byte, error)
	ListByTunnel(ctx context.Context, tunnelID string, limit int) ([]*models.Event, error)
}

type Config struct {
	PayloadThresholdBytes int64
}

type repository struct {
	pool    *pgxpool.Pool
	objects ObjectStore
	cfg     Config
}

func New(pool *pgxpool.Pool, objects ObjectStore, cfg Config) Repository {
	return &repository{pool: pool, objects: objects, cfg: cfg}
}

func (r *repository) Insert(ctx context.Context, ev *models.Event) (bool, error) {
	payloadSize := int32(len(ev.Payload))

	var payload []byte
	var payloadRef *string

	switch {
	case payloadSize == 0:
		payload = nil
	case int64(payloadSize) < r.cfg.PayloadThresholdBytes:
		payload = ev.Payload
	default:
		key := objectstore.ObjectKey(ev.TunnelID, ev.ID)
		if err := r.objects.Put(ctx, key, ev.Payload); err != nil {
			return false, err
		}
		payloadRef = &key
	}

	headersJSON, err := json.Marshal(ev.Headers)
	if err != nil {
		return false, err
	}

	createdAt := ev.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	dedupTag, err := tx.Exec(ctx, `
		INSERT INTO event_dedup (id) VALUES ($1)
		ON CONFLICT (id) DO NOTHING
	`, ev.ID)
	if err != nil {
		return false, err
	}
	if dedupTag.RowsAffected() == 0 {
		return false, nil
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO events (
			id, tunnel_id, topic, partition, broker_offset, key, headers,
			payload, payload_ref, payload_size, is_replay, broker_ts, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
	`,
		ev.ID, ev.TunnelID, ev.Topic, ev.Partition, ev.BrokerOffset, ev.Key, headersJSON,
		payload, payloadRef, payloadSize, ev.IsReplay, ev.BrokerTS, createdAt,
	)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (r *repository) GetPayload(ctx context.Context, ev *models.Event) ([]byte, error) {
	if len(ev.Payload) > 0 {
		return ev.Payload, nil
	}
	if ev.PayloadRef == "" {
		return nil, pgx.ErrNoRows
	}
	return r.objects.Get(ctx, ev.PayloadRef)
}

func (r *repository) GetByID(ctx context.Context, id string) (*models.Event, error) {
	row := r.pool.QueryRow(ctx, eventSelectSQL+` WHERE e.id = $1`, id)
	return scanEvent(row)
}

func (r *repository) GetByIDForProject(ctx context.Context, projectID, eventID string) (*models.Event, error) {
	row := r.pool.QueryRow(ctx, eventSelectSQL+`
		JOIN tunnels t ON t.id = e.tunnel_id
		WHERE e.id = $1 AND t.project_id = $2
	`, eventID, projectID)
	return scanEvent(row)
}

func (r *repository) ListByTunnel(ctx context.Context, tunnelID string, limit int) ([]*models.Event, error) {
	rows, err := r.pool.Query(ctx, eventSelectSQL+`
		WHERE e.tunnel_id = $1
		ORDER BY e.created_at DESC
		LIMIT $2
	`, tunnelID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*models.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

const eventSelectSQL = `
	SELECT e.id, e.tunnel_id, e.topic, e.partition, e.broker_offset, e.key, e.headers,
	       e.payload, e.payload_ref, e.payload_size, e.is_replay, e.broker_ts, e.created_at
	FROM events e
`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanEvent(row rowScanner) (*models.Event, error) {
	var ev models.Event
	var headersJSON []byte
	var payload []byte
	var payloadRef *string
	var brokerTS *time.Time

	err := row.Scan(
		&ev.ID, &ev.TunnelID, &ev.Topic, &ev.Partition, &ev.BrokerOffset, &ev.Key, &headersJSON,
		&payload, &payloadRef, &ev.PayloadSize, &ev.IsReplay, &brokerTS, &ev.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	if len(headersJSON) > 0 {
		if err := json.Unmarshal(headersJSON, &ev.Headers); err != nil {
			return nil, errors.Join(ErrCorruptEvent, err)
		}
	}
	ev.Payload = payload
	if payloadRef != nil {
		ev.PayloadRef = *payloadRef
	}
	ev.BrokerTS = brokerTS
	return &ev, nil
}
