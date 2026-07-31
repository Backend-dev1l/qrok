// Package delivery persists event delivery attempts.
package delivery

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"qrok/internal/controlplane/model"
)

// Repository stores delivery records in Postgres.
type Repository interface {
	CreatePending(ctx context.Context, rec *model.Delivery) error
	UpsertResult(ctx context.Context, rec *model.Delivery) error
	GetByID(ctx context.Context, id string) (*model.Delivery, error)
	ListByEventID(ctx context.Context, eventID string) ([]*model.Delivery, error)
	ListLatestByEventIDs(ctx context.Context, eventIDs []string) (map[string]*model.Delivery, error)
}

type repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) Repository {
	return &repository{pool: pool}
}

func (r *repository) CreatePending(ctx context.Context, rec *model.Delivery) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO deliveries (id, event_id, target_id, kind, status)
		VALUES ($1, $2, $3, $4, $5)
	`, rec.ID, rec.EventID, rec.TargetID, string(rec.Kind), string(rec.Status))
	return err
}

func (r *repository) UpsertResult(ctx context.Context, rec *model.Delivery) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO deliveries (id, event_id, target_id, kind, status, status_code, error, latency_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			status_code = EXCLUDED.status_code,
			error = EXCLUDED.error,
			latency_ms = EXCLUDED.latency_ms
	`, rec.ID, rec.EventID, rec.TargetID, string(rec.Kind), string(rec.Status),
		rec.StatusCode, nullIfEmpty(rec.Error), rec.LatencyMS)
	return err
}

func (r *repository) GetByID(ctx context.Context, id string) (*model.Delivery, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, event_id, target_id, kind, status, status_code, error, latency_ms, created_at
		FROM deliveries WHERE id = $1
	`, id)

	var rec model.Delivery
	var kind string
	var status string
	var statusCode *int32
	var latency *int32
	var errText *string

	err := row.Scan(
		&rec.ID, &rec.EventID, &rec.TargetID, &kind, &status,
		&statusCode, &errText, &latency, &rec.CreatedAt,
	)
	if err != nil {
		return nil, err
	}

	rec.Kind = model.DeliveryKind(kind)
	rec.Status = model.DeliveryStatus(status)
	rec.StatusCode = statusCode
	rec.LatencyMS = latency
	if errText != nil {
		rec.Error = *errText
	}
	return &rec, nil
}

func (r *repository) ListByEventID(ctx context.Context, eventID string) ([]*model.Delivery, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, event_id, target_id, kind, status, status_code, error, latency_ms, created_at
		FROM deliveries
		WHERE event_id = $1
		ORDER BY created_at DESC
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanDeliveries(rows)
}

func (r *repository) ListLatestByEventIDs(ctx context.Context, eventIDs []string) (map[string]*model.Delivery, error) {
	if len(eventIDs) == 0 {
		return map[string]*model.Delivery{}, nil
	}

	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT ON (event_id)
			id, event_id, target_id, kind, status, status_code, error, latency_ms, created_at
		FROM deliveries
		WHERE event_id = ANY($1)
		ORDER BY event_id, created_at DESC
	`, eventIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list, err := scanDeliveries(rows)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*model.Delivery, len(list))
	for _, rec := range list {
		out[rec.EventID] = rec
	}
	return out, nil
}

type rowScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanDeliveries(rows rowScanner) ([]*model.Delivery, error) {
	var out []*model.Delivery
	for rows.Next() {
		var rec model.Delivery
		var kind string
		var status string
		var statusCode *int32
		var latency *int32
		var errText *string

		if err := rows.Scan(
			&rec.ID, &rec.EventID, &rec.TargetID, &kind, &status,
			&statusCode, &errText, &latency, &rec.CreatedAt,
		); err != nil {
			return nil, err
		}

		rec.Kind = model.DeliveryKind(kind)
		rec.Status = model.DeliveryStatus(status)
		rec.StatusCode = statusCode
		rec.LatencyMS = latency
		if errText != nil {
			rec.Error = *errText
		}
		out = append(out, &rec)
	}
	return out, rows.Err()
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
