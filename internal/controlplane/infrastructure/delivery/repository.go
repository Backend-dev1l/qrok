// Package delivery persists event delivery attempts.
package delivery

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"

	"qrok/internal/controlplane/infrastructure/models"
)

var ErrDeliveryEventMismatch = errors.New("delivery belongs to a different event")

// Repository stores delivery records in Postgres.
type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) CreatePending(ctx context.Context, rec *models.Delivery) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO deliveries (id, event_id, target_id, kind, status)
		VALUES ($1, $2, $3, $4, $5)
	`, rec.ID, rec.EventID, rec.TargetID, string(rec.Kind), string(rec.Status))
	return err
}

func (r *Repository) UpsertResult(ctx context.Context, rec *models.Delivery) error {
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO deliveries (id, event_id, target_id, kind, status, status_code, error, latency_ms)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			status_code = EXCLUDED.status_code,
			error = EXCLUDED.error,
			latency_ms = EXCLUDED.latency_ms
		WHERE deliveries.event_id = EXCLUDED.event_id
	`, rec.ID, rec.EventID, rec.TargetID, string(rec.Kind), string(rec.Status),
		rec.StatusCode, nullIfEmpty(rec.Error), rec.LatencyMS)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrDeliveryEventMismatch
	}
	return nil
}

func (r *Repository) GetByID(ctx context.Context, id string) (*models.Delivery, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, event_id, target_id, kind, status, status_code, error, latency_ms, created_at
		FROM deliveries WHERE id = $1
	`, id)

	var rec models.Delivery
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

	rec.Kind = models.DeliveryKind(kind)
	rec.Status = models.DeliveryStatus(status)
	rec.StatusCode = statusCode
	rec.LatencyMS = latency
	if errText != nil {
		rec.Error = *errText
	}
	return &rec, nil
}

func (r *Repository) ListByEventID(ctx context.Context, eventID string) ([]*models.Delivery, error) {
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

func (r *Repository) ListLatestByEventIDs(ctx context.Context, eventIDs []string) (map[string]*models.Delivery, error) {
	if len(eventIDs) == 0 {
		return map[string]*models.Delivery{}, nil
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
	out := make(map[string]*models.Delivery, len(list))
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

func scanDeliveries(rows rowScanner) ([]*models.Delivery, error) {
	var out []*models.Delivery
	for rows.Next() {
		var rec models.Delivery
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

		rec.Kind = models.DeliveryKind(kind)
		rec.Status = models.DeliveryStatus(status)
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
