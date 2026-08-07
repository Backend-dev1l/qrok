package auth

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"qrok/internal/controlplane/infrastructure/models"
)

// Repository provides auth-related persistence operations.
type Repository interface {
	FindAgentToken(ctx context.Context, tokenHash, tunnelID string) (tokenID, projectID string, err error)
	FindAPIToken(ctx context.Context, tokenHash string) (tokenID, projectID string, err error)
	FindDevToken(ctx context.Context, tokenHash, tunnelID string) (tokenID, projectID, userID string, err error)
	TunnelOwnedByProject(ctx context.Context, projectID, tunnelID string) (bool, error)
	EventOwnedByProject(ctx context.Context, projectID, eventID string) (bool, error)
	ProjectExists(ctx context.Context, projectID string) (bool, error)

	InsertDeviceAuthorization(ctx context.Context, deviceHash, userCode, projectID string, pollInterval int, expiresAt time.Time) error
	GetDeviceByHash(ctx context.Context, deviceHash string) (status models.DeviceStatus, expiresAt time.Time, accessToken *string, err error)
	UpdateDeviceStatus(ctx context.Context, deviceHash string, status models.DeviceStatus) error
	ConsumeDeviceToken(ctx context.Context, deviceHash string) error
	WithinTx(ctx context.Context, fn func(TxRepository) error) error

	InsertDevToken(ctx context.Context, id, projectID, tokenHash, name string) error
}

// TxRepository exposes auth persistence operations inside a transaction.
type TxRepository interface {
	GetDeviceByUserCodeForUpdate(ctx context.Context, userCode string) (deviceHash string, status models.DeviceStatus, expiresAt time.Time, err error)
	UpdateDeviceStatus(ctx context.Context, deviceHash string, status models.DeviceStatus) error
	ApproveDevice(ctx context.Context, deviceHash, projectID, devTokenID, accessToken string) error
	InsertDevToken(ctx context.Context, id, projectID, tokenHash, name string) error
	ProjectExists(ctx context.Context, projectID string) (bool, error)
}

type repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) Repository {
	return &repository{pool: pool}
}

func (r *repository) FindAgentToken(ctx context.Context, tokenHash, tunnelID string) (string, string, error) {
	var tokenID, projectID string
	err := r.pool.QueryRow(ctx, `
		SELECT at.id, at.project_id
		FROM agent_tokens at
		JOIN tunnels t ON t.project_id = at.project_id
		WHERE at.token_hash = $1
		  AND at.revoked_at IS NULL
		  AND t.id = $2
	`, tokenHash, tunnelID).Scan(&tokenID, &projectID)
	return tokenID, projectID, err
}

func (r *repository) FindAPIToken(ctx context.Context, tokenHash string) (string, string, error) {
	var tokenID, projectID string
	err := r.pool.QueryRow(ctx, `
		SELECT id, project_id
		FROM agent_tokens
		WHERE token_hash = $1
		  AND revoked_at IS NULL
	`, tokenHash).Scan(&tokenID, &projectID)
	return tokenID, projectID, err
}

func (r *repository) FindDevToken(ctx context.Context, tokenHash, tunnelID string) (string, string, string, error) {
	var tokenID, projectID, userID string
	err := r.pool.QueryRow(ctx, `
		SELECT dt.id, dt.project_id, COALESCE(dt.user_id, '')
		FROM dev_tokens dt
		JOIN tunnels t ON t.project_id = dt.project_id
		WHERE dt.token_hash = $1
		  AND dt.revoked_at IS NULL
		  AND t.id = $2
	`, tokenHash, tunnelID).Scan(&tokenID, &projectID, &userID)
	return tokenID, projectID, userID, err
}

func (r *repository) TunnelOwnedByProject(ctx context.Context, projectID, tunnelID string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM tunnels WHERE id = $1 AND project_id = $2
		)
	`, tunnelID, projectID).Scan(&exists)
	return exists, err
}

func (r *repository) EventOwnedByProject(ctx context.Context, projectID, eventID string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM events e
			JOIN tunnels t ON t.id = e.tunnel_id
			WHERE e.id = $1 AND t.project_id = $2
		)
	`, eventID, projectID).Scan(&exists)
	return exists, err
}

func (r *repository) ProjectExists(ctx context.Context, projectID string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM projects WHERE id = $1)`, projectID).Scan(&exists)
	return exists, err
}

func (r *repository) InsertDeviceAuthorization(ctx context.Context, deviceHash, userCode, projectID string, pollInterval int, expiresAt time.Time) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO device_authorizations (
			device_code_hash, user_code, status, project_id, poll_interval_sec, expires_at
		) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6)
	`, deviceHash, userCode, string(models.DeviceStatusPending), projectID, pollInterval, expiresAt)
	return err
}

func (r *repository) GetDeviceByHash(ctx context.Context, deviceHash string) (models.DeviceStatus, time.Time, *string, error) {
	var status string
	var expiresAt time.Time
	var accessToken *string
	err := r.pool.QueryRow(ctx, `
		SELECT status, expires_at, access_token_plaintext
		FROM device_authorizations
		WHERE device_code_hash = $1
	`, deviceHash).Scan(&status, &expiresAt, &accessToken)
	return models.DeviceStatus(status), expiresAt, accessToken, err
}

func (r *repository) UpdateDeviceStatus(ctx context.Context, deviceHash string, status models.DeviceStatus) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE device_authorizations SET status = $1 WHERE device_code_hash = $2
	`, string(status), deviceHash)
	return err
}

func (r *repository) ConsumeDeviceToken(ctx context.Context, deviceHash string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE device_authorizations
		SET status = $1, access_token_plaintext = NULL
		WHERE device_code_hash = $2
	`, string(models.DeviceStatusConsumed), deviceHash)
	return err
}

func (r *repository) InsertDevToken(ctx context.Context, id, projectID, tokenHash, name string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO dev_tokens (id, project_id, token_hash, name)
		VALUES ($1, $2, $3, $4)
	`, id, projectID, tokenHash, name)
	return err
}

func (r *repository) WithinTx(ctx context.Context, fn func(TxRepository) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := fn(&txRepository{tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type txRepository struct {
	tx pgx.Tx
}

func (r *txRepository) GetDeviceByUserCodeForUpdate(ctx context.Context, userCode string) (string, models.DeviceStatus, time.Time, error) {
	var deviceHash string
	var status string
	var expiresAt time.Time
	err := r.tx.QueryRow(ctx, `
		SELECT device_code_hash, status, expires_at
		FROM device_authorizations
		WHERE user_code = $1
		FOR UPDATE
	`, userCode).Scan(&deviceHash, &status, &expiresAt)
	return deviceHash, models.DeviceStatus(status), expiresAt, err
}

func (r *txRepository) UpdateDeviceStatus(ctx context.Context, deviceHash string, status models.DeviceStatus) error {
	_, err := r.tx.Exec(ctx, `
		UPDATE device_authorizations SET status = $1 WHERE device_code_hash = $2
	`, string(status), deviceHash)
	return err
}

func (r *txRepository) ApproveDevice(ctx context.Context, deviceHash, projectID, devTokenID, accessToken string) error {
	_, err := r.tx.Exec(ctx, `
		UPDATE device_authorizations
		SET status = $1, project_id = $2, dev_token_id = $3, access_token_plaintext = $4
		WHERE device_code_hash = $5
	`, string(models.DeviceStatusApproved), projectID, devTokenID, accessToken, deviceHash)
	return err
}

func (r *txRepository) InsertDevToken(ctx context.Context, id, projectID, tokenHash, name string) error {
	_, err := r.tx.Exec(ctx, `
		INSERT INTO dev_tokens (id, project_id, token_hash, name)
		VALUES ($1, $2, $3, $4)
	`, id, projectID, tokenHash, name)
	return err
}

func (r *txRepository) ProjectExists(ctx context.Context, projectID string) (bool, error) {
	var exists bool
	err := r.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM projects WHERE id = $1)`, projectID).Scan(&exists)
	return exists, err
}
