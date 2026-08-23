package service

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/oklog/ulid/v2"

	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/internal/controlplane/infrastructure/models"
	"qrok/pkg/fault"
)

type deviceAuthQuerier interface {
	InsertDeviceAuthorization(ctx context.Context, deviceHash, userCode, projectID string, pollInterval int, expiresAt time.Time) error
	GetDeviceByHash(ctx context.Context, deviceHash string) (status models.DeviceStatus, expiresAt time.Time, accessToken *string, err error)
	UpdateDeviceStatus(ctx context.Context, deviceHash string, status models.DeviceStatus) error
	ConsumeDeviceToken(ctx context.Context, deviceHash string) error
	WithinTx(ctx context.Context, fn func(deviceAuthTxQuerier) error) error
}

type deviceAuthTxQuerier interface {
	GetDeviceByUserCodeForUpdate(ctx context.Context, userCode string) (deviceHash string, status models.DeviceStatus, expiresAt time.Time, err error)
	UpdateDeviceStatus(ctx context.Context, deviceHash string, status models.DeviceStatus) error
	ApproveDevice(ctx context.Context, deviceHash, projectID, devTokenID, accessToken string) error
	InsertDevToken(ctx context.Context, id, projectID, tokenHash, name string) error
	ProjectExists(ctx context.Context, projectID string) (bool, error)
}

type deviceAuthRepo struct {
	*auth.Repository
}

// DeviceAuthRepository adapts the Postgres auth repository for the device flow.
func DeviceAuthRepository(repo *auth.Repository) deviceAuthQuerier {
	return deviceAuthRepo{Repository: repo}
}

func (d deviceAuthRepo) WithinTx(ctx context.Context, fn func(deviceAuthTxQuerier) error) error {
	return d.Repository.WithinTx(ctx, func(tx *auth.Tx) error {
		return fn(tx)
	})
}

// Device implements the OAuth device authorization flow.
type Device struct {
	repo deviceAuthQuerier
	now  func() time.Time
}

func NewDeviceService(repo deviceAuthQuerier) *Device {
	return &Device{
		repo: repo,
		now:  time.Now,
	}
}

func (s *Device) Start(ctx context.Context, projectID, verificationURI string) (*models.DeviceStart, error) {
	const op = "device.start"

	deviceCode, err := auth.RandomURLSafe(32)
	if err != nil {
		return nil, internalErr(op, err)
	}
	userCode, err := auth.RandomUserCode()
	if err != nil {
		return nil, internalErr(op, err)
	}

	expiresAt := s.now().UTC().Add(auth.DefaultDeviceTTL)
	deviceHash := auth.HashDeviceCode(deviceCode)

	if err := s.repo.InsertDeviceAuthorization(ctx, deviceHash, userCode, projectID, auth.DefaultPollInterval, expiresAt); err != nil {
		return nil, mapRepoErr(op, err)
	}

	return &models.DeviceStart{
		DeviceCode:      deviceCode,
		UserCode:        userCode,
		VerificationURI: verificationURI,
		ExpiresIn:       int(auth.DefaultDeviceTTL.Seconds()),
		Interval:        auth.DefaultPollInterval,
	}, nil
}

func (s *Device) Approve(ctx context.Context, subject *models.Subject, userCode, requestedProjectID string) error {
	const op = "device.approve"

	if err := ensureSubject(subject); err != nil {
		return err
	}
	projectID := subject.ProjectID
	if subject.AllowAll {
		projectID = requestedProjectID
	} else if requestedProjectID != "" && requestedProjectID != projectID {
		return forbiddenErr(op, "cannot approve a device for another project")
	}
	if projectID == "" {
		return fault.ErrValidation.New("project_id is required").WithOp(op)
	}

	userCode = auth.NormalizeUserCode(userCode)

	return s.repo.WithinTx(ctx, func(tx deviceAuthTxQuerier) error {
		deviceHash, status, expiresAt, err := tx.GetDeviceByUserCodeForUpdate(ctx, userCode)
		if err != nil {
			return notFoundErr(op, "unknown user_code", err)
		}
		if status != models.DeviceStatusPending {
			return fault.ErrConflict.Newf("session already in status %s", status).WithOp(op)
		}
		if s.now().UTC().After(expiresAt) {
			_ = tx.UpdateDeviceStatus(ctx, deviceHash, models.DeviceStatusExpired)
			return fault.ErrUnauthorized.New("code expired, run qrok login again").WithOp(op)
		}

		exists, err := tx.ProjectExists(ctx, projectID)
		if err != nil {
			return mapRepoErr(op, err)
		}
		if !exists {
			return fault.ErrNotFound.New("project not found").WithOp(op).WithArg("project_id", projectID)
		}

		plaintext, tokenHash, err := auth.GenerateDevToken()
		if err != nil {
			return internalErr(op, err)
		}

		devTokenID := ulid.MustNew(ulid.Now(), rand.Reader).String()
		if err := tx.InsertDevToken(ctx, devTokenID, projectID, tokenHash, "qrok login"); err != nil {
			return mapRepoErr(op, err)
		}
		if err := tx.ApproveDevice(ctx, deviceHash, projectID, devTokenID, plaintext); err != nil {
			return mapRepoErr(op, err)
		}
		return nil
	})
}

func (s *Device) Poll(ctx context.Context, deviceCode string) (*models.DeviceTokenPoll, error) {
	const op = "device.poll"

	deviceHash := auth.HashDeviceCode(deviceCode)
	status, expiresAt, accessToken, err := s.repo.GetDeviceByHash(ctx, deviceHash)
	if err != nil {
		if err == pgx.ErrNoRows {
			return &models.DeviceTokenPoll{
				Error:       "invalid_grant",
				Description: "unknown device_code",
			}, nil
		}
		return nil, mapRepoErr(op, err)
	}

	if s.now().UTC().After(expiresAt) && status == models.DeviceStatusPending {
		_ = s.repo.UpdateDeviceStatus(ctx, deviceHash, models.DeviceStatusExpired)
		status = models.DeviceStatusExpired
	}

	switch status {
	case models.DeviceStatusPending:
		return &models.DeviceTokenPoll{
			Error:       "authorization_pending",
			Description: "dashboard approval pending",
		}, nil
	case models.DeviceStatusDenied:
		return &models.DeviceTokenPoll{Error: "access_denied", Description: "access denied"}, nil
	case models.DeviceStatusExpired:
		return &models.DeviceTokenPoll{Error: "expired_token", Description: "code expired"}, nil
	case models.DeviceStatusConsumed:
		return &models.DeviceTokenPoll{Error: "invalid_grant", Description: "token already issued"}, nil
	case models.DeviceStatusApproved:
		if accessToken == nil || *accessToken == "" {
			return nil, fault.ErrInternal.New("corrupt device session").WithOp(op)
		}
		token := *accessToken
		if err := s.repo.ConsumeDeviceToken(ctx, deviceHash); err != nil {
			return nil, mapRepoErr(op, err)
		}
		return &models.DeviceTokenPoll{
			AccessToken: token,
			TokenType:   "Bearer",
		}, nil
	default:
		return nil, fault.ErrInternal.Newf("unknown device session status: %s", status).WithOp(op)
	}
}
