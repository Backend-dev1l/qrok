package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/internal/controlplane/infrastructure/models"
	"qrok/internal/controlplane/service"
	"qrok/pkg/fault"
)

func TestDeviceService(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("start_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeAuthRepo()
		svc := service.NewDeviceService(repo)

		start, err := svc.Start(ctx, "prj-1", "http://localhost/dashboard/device")

		require.NoError(t, err)
		assert.NotEmpty(t, start.DeviceCode)
		assert.NotEmpty(t, start.UserCode)
		assert.Equal(t, "http://localhost/dashboard/device", start.VerificationURI)
		assert.Equal(t, auth.DefaultPollInterval, start.Interval)
		assert.Positive(t, start.ExpiresIn)
	})

	t.Run("poll_pending", func(t *testing.T) {
		t.Parallel()

		repo := newFakeAuthRepo()
		hash := auth.HashDeviceCode("device-code-abc")
		repo.addDevice(hash, deviceRec{
			userCode:  "ABCD-EFGH",
			status:    models.DeviceStatusPending,
			expiresAt: time.Now().UTC().Add(time.Hour),
		})

		svc := service.NewDeviceService(repo)
		result, err := svc.Poll(ctx, "device-code-abc")

		require.NoError(t, err)
		assert.Equal(t, "authorization_pending", result.Error)
	})

	t.Run("poll_unknown_device", func(t *testing.T) {
		t.Parallel()

		svc := service.NewDeviceService(newFakeAuthRepo())
		result, err := svc.Poll(ctx, "unknown-device-code")

		require.NoError(t, err)
		assert.Equal(t, "invalid_grant", result.Error)
	})

	t.Run("poll_approved_returns_token", func(t *testing.T) {
		t.Parallel()

		token := "qrok_dev_secret"
		repo := newFakeAuthRepo()
		hash := auth.HashDeviceCode("device-code-xyz")
		repo.addDevice(hash, deviceRec{
			userCode:    "WXYZ-1234",
			status:      models.DeviceStatusApproved,
			expiresAt:   time.Now().UTC().Add(time.Hour),
			accessToken: &token,
		})

		svc := service.NewDeviceService(repo)
		result, err := svc.Poll(ctx, "device-code-xyz")

		require.NoError(t, err)
		assert.Equal(t, token, result.AccessToken)
		assert.Equal(t, "Bearer", result.TokenType)

		status, _, accessToken, err := repo.GetDeviceByHash(ctx, hash)
		require.NoError(t, err)
		assert.Equal(t, models.DeviceStatusConsumed, status)
		assert.Nil(t, accessToken)
	})

	t.Run("poll_expired", func(t *testing.T) {
		t.Parallel()

		repo := newFakeAuthRepo()
		hash := auth.HashDeviceCode("expired-code")
		repo.addDevice(hash, deviceRec{
			userCode:  "EXPI-RED1",
			status:    models.DeviceStatusPending,
			expiresAt: time.Now().UTC().Add(-time.Minute),
		})

		svc := service.NewDeviceService(repo)
		result, err := svc.Poll(ctx, "expired-code")

		require.NoError(t, err)
		assert.Equal(t, "expired_token", result.Error)
	})

	t.Run("poll_empty_device_code", func(t *testing.T) {
		t.Parallel()

		svc := service.NewDeviceService(newFakeAuthRepo())
		_, err := svc.Poll(ctx, "")

		require.Error(t, err)
		assert.Equal(t, fault.ErrValidation, fault.FromError(err).Code())
	})

	t.Run("approve_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeAuthRepo()
		repo.projects["prj-1"] = true
		hash := auth.HashDeviceCode("approve-me")
		repo.addDevice(hash, deviceRec{
			userCode:  "APPR-OVE1",
			status:    models.DeviceStatusPending,
			expiresAt: time.Now().UTC().Add(time.Hour),
		})

		svc := service.NewDeviceService(repo)
		err := svc.Approve(ctx, "APPR-OVE1", "prj-1")

		require.NoError(t, err)

		status, _, accessToken, err := repo.GetDeviceByHash(ctx, hash)
		require.NoError(t, err)
		assert.Equal(t, models.DeviceStatusApproved, status)
		require.NotNil(t, accessToken)
		assert.NotEmpty(t, *accessToken)
	})

	t.Run("approve_unknown_user_code", func(t *testing.T) {
		t.Parallel()

		svc := service.NewDeviceService(newFakeAuthRepo())
		err := svc.Approve(ctx, "NOPE-CODE", "prj-1")

		require.Error(t, err)
		assert.Equal(t, fault.ErrNotFound, fault.FromError(err).Code())
	})

	t.Run("approve_already_approved", func(t *testing.T) {
		t.Parallel()

		repo := newFakeAuthRepo()
		repo.projects["prj-1"] = true
		repo.addDevice("hash", deviceRec{
			userCode:  "DONE-CODE",
			status:    models.DeviceStatusApproved,
			expiresAt: time.Now().UTC().Add(time.Hour),
		})

		svc := service.NewDeviceService(repo)
		err := svc.Approve(ctx, "DONE-CODE", "prj-1")

		require.Error(t, err)
		assert.Equal(t, fault.ErrConflict, fault.FromError(err).Code())
	})

	t.Run("approve_missing_user_code", func(t *testing.T) {
		t.Parallel()

		svc := service.NewDeviceService(newFakeAuthRepo())
		err := svc.Approve(ctx, "", "prj-1")

		require.Error(t, err)
		assert.Equal(t, fault.ErrValidation, fault.FromError(err).Code())
	})
}
