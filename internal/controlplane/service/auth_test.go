package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"qrok/pkg/fault"
)

func TestAuthService(t *testing.T) {
	t.Parallel()

	const (
		apiToken   = "qrok_agt_api-token-secret"
		agentToken = "qrok_agt_agent-token-secret"
		devToken   = "qrok_dev_dev-token-secret"
		tunnelID   = "tunnel-1"
		projectID  = "prj-1"
	)

	ctx := context.Background()

	t.Run("authenticate_api_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeAuthRepo()
		repo.addAPIToken(apiToken, "tok-api", projectID)

		svc := NewAuthService(repo)
		subject, err := svc.AuthenticateAPI(ctx, apiToken)

		require.NoError(t, err)
		assert.Equal(t, "tok-api", subject.TokenID)
		assert.Equal(t, projectID, subject.ProjectID)
	})

	t.Run("authenticate_api_invalid_token", func(t *testing.T) {
		t.Parallel()

		svc := NewAuthService(newFakeAuthRepo())
		_, err := svc.AuthenticateAPI(ctx, "qrok_agt_unknown")

		require.Error(t, err)
		assert.Equal(t, fault.ErrUnauthorized, fault.FromError(err).Code())
	})

	t.Run("authenticate_api_accepts_dev_token", func(t *testing.T) {
		t.Parallel()

		repo := newFakeAuthRepo()
		repo.addDevToken(devToken, tunnelID, "tok-dev", projectID, "user-1")

		subject, err := NewAuthService(repo).AuthenticateAPI(ctx, devToken)

		require.NoError(t, err)
		assert.Equal(t, projectID, subject.ProjectID)
	})

	t.Run("authenticate_agent_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeAuthRepo()
		repo.addAgentToken(agentToken, tunnelID, "tok-agent", projectID)

		svc := NewAuthService(repo)
		subject, err := svc.AuthenticateAgent(ctx, agentToken, tunnelID)

		require.NoError(t, err)
		assert.Equal(t, "tok-agent", subject.TokenID)
		assert.Equal(t, projectID, subject.ProjectID)
		assert.Equal(t, tunnelID, subject.TunnelID)
	})

	t.Run("authenticate_dev_success", func(t *testing.T) {
		t.Parallel()

		repo := newFakeAuthRepo()
		repo.addDevToken(devToken, tunnelID, "tok-dev", projectID, "user-1")

		svc := NewAuthService(repo)
		subject, err := svc.AuthenticateDev(ctx, devToken, tunnelID)

		require.NoError(t, err)
		assert.Equal(t, "tok-dev", subject.TokenID)
		assert.Equal(t, projectID, subject.ProjectID)
		assert.Equal(t, "user-1", subject.UserID)
		assert.Equal(t, tunnelID, subject.TunnelID)
	})

	t.Run("authenticate_dev_wrong_prefix", func(t *testing.T) {
		t.Parallel()

		svc := NewAuthService(newFakeAuthRepo())
		_, err := svc.AuthenticateDev(ctx, "qrok_agt_not-a-dev-token", tunnelID)

		require.Error(t, err)
		assert.Equal(t, fault.ErrUnauthorized, fault.FromError(err).Code())
	})

	t.Run("insecure_subject", func(t *testing.T) {
		t.Parallel()

		subject := NewAuthService(newFakeAuthRepo()).InsecureSubject()
		require.True(t, subject.AllowAll)
	})
}
