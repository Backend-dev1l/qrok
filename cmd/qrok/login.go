package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"qrok/internal/devcli/credentials"
	"qrok/pkg/fault"
)

func newLoginCmd() *cobra.Command {
	var (
		apiURL    string
		gateway   string
		projectID string
	)

	cmd := &cobra.Command{
		Use:   "login",
		Short: "Авторизация dev-клиента (OAuth device flow через дашборд)",
		RunE: func(cmd *cobra.Command, args []string) error {
			apiURL = strings.TrimRight(apiURL, "/")
			if apiURL == "" {
				return fault.ErrValidation.New("specify --api control plane URL").WithOp("cli.login")
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Minute)
			defer cancel()

			startBody := map[string]string{}
			if projectID != "" {
				startBody["project_id"] = projectID
			}
			raw, err := json.Marshal(startBody)
			if err != nil {
				return fault.ErrInternal.Wrap(err, "failed to build request").WithOp("cli.login")
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/api/v1/oauth/device/code", bytes.NewReader(raw))
			if err != nil {
				return fault.ErrInternal.Wrap(err, "failed to create request").WithOp("cli.login")
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fault.ErrServiceUnavail.Wrap(err, "failed to call API").WithOp("cli.login")
			}
			defer resp.Body.Close()

			startPayload, _ := io.ReadAll(resp.Body)
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return fault.ErrServiceUnavail.
					Newf("API вернул HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(startPayload))).
					WithOp("cli.login")
			}

			var start struct {
				DeviceCode      string `json:"device_code"`
				UserCode        string `json:"user_code"`
				VerificationURI string `json:"verification_uri"`
				ExpiresIn       int    `json:"expires_in"`
				Interval        int    `json:"interval"`
			}
			if err := json.Unmarshal(startPayload, &start); err != nil {
				return fault.ErrInternal.Wrap(err, "failed to parse API response").WithOp("cli.login")
			}

			interval := time.Duration(start.Interval) * time.Second
			if interval <= 0 {
				interval = 5 * time.Second
			}

			fmt.Fprintf(os.Stdout, "Open in browser: %s\n", start.VerificationURI)
			fmt.Fprintf(os.Stdout, "Verification code: %s\n", start.UserCode)
			if projectID != "" {
				fmt.Fprintf(os.Stdout, "Project ID: %s\n", projectID)
			} else {
				fmt.Fprintln(os.Stdout, "On the page enter project_id (from make seed-dev).")
			}
			fmt.Fprintln(os.Stdout, "Waiting for approval…")

			tokenBody, _ := json.Marshal(map[string]string{"device_code": start.DeviceCode})
			pollURL := apiURL + "/api/v1/oauth/device/token"

			for {
				select {
				case <-ctx.Done():
					return fault.ErrTimeout.Wrap(ctx.Err(), "approval wait timed out").WithOp("cli.login")
				default:
				}

				time.Sleep(interval)

				pollReq, err := http.NewRequestWithContext(ctx, http.MethodPost, pollURL, bytes.NewReader(tokenBody))
				if err != nil {
					return fault.ErrInternal.Wrap(err, "failed to create poll request").WithOp("cli.login")
				}
				pollReq.Header.Set("Content-Type", "application/json")

				pollResp, err := http.DefaultClient.Do(pollReq)
				if err != nil {
					return fault.ErrServiceUnavail.Wrap(err, "poll API error").WithOp("cli.login")
				}

				pollPayload, _ := io.ReadAll(pollResp.Body)
				_ = pollResp.Body.Close()

				var poll struct {
					AccessToken string `json:"access_token"`
					TokenType   string `json:"token_type"`
					Error       string `json:"error"`
					Description string `json:"error_description"`
				}
				_ = json.Unmarshal(pollPayload, &poll)

				if poll.AccessToken != "" {
					if err := credentials.Save(&credentials.File{
						APIURL:      apiURL,
						Gateway:     gateway,
						AccessToken: poll.AccessToken,
					}); err != nil {
						return err
					}
					path, _ := credentials.DefaultPath()
					fmt.Fprintf(os.Stdout, "Login complete. Token saved to %s\n", path)
					fmt.Fprintln(os.Stdout, "Run: qrok listen --config deploy/listen.example.yaml")
					return nil
				}

				switch poll.Error {
				case "authorization_pending":
					continue
				case "slow_down":
					interval += 2 * time.Second
					continue
				case "":
					if pollResp.StatusCode >= 200 && pollResp.StatusCode < 300 {
						continue
					}
					return fault.ErrServiceUnavail.
						Newf("unexpected poll response: HTTP %d %s", pollResp.StatusCode, string(pollPayload)).
						WithOp("cli.login")
				default:
					msg := poll.Description
					if msg == "" {
						msg = poll.Error
					}
					return fault.ErrUnauthorized.New(msg).WithOp("cli.login")
				}
			}
		},
	}

	cmd.Flags().StringVar(&apiURL, "api", "http://127.0.0.1:8080", "URL control plane API")
	cmd.Flags().StringVar(&gateway, "gateway", "", "gRPC gateway (сохраняется в credentials, опционально)")
	cmd.Flags().StringVar(&projectID, "project", "", "project_id для привязки dev-токена (из make seed-dev)")
	return cmd
}
