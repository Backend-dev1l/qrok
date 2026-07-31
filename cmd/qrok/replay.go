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

	"qrok/pkg/fault"
)

func newReplayCmd() *cobra.Command {
	var apiURL string
	var target string

	cmd := &cobra.Command{
		Use:   "replay [event-id]",
		Short: "Переотправить сохранённое событие на localhost через облако",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			eventID := strings.TrimSpace(args[0])
			if eventID == "" {
				return fault.ErrValidation.New("event_id обязателен").WithOp("cli.replay")
			}

			body := map[string]string{}
			if target != "" {
				body["target"] = target
			}
			raw, err := json.Marshal(body)
			if err != nil {
				return fault.ErrInternal.Wrap(err, "не удалось собрать запрос").WithOp("cli.replay")
			}

			url := strings.TrimRight(apiURL, "/") + "/api/v1/events/" + eventID + "/replay"
			ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
			if err != nil {
				return fault.ErrInternal.Wrap(err, "не удалось создать запрос").WithOp("cli.replay")
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fault.ErrServiceUnavail.Wrap(err, "не удалось вызвать API").WithOp("cli.replay")
			}
			defer resp.Body.Close()

			respBody, _ := io.ReadAll(resp.Body)
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return fault.ErrServiceUnavail.
					Newf("API вернул HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody))).
					WithOp("cli.replay")
			}

			var result struct {
				DeliveryID string `json:"delivery_id"`
				EventID    string `json:"event_id"`
				TargetID   string `json:"target_id"`
				Status     string `json:"status"`
			}
			if err := json.Unmarshal(respBody, &result); err != nil {
				return fault.ErrInternal.Wrap(err, "не удалось разобрать ответ API").WithOp("cli.replay")
			}

			fmt.Fprintf(os.Stdout, "replay поставлен: delivery_id=%s event_id=%s target=%s status=%s\n",
				result.DeliveryID, result.EventID, result.TargetID, result.Status)
			return nil
		},
	}

	cmd.Flags().StringVar(&apiURL, "api", "http://localhost:8080", "URL control plane API")
	cmd.Flags().StringVar(&target, "target", "", "dev_client_id (пусто = broadcast)")
	return cmd
}
