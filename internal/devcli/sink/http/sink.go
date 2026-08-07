package http

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	qrokv1 "qrok/internal/proto/qrok/v1"
)

// Delivery — результат доставки на localhost.
type Delivery struct {
	StatusCode int32
	LatencyMS  int64
	Error      string
}

// Sink доставляет событие на локальный endpoint.
type Sink interface {
	Deliver(ctx context.Context, ev *qrokv1.EventEnvelope) Delivery
}

// Client доставляет событие POST-запросом на локальный URL.
type Client struct {
	url    string
	client *http.Client
}

func New(url string, timeout time.Duration) Sink {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Client{
		url:    url,
		client: &http.Client{Timeout: timeout},
	}
}

func (c *Client) Deliver(ctx context.Context, ev *qrokv1.EventEnvelope) Delivery {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(ev.GetPayload()))
	if err != nil {
		return Delivery{Error: err.Error(), LatencyMS: time.Since(start).Milliseconds()}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Qrok-Event-Id", ev.GetEventId())
	req.Header.Set("X-Qrok-Topic", ev.GetTopic())
	req.Header.Set("X-Qrok-Key", string(ev.GetKey()))
	req.Header.Set("X-Qrok-Offset", strconv.FormatInt(ev.GetOffset(), 10))
	req.Header.Set("X-Qrok-Replay", strconv.FormatBool(ev.GetIsReplay()))
	for k, v := range ev.GetHeaders() {
		req.Header.Set("X-Qrok-Header-"+k, v)
	}

	resp, err := c.client.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return Delivery{Error: err.Error(), LatencyMS: latency}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Delivery{
			StatusCode: int32(resp.StatusCode),
			LatencyMS:  latency,
			Error:      fmt.Sprintf("localhost returned HTTP %d", resp.StatusCode),
		}
	}
	return Delivery{StatusCode: int32(resp.StatusCode), LatencyMS: latency}
}
