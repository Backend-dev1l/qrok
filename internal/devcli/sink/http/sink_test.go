package http_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	httpsink "qrok/internal/devcli/sink/http"
	qrokv1 "qrok/internal/proto/qrok/v1"
)

func TestSinkDeliver(t *testing.T) {
	t.Parallel()

	var gotEventID string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEventID = r.Header.Get("X-Qrok-Event-Id")
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"x":1}` {
			t.Fatalf("body = %q", body)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sink := httpsink.New(srv.URL, 0)
	result := sink.Deliver(context.Background(), &qrokv1.EventEnvelope{
		EventId: "01ABC",
		Topic:   "demo",
		Payload: []byte(`{"x":1}`),
	})

	if result.Error != "" || result.StatusCode != 200 {
		t.Fatalf("result = %+v", result)
	}
	if gotEventID != "01ABC" {
		t.Fatalf("event id header = %q", gotEventID)
	}
}
