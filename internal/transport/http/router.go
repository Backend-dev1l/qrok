// Package httpapi — REST API control plane (chi).
package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"qrok/internal/config"
	"qrok/internal/controlplane/infrastructure/models"
	"qrok/internal/controlplane/service"
	"qrok/internal/middleware"
	"qrok/pkg/fault"
)

// Deps — зависимости HTTP API.
type Deps struct {
	Events service.EventService
	Replay service.ReplayService
	Device service.DeviceService
	Auth   service.AuthService
	Log    *slog.Logger
}

// NewRouter создаёт chi-роутер со стеком middleware control plane.
func NewRouter(httpCfg config.HTTP, deps Deps) http.Handler {
	if deps.Log == nil {
		deps.Log = slog.Default()
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.CORS(httpCfg.CORSOrigins))
	r.Use(middleware.SecurityHeaders())
	r.Use(middleware.BodyLimit(httpCfg.MaxBodyBytes))
	r.Use(middleware.RequestLogger(deps.Log))
	r.Use(middleware.Recoverer(deps.Log))
	r.Use(middleware.Timeout(time.Duration(httpCfg.RequestTimeout)))

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusFound)
	})
	r.Handle("/dashboard/*", http.StripPrefix("/dashboard/", dashboardHandler()))

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/oauth/device/code", deviceCodeHandler(deps))
		r.Post("/oauth/device/token", deviceTokenHandler(deps))
		r.Post("/oauth/device/approve", deviceApproveHandler(deps))

		r.Group(func(r chi.Router) {
			r.Use(middleware.Auth(middleware.AuthConfig{
				Auth:          deps.Auth,
				AllowInsecure: httpCfg.AllowInsecureAPI,
			}))
			r.Get("/tunnels/{tunnelID}/events", listEvents(deps))
			r.Get("/events/{eventID}", getEvent(deps))
			r.Post("/events/{eventID}/replay", replayEvent(deps))
		})
	})

	return r
}

func listEvents(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tunnelID := chi.URLParam(r, "tunnelID")
		subject, _ := middleware.SubjectFromContext(r.Context())

		limit := 50
		if raw := r.URL.Query().Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n <= 0 || n > 200 {
				fault.WriteHTTPError(r.Context(), w, fault.ErrBadRequest.New("limit: expected 1..200").WithOp("httpapi.list_events"))
				return
			}
			limit = n
		}

		events, err := deps.Events.ListEvents(r.Context(), subject, tunnelID, limit)
		if err != nil {
			fault.WriteHTTPError(r.Context(), w, err)
			return
		}

		out := make([]eventJSON, 0, len(events))
		for _, item := range events {
			out = append(out, toEventJSON(item.Event, "", "", item.LatestDelivery, nil))
		}

		writeJSON(w, map[string]any{
			"events": out,
			"count":  len(out),
		})
	}
}

func getEvent(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		eventID := chi.URLParam(r, "eventID")
		subject, _ := middleware.SubjectFromContext(r.Context())

		view, err := deps.Events.GetEvent(r.Context(), subject, eventID)
		if err != nil {
			fault.WriteHTTPError(r.Context(), w, err)
			return
		}

		encoding := ""
		encoded := ""
		if len(view.Payload) > 0 {
			encoding = "base64"
			encoded = base64.StdEncoding.EncodeToString(view.Payload)
		}

		writeJSON(w, toEventJSON(view.Event, encoded, encoding, nil, view.Deliveries))
	}
}

func replayEvent(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if deps.Replay == nil {
			fault.WriteHTTPError(r.Context(), w, fault.ErrInternal.New("replay is not configured").WithOp("httpapi.replay"))
			return
		}

		eventID := chi.URLParam(r, "eventID")
		subject, _ := middleware.SubjectFromContext(r.Context())

		var body struct {
			Target string `json:"target"`
		}
		if r.Body != nil && r.ContentLength != 0 {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				fault.WriteHTTPError(r.Context(), w, fault.ErrBadRequest.Wrap(err, "invalid JSON").WithOp("httpapi.replay"))
				return
			}
		}

		result, err := deps.Replay.Replay(r.Context(), subject, eventID, body.Target)
		if err != nil {
			fault.WriteHTTPError(r.Context(), w, err)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		writeJSON(w, result)
	}
}

type deliveryJSON struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	Status     string    `json:"status"`
	StatusCode *int32    `json:"status_code,omitempty"`
	Error      string    `json:"error,omitempty"`
	LatencyMS  *int32    `json:"latency_ms,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

type eventJSON struct {
	ID              string            `json:"id"`
	TunnelID        string            `json:"tunnel_id"`
	Topic           string            `json:"topic"`
	Partition       *int32            `json:"partition,omitempty"`
	BrokerOffset    *int64            `json:"broker_offset,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
	PayloadSize     int32             `json:"payload_size"`
	PayloadRef      string            `json:"payload_ref,omitempty"`
	Payload         string            `json:"payload,omitempty"`
	PayloadEncoding string            `json:"payload_encoding,omitempty"`
	IsReplay        bool              `json:"is_replay"`
	BrokerTS        *time.Time        `json:"broker_ts,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	LatestDelivery  *deliveryJSON     `json:"latest_delivery,omitempty"`
	Deliveries      []deliveryJSON    `json:"deliveries,omitempty"`
}

func toDeliveryJSON(rec *models.Delivery) *deliveryJSON {
	if rec == nil {
		return nil
	}
	return &deliveryJSON{
		ID:         rec.ID,
		Kind:       string(rec.Kind),
		Status:     string(rec.Status),
		StatusCode: rec.StatusCode,
		Error:      rec.Error,
		LatencyMS:  rec.LatencyMS,
		CreatedAt:  rec.CreatedAt,
	}
}

func toEventJSON(ev *models.Event, payload, encoding string, latest *models.Delivery, all []*models.Delivery) eventJSON {
	out := eventJSON{
		ID:              ev.ID,
		TunnelID:        ev.TunnelID,
		Topic:           ev.Topic,
		Partition:       ev.Partition,
		BrokerOffset:    ev.BrokerOffset,
		Headers:         ev.Headers,
		PayloadSize:     ev.PayloadSize,
		PayloadRef:      ev.PayloadRef,
		Payload:         payload,
		PayloadEncoding: encoding,
		IsReplay:        ev.IsReplay,
		BrokerTS:        ev.BrokerTS,
		CreatedAt:       ev.CreatedAt,
		LatestDelivery:  toDeliveryJSON(latest),
	}
	if len(all) > 0 {
		out.Deliveries = make([]deliveryJSON, 0, len(all))
		for _, rec := range all {
			if d := toDeliveryJSON(rec); d != nil {
				out.Deliveries = append(out.Deliveries, *d)
			}
		}
	}
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
