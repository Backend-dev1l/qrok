package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"qrok/pkg/fault"
)

func deviceCodeHandler(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ProjectID string `json:"project_id"`
		}
		if r.Body != nil && r.ContentLength != 0 {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				fault.WriteHTTPError(r.Context(), w, fault.ErrBadRequest.Wrap(err, "invalid JSON").WithOp("httpapi.device_code"))
				return
			}
		}

		verificationURI := strings.TrimRight(publicAPIBase(r), "/") + "/dashboard/device"
		start, err := deps.Device.Start(r.Context(), body.ProjectID, verificationURI)
		if err != nil {
			fault.WriteHTTPError(r.Context(), w, err)
			return
		}

		w.WriteHeader(http.StatusOK)
		writeJSON(w, start)
	}
}

func deviceTokenHandler(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			DeviceCode string `json:"device_code"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			fault.WriteHTTPError(r.Context(), w, fault.ErrBadRequest.Wrap(err, "invalid JSON").WithOp("httpapi.device_token"))
			return
		}

		result, err := deps.Device.Poll(r.Context(), body.DeviceCode)
		if err != nil {
			fault.WriteHTTPError(r.Context(), w, err)
			return
		}

		if result.Error != "" {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, result)
			return
		}

		writeJSON(w, result)
	}
}

func deviceApproveHandler(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			UserCode  string `json:"user_code"`
			ProjectID string `json:"project_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			fault.WriteHTTPError(r.Context(), w, fault.ErrBadRequest.Wrap(err, "invalid JSON").WithOp("httpapi.device_approve"))
			return
		}

		if err := deps.Device.Approve(r.Context(), body.UserCode, body.ProjectID); err != nil {
			fault.WriteHTTPError(r.Context(), w, err)
			return
		}

		writeJSON(w, map[string]string{"status": "approved"})
	}
}

func publicAPIBase(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	return scheme + "://" + r.Host
}
