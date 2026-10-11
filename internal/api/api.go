// Package api exposes the HTTP surface of AuthGate.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"authgate/internal/store"
)

// Handler returns the HTTP mux serving POST /authorize and GET /healthz.
func Handler(s *store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /authorize", func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Idempotency-Key header required"})
			return
		}
		var req store.Request
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil || req.AccountID == "" || req.CardID == "" || req.AmountCents <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "account_id, card_id and positive amount_cents required"})
			return
		}
		d, err := s.Authorize(r.Context(), key, req)
		switch {
		case errors.Is(err, store.ErrKeyReuse):
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		case err != nil:
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		default:
			writeJSON(w, http.StatusOK, d)
		}
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
