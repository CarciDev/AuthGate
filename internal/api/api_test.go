package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"authgate/internal/store"
	"authgate/internal/testdb"
)

func TestAuthorizeHTTP(t *testing.T) {
	s, err := store.New(context.Background(), testdb.DSN(t, "api_test"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	s.Pool.Exec(context.Background(), `TRUNCATE outbox, authorizations, accounts`)
	s.SeedAccount(context.Background(), "acc", 500)
	h := Handler(s)
	do := func(key, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/authorize", strings.NewReader(body))
		if key != "" {
			r.Header.Set("Idempotency-Key", key)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	ok := `{"account_id":"acc","card_id":"c","amount_cents":400,"merchant":"m"}`
	if w := do("", ok); w.Code != http.StatusBadRequest {
		t.Fatalf("missing key: %d", w.Code)
	}
	w := do("k", ok)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"reason_code":"APPROVED"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w2 := do("k", ok); w2.Body.String() != w.Body.String() {
		t.Fatalf("replay differs: %s vs %s", w2.Body, w.Body)
	}
	if w := do("k2", ok); !strings.Contains(w.Body.String(), "INSUFFICIENT_FUNDS") {
		t.Fatalf("%s", w.Body)
	}
	if w := do("k", strings.Replace(ok, "400", "5", 1)); w.Code != 422 {
		t.Fatalf("key reuse: %d", w.Code)
	}
}
