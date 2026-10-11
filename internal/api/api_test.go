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

// TestScreenDeclinesWithoutHold checks that a rule decline is stored, replays identically,
// and leaves the balance untouched.
func TestScreenDeclinesWithoutHold(t *testing.T) {
	ctx := context.Background()
	s, err := store.New(ctx, testdb.DSN(t, "api_test"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Pool.Close()
	s.Pool.Exec(ctx, `TRUNCATE outbox, authorizations, accounts`)
	s.SeedAccount(ctx, "acc", 1000)
	calls := 0
	s.Screen = func(context.Context, store.Request) string { calls++; return "AMOUNT_CAP" }
	h := Handler(s)
	body := `{"account_id":"acc","card_id":"c","amount_cents":100,"merchant":"m"}`
	var first string
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest("POST", "/authorize", strings.NewReader(body))
		r.Header.Set("Idempotency-Key", "s1")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if !strings.Contains(w.Body.String(), `"reason_code":"AMOUNT_CAP"`) || !strings.Contains(w.Body.String(), `"decision":"decline"`) {
			t.Fatalf("%s", w.Body)
		}
		if i == 0 {
			first = w.Body.String()
		} else if w.Body.String() != first {
			t.Fatalf("replay differs")
		}
	}
	if calls != 1 {
		t.Fatalf("screen ran %d times; replays must not re-screen", calls)
	}
	var avail int64
	s.Pool.QueryRow(ctx, `SELECT available_cents FROM accounts WHERE id='acc'`).Scan(&avail)
	if avail != 1000 {
		t.Fatalf("balance changed: %d", avail)
	}
}
