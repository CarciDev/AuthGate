package store

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"authgate/internal/testdb"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := New(context.Background(), testdb.DSN(t, "store_test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Pool.Close)
	if _, err := s.Pool.Exec(context.Background(), `TRUNCATE outbox, authorizations, accounts`); err != nil {
		t.Fatal(err)
	}
	return s
}

func count(t *testing.T, s *Store, q string, args ...any) int {
	t.Helper()
	var n int
	if err := s.Pool.QueryRow(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestIdempotentReplay(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	s.SeedAccount(ctx, "a", 10_000)
	req := Request{AccountID: "a", CardID: "c", AmountCents: 4_000, Merchant: "m"}
	first, err := s.Authorize(ctx, "k1", req)
	if err != nil || first.Decision != "approve" {
		t.Fatalf("first: %+v %v", first, err)
	}
	for i := 0; i < 5; i++ {
		again, err := s.Authorize(ctx, "k1", req)
		if err != nil || again != first {
			t.Fatalf("replay %d: %+v %v, want %+v", i, again, err, first)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM authorizations WHERE idempotency_key='k1'`); n != 1 {
		t.Fatalf("authorization rows = %d, want 1", n)
	}
	if n := count(t, s, `SELECT count(*) FROM outbox`); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
	if n := count(t, s, `SELECT held_cents FROM accounts WHERE id='a'`); n != 4_000 {
		t.Fatalf("held = %d, want 4000 (hold placed once)", n)
	}
	if _, err := s.Authorize(ctx, "k1", Request{AccountID: "a", CardID: "c", AmountCents: 1, Merchant: "m"}); err != ErrKeyReuse {
		t.Fatalf("different body, same key: err = %v, want ErrKeyReuse", err)
	}
}

func TestIdempotentReplayConcurrent(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	s.SeedAccount(ctx, "a", 10_000)
	req := Request{AccountID: "a", CardID: "c", AmountCents: 1_000, Merchant: "m"}
	var wg sync.WaitGroup
	ids := make([]string, 20)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := s.Authorize(ctx, "same", req)
			if err != nil {
				t.Error(err)
			}
			ids[i] = d.ID
		}()
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("concurrent same-key calls got different decisions: %v", ids)
		}
	}
	if n := count(t, s, `SELECT count(*) FROM authorizations`); n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
	if n := count(t, s, `SELECT held_cents FROM accounts WHERE id='a'`); n != 1_000 {
		t.Fatalf("held = %d, want 1000", n)
	}
}

func TestDoubleSpend(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	const balance, amount, attempts = 10_000, 1_000, 100
	s.SeedAccount(ctx, "a", balance)
	var wg sync.WaitGroup
	var mu sync.Mutex
	approved := 0
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := s.Authorize(ctx, fmt.Sprintf("k-%d", i), Request{AccountID: "a", CardID: "c", AmountCents: amount, Merchant: "m"})
			if err != nil {
				t.Error(err)
				return
			}
			if d.Decision == "approve" {
				mu.Lock()
				approved++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if approved != balance/amount {
		t.Fatalf("approved = %d, want %d", approved, balance/amount)
	}
	var avail, held int64
	s.Pool.QueryRow(ctx, `SELECT available_cents, held_cents FROM accounts WHERE id='a'`).Scan(&avail, &held)
	if avail != 0 || held != balance {
		t.Fatalf("available=%d held=%d, want 0/%d", avail, held, balance)
	}
	if n := count(t, s, `SELECT count(*) FROM outbox`); n != attempts {
		t.Fatalf("outbox rows = %d, want %d (one per decision)", n, attempts)
	}
}

func TestUnknownAccountDeclined(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	d, err := s.Authorize(ctx, "k", Request{AccountID: "nope", CardID: "c", AmountCents: 1, Merchant: "m"})
	if err != nil || d.Decision != "decline" || d.ReasonCode != ReasonAccountNotFound {
		t.Fatalf("%+v %v", d, err)
	}
}
