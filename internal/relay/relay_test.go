package relay

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"authgate/internal/consumer"
	"authgate/internal/store"
	"authgate/internal/testdb"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.New(context.Background(), testdb.DSN(t, "relay_test"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Pool.Close)
	if _, err := s.Pool.Exec(context.Background(), `TRUNCATE ledger, outbox, authorizations, accounts`); err != nil {
		t.Fatal(err)
	}
	return s
}

// seed creates n authorizations (and so n outbox rows).
func seed(t *testing.T, s *store.Store, n int) {
	t.Helper()
	ctx := context.Background()
	s.SeedAccount(ctx, "a", 1_000_000_000)
	for i := 0; i < n; i++ {
		if _, err := s.Authorize(ctx, fmt.Sprintf("k%d", i), store.Request{AccountID: "a", CardID: "c", AmountCents: 1, Merchant: "m"}); err != nil {
			t.Fatal(err)
		}
	}
}

func unsent(t *testing.T, s *store.Store) int {
	var n int
	if err := s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox WHERE sent_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// fakePub records published event IDs; fail makes Publish error.
type fakePub struct {
	mu   sync.Mutex
	got  map[string]int
	fail bool
}

func (f *fakePub) Publish(_ context.Context, evs []Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("broker down")
	}
	for _, e := range evs {
		f.got[e.ID]++
	}
	return nil
}

func TestWorkersNeverDoubleClaim(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	seed(t, s, 300)
	pub := &fakePub{got: map[string]int{}}
	r := &Relay{Pool: s.Pool, Pub: pub, Workers: 8, BatchSize: 7, Poll: 5 * time.Millisecond}
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { r.Run(cctx); close(done) }()
	for i := 0; i < 400 && unsent(t, s) > 0; i++ {
		time.Sleep(25 * time.Millisecond)
	}
	cancel()
	<-done
	if len(pub.got) != 300 {
		t.Fatalf("published %d distinct events, want 300", len(pub.got))
	}
	for id, n := range pub.got {
		if n != 1 {
			t.Fatalf("event %s published %d times with no failures", id, n)
		}
	}
}

func TestPublishFailureKeepsRowsUnsent(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	seed(t, s, 10)
	pub := &fakePub{got: map[string]int{}, fail: true}
	r := &Relay{Pool: s.Pool, Pub: pub}
	if _, err := r.Step(ctx); err == nil {
		t.Fatal("expected error")
	}
	if unsent(t, s) != 10 {
		t.Fatal("rows must stay unsent when publish fails")
	}
	pub.fail = false
	if n, err := r.Step(ctx); err != nil || n != 10 || unsent(t, s) != 0 {
		t.Fatalf("retry: n=%d err=%v unsent=%d", n, err, unsent(t, s))
	}
}

func TestLedgerDedupesByEventID(t *testing.T) {
	s, ctx := testStore(t), context.Background()
	l := &consumer.Ledger{Pool: s.Pool}
	ev := uuid.NewString()
	payload := []byte(`{"authorization_id":"` + uuid.NewString() + `"}`)
	for i := 0; i < 3; i++ {
		isNew, err := l.Record(ctx, ev, payload)
		if err != nil || isNew != (i == 0) {
			t.Fatalf("delivery %d: new=%v err=%v", i, isNew, err)
		}
	}
	var rows, deliveries int
	s.Pool.QueryRow(ctx, `SELECT count(*), max(deliveries) FROM ledger`).Scan(&rows, &deliveries)
	if rows != 1 || deliveries != 3 {
		t.Fatalf("rows=%d deliveries=%d", rows, deliveries)
	}
}

// TestRedpandaEndToEnd publishes through real Redpanda, replays the same rows (simulating a relay
// that died after publishing but before marking sent) and checks the ledger holds each event once.
func TestRedpandaEndToEnd(t *testing.T) {
	bootstrap := os.Getenv("KAFKA_BOOTSTRAP")
	if bootstrap == "" {
		t.Skip("KAFKA_BOOTSTRAP not set")
	}
	brokers := strings.Split(bootstrap, ",")
	s, ctx := testStore(t), context.Background()
	const n = 50
	seed(t, s, n)
	topic := "authorizations-test-" + uuid.NewString()[:8]
	s.Pool.Exec(ctx, `UPDATE outbox SET topic = $1`, topic)

	pub, err := NewKafkaPublisher(brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	r := &Relay{Pool: s.Pool, Pub: pub, Workers: 3, BatchSize: 20}
	for sent := 0; sent < n; {
		k, err := r.Step(ctx)
		if err != nil {
			t.Fatal(err)
		}
		sent += k
	}
	// Crash simulation: un-mark everything, so the relay publishes all events a second time.
	s.Pool.Exec(ctx, `UPDATE outbox SET sent_at = NULL`)
	for sent := 0; sent < n; {
		k, err := r.Step(ctx)
		if err != nil {
			t.Fatal(err)
		}
		sent += k
	}

	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	go consumer.Run(cctx, &consumer.Ledger{Pool: s.Pool}, brokers, "g-"+topic, topic, kgo.FetchMaxWait(time.Second))
	var rows, dups int
	for i := 0; i < 120; i++ {
		s.Pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE deliveries > 1) FROM ledger`).Scan(&rows, &dups)
		if rows == n && dups == n {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if rows != n || dups != n {
		t.Fatalf("ledger rows=%d (want %d), redelivered=%d (want %d)", rows, n, dups, n)
	}
	var lost int
	s.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox o LEFT JOIN ledger l USING (event_id) WHERE l.event_id IS NULL`).Scan(&lost)
	if lost != 0 {
		t.Fatalf("%d events lost", lost)
	}
}
