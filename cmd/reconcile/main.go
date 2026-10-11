// Command reconcile compares the authorizations table with the consumer ledger and writes the
// chaos report. lost = authorizations with no ledger row; duplicates = event_ids that appear more
// than once in the ledger (must be 0: the ledger PK dedupes) ; redeliveries = extra Kafka
// deliveries the ledger absorbed.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	out := flag.String("out", "results/chaos.json", "report path")
	prefix := flag.String("prefix", "", "only count idempotency keys with this prefix (the run id)")
	wait := flag.Duration("wait", 10*time.Minute, "max time to wait for the ledger to catch up")
	flag.Parse()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close(ctx)

	q := func(sql string, args ...any) (n int64) {
		if err := c.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
			log.Fatal(err)
		}
		return
	}
	like := *prefix + "%"
	lost := func() int64 {
		return q(`SELECT count(*) FROM authorizations a WHERE a.idempotency_key LIKE $1
			AND NOT EXISTS (SELECT 1 FROM outbox o JOIN ledger l ON l.event_id = o.event_id WHERE o.aggregate_id = a.id)`, like)
	}
	deadline := time.Now().Add(*wait)
	for lost() > 0 && time.Now().Before(deadline) { // let the relay/consumer drain after chaos stops
		time.Sleep(2 * time.Second)
	}
	rep := map[string]any{
		"authorizations": q(`SELECT count(*) FROM authorizations WHERE idempotency_key LIKE $1`, like),
		"outbox_rows":    q(`SELECT count(*) FROM outbox o JOIN authorizations a ON a.id = o.aggregate_id WHERE a.idempotency_key LIKE $1`, like),
		"outbox_unsent":  q(`SELECT count(*) FROM outbox o JOIN authorizations a ON a.id = o.aggregate_id WHERE o.sent_at IS NULL AND a.idempotency_key LIKE $1`, like),
		"ledger_rows":    q(`SELECT count(*) FROM ledger l JOIN authorizations a ON a.id = l.aggregate_id WHERE a.idempotency_key LIKE $1`, like),
		"lost_events":    lost(),
		"duplicates_in_ledger": q(`SELECT count(*) FROM (SELECT l.event_id FROM ledger l JOIN authorizations a ON a.id = l.aggregate_id
			WHERE a.idempotency_key LIKE $1 GROUP BY l.event_id HAVING count(*) > 1) d`, like) +
			q(`SELECT count(*) FROM (SELECT l.aggregate_id FROM ledger l JOIN authorizations a ON a.id = l.aggregate_id
			WHERE a.idempotency_key LIKE $1 GROUP BY l.aggregate_id HAVING count(*) > 1) d`, like),
		"redelivered_events":        q(`SELECT count(*) FROM ledger l JOIN authorizations a ON a.id = l.aggregate_id WHERE l.deliveries > 1 AND a.idempotency_key LIKE $1`, like),
		"extra_deliveries_absorbed": q(`SELECT coalesce(sum(l.deliveries - 1),0) FROM ledger l JOIN authorizations a ON a.id = l.aggregate_id WHERE a.idempotency_key LIKE $1`, like),
		"negative_balances":         q(`SELECT count(*) FROM accounts WHERE available_cents < 0`),
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	// Merge into the harness-provided partial report if the output exists.
	if old, err := os.ReadFile(*out); err == nil {
		var m map[string]any
		if json.Unmarshal(old, &m) == nil {
			for k, v := range rep {
				m[k] = v
			}
			b, _ = json.MarshalIndent(m, "", "  ")
		}
	}
	if err := os.WriteFile(*out, b, 0o644); err != nil {
		log.Fatal(err)
	}
}
