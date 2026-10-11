// Package consumer reads authorization events from Kafka and records each event_id exactly once
// in the ledger table, turning the relay's at-least-once delivery into effectively-once.
package consumer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Ledger writes deduplicated events to Postgres.
type Ledger struct{ Pool *pgxpool.Pool }

// Record stores the event; a repeat delivery only bumps the deliveries counter.
// It reports whether the event was new.
func (l *Ledger) Record(ctx context.Context, eventID string, payload []byte) (bool, error) {
	var agg struct {
		ID string `json:"authorization_id"`
	}
	if err := json.Unmarshal(payload, &agg); err != nil {
		return false, err
	}
	var inserted bool
	err := l.Pool.QueryRow(ctx, `INSERT INTO ledger (event_id, aggregate_id, payload)
		VALUES ($1,$2,$3) ON CONFLICT (event_id) DO UPDATE SET deliveries = ledger.deliveries + 1
		RETURNING (xmax = 0)`, eventID, agg.ID, payload).Scan(&inserted)
	return inserted, err
}

// RecordBatch stores many events in one round trip and one transaction (same per-event semantics
// as Record). It is all-or-nothing: on error nothing is written and the caller retries.
func (l *Ledger) RecordBatch(ctx context.Context, ids []string, payloads [][]byte) error {
	b := &pgx.Batch{}
	for i, id := range ids {
		var agg struct {
			ID string `json:"authorization_id"`
		}
		if err := json.Unmarshal(payloads[i], &agg); err != nil {
			return err
		}
		b.Queue(`INSERT INTO ledger (event_id, aggregate_id, payload) VALUES ($1,$2,$3)
			ON CONFLICT (event_id) DO UPDATE SET deliveries = ledger.deliveries + 1`, id, agg.ID, payloads[i])
	}
	tx, err := l.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Run consumes topic as a group member until ctx is cancelled. Offsets are committed only after
// the ledger write, so a crash re-delivers (and the ledger dedupes) instead of losing events.
func Run(ctx context.Context, l *Ledger, brokers []string, group, topic string, opts ...kgo.Opt) error {
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		// One batched write per poll. Retry in place: skipping a failed batch and committing later
		// offsets would lose events. Duplicates inside or across batches collapse on the PK.
		var ids []string
		var payloads [][]byte
		fetches.EachRecord(func(r *kgo.Record) { ids = append(ids, string(r.Key)); payloads = append(payloads, r.Value) })
		for len(ids) > 0 && ctx.Err() == nil {
			if err := l.RecordBatch(ctx, ids, payloads); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if ctx.Err() == nil && fetches.NumRecords() > 0 {
			_ = cl.CommitUncommittedOffsets(ctx) // on failure the next commit covers it
		}
	}
	return ctx.Err()
}
