// Package relay drains the transactional outbox into Kafka (Redpanda).
//
// Delivery is at-least-once: a batch is published first and marked sent afterwards, all inside
// one Postgres transaction that holds FOR UPDATE SKIP LOCKED row locks. If the relay dies after
// publishing but before the commit, the rows stay unsent and are published again; the consumer
// removes those duplicates by event_id.
package relay

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Event is one claimed outbox row.
type Event struct {
	ID      string
	Topic   string
	Payload []byte
}

// Publisher synchronously publishes events; it returns nil only once all are durably acked.
type Publisher interface {
	Publish(ctx context.Context, evs []Event) error
}

// KafkaPublisher publishes with franz-go. The client is idempotent (acks=all, retries
// without reordering or broker-side duplicates within a producer session).
type KafkaPublisher struct{ Client *kgo.Client }

// NewKafkaPublisher connects to the given brokers with idempotent producer settings.
func NewKafkaPublisher(brokers []string, opts ...kgo.Opt) (*KafkaPublisher, error) {
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()), // required for idempotence (and the franz-go default)
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordRetries(1 << 30),
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.AllowAutoTopicCreation(),
	}, opts...)
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{Client: c}, nil
}

// Publish implements Publisher. The event ID is both the record key and a header.
func (p *KafkaPublisher) Publish(ctx context.Context, evs []Event) error {
	recs := make([]*kgo.Record, len(evs))
	for i, e := range evs {
		recs[i] = &kgo.Record{Topic: e.Topic, Key: []byte(e.ID), Value: e.Payload,
			Headers: []kgo.RecordHeader{{Key: "event_id", Value: []byte(e.ID)}}}
	}
	return p.Client.ProduceSync(ctx, recs...).FirstErr()
}

// Close closes the underlying client.
func (p *KafkaPublisher) Close() { p.Client.Close() }

// Relay is a pool of workers that claim and publish outbox rows.
type Relay struct {
	Pool      *pgxpool.Pool
	Pub       Publisher
	Workers   int           // goroutines polling concurrently (default 4)
	BatchSize int           // rows claimed per transaction (default 200)
	Poll      time.Duration // idle sleep when the outbox is empty (default 50ms)
}

// Run starts the workers and blocks until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	n := r.Workers
	if n == 0 {
		n = 4
	}
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() { r.worker(ctx); done <- struct{}{} }()
	}
	for i := 0; i < n; i++ {
		<-done
	}
}

func (r *Relay) worker(ctx context.Context) {
	poll := r.Poll
	if poll == 0 {
		poll = 50 * time.Millisecond
	}
	for ctx.Err() == nil {
		n, err := r.Step(ctx)
		if err != nil || n == 0 { // back off on errors (broker down) and on an empty outbox
			select {
			case <-ctx.Done():
			case <-time.After(poll):
			}
		}
	}
}

// Step claims one batch, publishes it and marks it sent, returning how many rows were sent.
// Rows locked by other workers are skipped, so workers never block or double-claim.
func (r *Relay) Step(ctx context.Context) (int, error) {
	batch := r.BatchSize
	if batch == 0 {
		batch = 200
	}
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	rows, err := tx.Query(ctx, `SELECT event_id::text, topic, payload::text FROM outbox
		WHERE sent_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return 0, err
	}
	var evs []Event
	var ids []string
	for rows.Next() {
		var e Event
		var payload string
		if err := rows.Scan(&e.ID, &e.Topic, &payload); err != nil {
			rows.Close()
			return 0, err
		}
		e.Payload = []byte(payload)
		evs = append(evs, e)
		ids = append(ids, e.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(evs) == 0 {
		return 0, nil
	}
	if err := r.Pub.Publish(ctx, evs); err != nil {
		return 0, err // rollback: rows stay unsent and are retried
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE event_id = ANY($1::uuid[])`, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return 0, err
	}
	return len(evs), nil
}
