// Package store implements the Postgres side of AuthGate: schema, and the single
// transaction that places a balance hold, records the decision and writes the outbox row.
package store

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// Reason codes returned with every decision.
const (
	ReasonApproved          = "APPROVED"
	ReasonInsufficientFunds = "INSUFFICIENT_FUNDS"
	ReasonAccountNotFound   = "ACCOUNT_NOT_FOUND"

	// OutboxTopic is the Kafka topic the relay (milestone 4) publishes decisions to.
	OutboxTopic = "authorizations"
)

// ErrKeyReuse is returned when an Idempotency-Key is replayed with a different request body.
var ErrKeyReuse = errors.New("idempotency key reused with a different request")

// Request is a card authorization request.
type Request struct {
	AccountID   string `json:"account_id"`
	CardID      string `json:"card_id"`
	AmountCents int64  `json:"amount_cents"`
	Merchant    string `json:"merchant"`
}

// Decision is the stored outcome of an authorization.
type Decision struct {
	ID          string `json:"authorization_id"`
	Decision    string `json:"decision"`
	ReasonCode  string `json:"reason_code"`
	AmountCents int64  `json:"amount_cents"`
}

// Store wraps a pgx pool.
type Store struct{ Pool *pgxpool.Pool }

// New connects to Postgres and applies the schema.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{Pool: pool}, nil
}

func hashRequest(r Request) string {
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Authorize decides r exactly once per idempotency key. Replays return the stored decision.
// The account row lock serializes concurrent spends, so available_cents can never go negative.
func (s *Store) Authorize(ctx context.Context, key string, r Request) (Decision, error) {
	for attempt := 0; ; attempt++ {
		d, err := s.authorizeOnce(ctx, key, r)
		var pgErr *pgconn.PgError
		// Unique violation on the key: a concurrent request with the same key won; re-read it.
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && attempt < 3 {
			continue
		}
		return d, err
	}
}

func (s *Store) authorizeOnce(ctx context.Context, key string, r Request) (Decision, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Decision{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	hash := hashRequest(r)
	if d, found, err := lookup(ctx, tx, key, hash); err != nil || found {
		return d, err
	}

	var available int64
	err = tx.QueryRow(ctx, `SELECT available_cents FROM accounts WHERE id = $1 FOR UPDATE`, r.AccountID).Scan(&available)
	// Same-key requests for one account queue on the lock above; re-check now that we hold it.
	if d, found, lerr := lookup(ctx, tx, key, hash); lerr != nil || found {
		return d, lerr
	}

	d := Decision{ID: uuid.NewString(), AmountCents: r.AmountCents}
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		d.Decision, d.ReasonCode = "decline", ReasonAccountNotFound
	case err != nil:
		return Decision{}, err
	case available < r.AmountCents:
		d.Decision, d.ReasonCode = "decline", ReasonInsufficientFunds
	default:
		d.Decision, d.ReasonCode = "approve", ReasonApproved
		if _, err := tx.Exec(ctx, `UPDATE accounts SET available_cents = available_cents - $2,
			held_cents = held_cents + $2 WHERE id = $1`, r.AccountID, r.AmountCents); err != nil {
			return Decision{}, err
		}
	}

	if _, err := tx.Exec(ctx, `INSERT INTO authorizations
		(id, idempotency_key, request_hash, account_id, card_id, merchant, amount_cents, decision, reason_code)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		d.ID, key, hash, r.AccountID, r.CardID, r.Merchant, r.AmountCents, d.Decision, d.ReasonCode); err != nil {
		return Decision{}, err
	}
	payload, _ := json.Marshal(struct {
		Decision
		AccountID string `json:"account_id"`
		CardID    string `json:"card_id"`
		Merchant  string `json:"merchant"`
	}{d, r.AccountID, r.CardID, r.Merchant})
	if _, err := tx.Exec(ctx, `INSERT INTO outbox (event_id, aggregate_id, topic, payload)
		VALUES ($1,$2,$3,$4)`, uuid.NewString(), d.ID, OutboxTopic, payload); err != nil {
		return Decision{}, err
	}
	return d, tx.Commit(ctx)
}

// lookup returns the stored decision for key, or ErrKeyReuse if the request body differs.
func lookup(ctx context.Context, tx pgx.Tx, key, hash string) (Decision, bool, error) {
	var d Decision
	var storedHash string
	err := tx.QueryRow(ctx, `SELECT id, decision, reason_code, amount_cents, request_hash
		FROM authorizations WHERE idempotency_key = $1`, key).
		Scan(&d.ID, &d.Decision, &d.ReasonCode, &d.AmountCents, &storedHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return Decision{}, false, nil
	}
	if err != nil {
		return Decision{}, false, err
	}
	if storedHash != hash {
		return Decision{}, false, ErrKeyReuse
	}
	return d, true, nil
}

// SeedAccount creates an account if absent (used for the demo account and tests).
func (s *Store) SeedAccount(ctx context.Context, id string, cents int64) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO accounts (id, available_cents) VALUES ($1,$2)
		ON CONFLICT (id) DO NOTHING`, id, cents)
	return err
}
