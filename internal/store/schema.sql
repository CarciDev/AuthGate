CREATE TABLE IF NOT EXISTS accounts (
    id              TEXT PRIMARY KEY,
    available_cents BIGINT NOT NULL CHECK (available_cents >= 0),
    held_cents      BIGINT NOT NULL DEFAULT 0 CHECK (held_cents >= 0)
);

CREATE TABLE IF NOT EXISTS authorizations (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash    TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    card_id         TEXT NOT NULL,
    merchant        TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL CHECK (amount_cents > 0),
    decision        TEXT NOT NULL CHECK (decision IN ('approve', 'decline')),
    reason_code     TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    event_id     UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES authorizations (id),
    topic        TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unsent_idx ON outbox (created_at) WHERE sent_at IS NULL;

-- Consumer-side ledger: one row per event_id, however many times Kafka delivered it.
CREATE TABLE IF NOT EXISTS ledger (
    event_id       UUID PRIMARY KEY,
    aggregate_id   UUID NOT NULL,
    payload        JSONB NOT NULL,
    deliveries     INT NOT NULL DEFAULT 1,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
