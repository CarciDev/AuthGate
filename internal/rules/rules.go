// Package rules screens an authorization before money moves: an amount cap, Redis velocity
// counters per card (1 minute and 1 hour windows) and the in-process fraud model.
package rules

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Decline reason codes produced by the screen.
const (
	ReasonAmountCap  = "AMOUNT_CAP"
	ReasonVelocity1m = "VELOCITY_1M"
	ReasonVelocity1h = "VELOCITY_1H"
	ReasonFraudScore = "FRAUD_SCORE"
)

// Scorer returns a fraud probability for named features (implemented by scorer.Scorer).
type Scorer interface {
	ScoreMap(map[string]float64) float64
}

// Config holds the rule limits.
type Config struct {
	AmountCapCents int64   // decline any single amount above this
	Max1m, Max1h   int64   // max attempts per card per window
	FraudThreshold float64 // decline when model score >= threshold (0 disables the model rule)
}

// DefaultConfig is used when no overrides are given.
var DefaultConfig = Config{AmountCapCents: 500_000, Max1m: 5, Max1h: 30, FraudThreshold: 0.9685}

// Screener evaluates the rules. Scorer may be nil (model rule off).
type Screener struct {
	Redis  redis.Cmdable
	Scorer Scorer
	Cfg    Config
}

// Screen returns "" when the request passes, else the decline reason code.
// Order: amount cap (cheap, no state), velocity, model. Capped requests do not count toward
// velocity. A Redis failure fails open (velocity is skipped, logged): the balance check in
// Postgres is still authoritative, and an outage must not decline every card.
func (s *Screener) Screen(ctx context.Context, cardID string, amountCents int64, features map[string]float64) string {
	if amountCents > s.Cfg.AmountCapCents {
		return ReasonAmountCap
	}
	if r := s.velocity(ctx, cardID); r != "" {
		return r
	}
	if s.Scorer != nil && s.Cfg.FraudThreshold > 0 && len(features) > 0 &&
		s.Scorer.ScoreMap(features) >= s.Cfg.FraudThreshold {
		return ReasonFraudScore
	}
	return ""
}

// velocity does INCR + EXPIRE (only when the key is new, so the window is fixed from the
// first attempt) for both windows in one round trip.
func (s *Screener) velocity(ctx context.Context, card string) string {
	k1, k2 := "vel:1m:"+card, "vel:1h:"+card
	pipe := s.Redis.Pipeline()
	c1, c2 := pipe.Incr(ctx, k1), pipe.Incr(ctx, k2)
	pipe.ExpireNX(ctx, k1, time.Minute)
	pipe.ExpireNX(ctx, k2, time.Hour)
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("velocity check skipped (redis): %v", err)
		return ""
	}
	switch {
	case c1.Val() > s.Cfg.Max1m:
		return ReasonVelocity1m
	case c2.Val() > s.Cfg.Max1h:
		return ReasonVelocity1h
	}
	return ""
}

// ConfigFromEnv overrides DefaultConfig from AMOUNT_CAP_CENTS, VELOCITY_1M, VELOCITY_1H, FRAUD_THRESHOLD.
func ConfigFromEnv(get func(string) string) Config {
	c := DefaultConfig
	if v, err := strconv.ParseInt(get("AMOUNT_CAP_CENTS"), 10, 64); err == nil {
		c.AmountCapCents = v
	}
	if v, err := strconv.ParseInt(get("VELOCITY_1M"), 10, 64); err == nil {
		c.Max1m = v
	}
	if v, err := strconv.ParseInt(get("VELOCITY_1H"), 10, 64); err == nil {
		c.Max1h = v
	}
	if v, err := strconv.ParseFloat(get("FRAUD_THRESHOLD"), 64); err == nil {
		c.FraudThreshold = v
	}
	return c
}
