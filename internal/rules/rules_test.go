package rules

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeScorer float64

func (f fakeScorer) ScoreMap(map[string]float64) float64 { return float64(f) }

func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/0"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	c := redis.NewClient(opt)
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis required (set REDIS_URL): %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func card(t *testing.T) string { return fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()) }

func TestAmountCap(t *testing.T) {
	s := &Screener{Redis: newRedis(t), Cfg: DefaultConfig}
	if r := s.Screen(context.Background(), card(t), DefaultConfig.AmountCapCents+1, nil); r != ReasonAmountCap {
		t.Fatalf("got %q", r)
	}
	if r := s.Screen(context.Background(), card(t), DefaultConfig.AmountCapCents, nil); r != "" {
		t.Fatalf("at-cap amount declined: %q", r)
	}
}

func TestVelocityWindowsAndTTL(t *testing.T) {
	rc := newRedis(t)
	s := &Screener{Redis: rc, Cfg: Config{AmountCapCents: 1e9, Max1m: 3, Max1h: 5}}
	ctx, c := context.Background(), card(t)
	for i := 0; i < 3; i++ {
		if r := s.Screen(ctx, c, 100, nil); r != "" {
			t.Fatalf("attempt %d declined: %q", i, r)
		}
	}
	if r := s.Screen(ctx, c, 100, nil); r != ReasonVelocity1m {
		t.Fatalf("4th attempt: %q", r)
	}
	// Simulate the 1-minute window expiring: only the 1-hour counter (now at 4) remains.
	rc.Del(ctx, "vel:1m:"+c)
	if r := s.Screen(ctx, c, 100, nil); r != "" { // 5th hourly attempt, allowed
		t.Fatalf("5th: %q", r)
	}
	if r := s.Screen(ctx, c, 100, nil); r != ReasonVelocity1h { // 6th hourly attempt
		t.Fatalf("6th: %q", r)
	}
	if ttl := rc.TTL(ctx, "vel:1m:"+c).Val(); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("1m ttl = %v", ttl)
	}
	if ttl := rc.TTL(ctx, "vel:1h:"+c).Val(); ttl <= 50*time.Minute || ttl > time.Hour {
		t.Fatalf("1h ttl = %v", ttl)
	}
	// Another card is unaffected.
	if r := s.Screen(ctx, card(t), 100, nil); r != "" {
		t.Fatalf("other card: %q", r)
	}
}

func TestFraudScoreRule(t *testing.T) {
	rc := newRedis(t)
	f := map[string]float64{"price": 1}
	hi := &Screener{Redis: rc, Scorer: fakeScorer(0.99), Cfg: DefaultConfig}
	lo := &Screener{Redis: rc, Scorer: fakeScorer(0.1), Cfg: DefaultConfig}
	if r := hi.Screen(context.Background(), card(t), 100, f); r != ReasonFraudScore {
		t.Fatalf("got %q", r)
	}
	if r := lo.Screen(context.Background(), card(t), 100, f); r != "" {
		t.Fatalf("got %q", r)
	}
	if r := hi.Screen(context.Background(), card(t), 100, nil); r != "" { // no features, model skipped
		t.Fatalf("got %q", r)
	}
}
