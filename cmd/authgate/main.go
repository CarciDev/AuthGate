// Command authgate runs the card authorization HTTP service.
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"authgate/internal/api"
	"authgate/internal/rules"
	"authgate/internal/scorer"
	"authgate/internal/store"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/authgate"
	}
	s, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	if url := os.Getenv("REDIS_URL"); url != "" {
		opt, err := redis.ParseURL(url)
		if err != nil {
			log.Fatal(err)
		}
		scr := &rules.Screener{Redis: redis.NewClient(opt), Cfg: rules.ConfigFromEnv(os.Getenv)}
		if path := os.Getenv("MODEL_PATH"); path != "" {
			if m, err := scorer.Load(path, scorer.FeatureNames); err != nil {
				log.Printf("fraud model disabled: %v", err)
			} else {
				scr.Scorer = m
			}
		}
		s.Screen = func(ctx context.Context, r store.Request) string {
			return scr.Screen(ctx, r.CardID, r.AmountCents, r.Features)
		}
	}
	if os.Getenv("SEED_DEMO") == "1" { // demo accounts for `docker compose up` and the acceptance test
		for _, id := range []string{"acc-1", "acc-2"} {
			if err := s.SeedAccount(ctx, id, 100_000_000); err != nil {
				log.Fatal(err)
			}
		}
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("authgate listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, api.Handler(s)))
}
