// Command authgate runs the card authorization HTTP service.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"authgate/internal/api"
	"authgate/internal/consumer"
	"authgate/internal/kafkaopt"
	"authgate/internal/relay"
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
	if n, _ := strconv.Atoi(os.Getenv("SEED_ACCOUNTS")); n > 0 { // bulk accounts for the load/chaos harness
		for i := 0; i < n; i++ {
			if err := s.SeedAccount(ctx, "acc-"+strconv.Itoa(i), 1_000_000_000); err != nil {
				log.Fatal(err)
			}
		}
	}
	if os.Getenv("SEED_DEMO") == "1" { // demo accounts for `docker compose up` and the acceptance test
		for _, id := range []string{"acc-1", "acc-2"} {
			if err := s.SeedAccount(ctx, id, 100_000_000); err != nil {
				log.Fatal(err)
			}
		}
	}
	// RUN_RELAY / RUN_CONSUMER let the relay and ledger consumer run in-process or as separate
	// replicas (the chaos harness kills and restarts the relay).
	if bootstrap := os.Getenv("KAFKA_BOOTSTRAP"); bootstrap != "" {
		brokers := strings.Split(bootstrap, ",")
		if os.Getenv("RUN_RELAY") == "1" {
			pub, err := relay.NewKafkaPublisher(brokers, kafkaopt.FromEnv()...)
			if err != nil {
				log.Fatal(err)
			}
			go (&relay.Relay{Pool: s.Pool, Pub: pub}).Run(ctx)
		}
		if os.Getenv("RUN_CONSUMER") == "1" {
			go func() {
				log.Println("consumer stopped:", consumer.Run(ctx, &consumer.Ledger{Pool: s.Pool}, brokers, "authgate-ledger", store.OutboxTopic, kafkaopt.FromEnv()...))
			}()
		}
	}
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("authgate listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, api.Handler(s)))
}
