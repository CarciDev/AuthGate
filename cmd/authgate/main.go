// Command authgate runs the card authorization HTTP service.
package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"authgate/internal/api"
	"authgate/internal/store"
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
