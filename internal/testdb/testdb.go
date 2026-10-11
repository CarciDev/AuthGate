// Package testdb gives each test package its own Postgres schema so packages can run in parallel.
package testdb

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

// DSN returns a DSN whose search_path is a fresh-per-package schema named name.
// The base server comes from TEST_DATABASE_URL (default: local Postgres from docker-compose).
func DSN(t testing.TB, name string) string {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		base = os.Getenv("DATABASE_URL")
	}
	if base == "" {
		base = "postgres://postgres:postgres@localhost:5432/authgate_test"
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("postgres required (docker compose up postgres, or set TEST_DATABASE_URL): %v", err)
	}
	defer c.Close(ctx)
	if _, err := c.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+name); err != nil {
		t.Fatal(err)
	}
	sep := "?"
	for _, ch := range base {
		if ch == '?' {
			sep = "&"
		}
	}
	return base + sep + "search_path=" + name
}
