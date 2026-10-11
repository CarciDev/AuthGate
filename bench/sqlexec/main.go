// Command sqlexec runs each SQL argument against the DSN given as the first argument
// (lets the bench scripts reset databases without needing a psql binary).
package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()
	c, err := pgx.Connect(ctx, os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close(ctx)
	for _, q := range os.Args[2:] {
		if _, err := c.Exec(ctx, q); err != nil {
			log.Fatal(err)
		}
	}
}
