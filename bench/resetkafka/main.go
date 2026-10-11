// Command resetkafka deletes the outbox topic and the ledger consumer group so a benchmark run
// starts from an empty broker (stale members/offsets from earlier runs would otherwise skew it).
package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(os.Getenv("KAFKA_BOOTSTRAP"), ",")...))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()
	a := kadm.NewClient(cl)
	ctx := context.Background()
	if _, err := a.DeleteTopics(ctx, "authorizations"); err != nil {
		log.Println("delete topic:", err)
	}
	if _, err := a.DeleteGroups(ctx, "authgate-ledger"); err != nil {
		log.Println("delete group:", err)
	}
}
