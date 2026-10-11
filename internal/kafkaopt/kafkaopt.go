// Package kafkaopt holds client options shared by the relay and the consumer.
package kafkaopt

import (
	"context"
	"net"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FromEnv returns extra franz-go options. If KAFKA_DIAL_ADDR is set, every broker connection is
// dialled to that address instead of the one advertised in cluster metadata. The chaos harness
// points it at cmd/chaosproxy to inject broker outages when the broker process itself cannot be
// restarted (no Docker); it is unset in docker-compose.
func FromEnv() []kgo.Opt {
	addr := os.Getenv("KAFKA_DIAL_ADDR")
	if addr == "" {
		return nil
	}
	d := &net.Dialer{Timeout: 5 * time.Second}
	return []kgo.Opt{kgo.Dialer(func(ctx context.Context, network, _ string) (net.Conn, error) {
		return d.DialContext(ctx, network, addr)
	})}
}
