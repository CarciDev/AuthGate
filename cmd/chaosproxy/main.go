// Command chaosproxy is a TCP proxy in front of the Kafka broker with an HTTP control port.
// POST /down severs all connections and refuses new ones (a broker outage); POST /up restores
// service. It stands in for `docker compose restart redpanda` where Docker is unavailable.
package main

import (
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:19092", "proxy listen address")
	target := flag.String("target", "build-redpanda:9092", "broker address")
	ctl := flag.String("control", "127.0.0.1:19093", "control HTTP address")
	flag.Parse()

	var (
		down  atomic.Bool
		mu    sync.Mutex
		conns = map[net.Conn]struct{}{}
	)
	track := func(c net.Conn, add bool) {
		mu.Lock()
		defer mu.Unlock()
		if add {
			conns[c] = struct{}{}
		} else {
			delete(conns, c)
		}
	}
	http.HandleFunc("POST /down", func(w http.ResponseWriter, _ *http.Request) {
		down.Store(true)
		mu.Lock()
		for c := range conns {
			c.Close()
		}
		mu.Unlock()
		w.Write([]byte("down"))
	})
	http.HandleFunc("POST /up", func(w http.ResponseWriter, _ *http.Request) { down.Store(false); w.Write([]byte("up")) })
	go func() { log.Fatal(http.ListenAndServe(*ctl, nil)) }()

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		if down.Load() {
			c.Close()
			continue
		}
		go func() {
			up, err := net.Dial("tcp", *target)
			if err != nil {
				c.Close()
				return
			}
			track(c, true)
			track(up, true)
			go func() { io.Copy(up, c); up.Close(); c.Close() }()
			io.Copy(c, up)
			up.Close()
			c.Close()
			track(c, false)
			track(up, false)
		}()
	}
}
