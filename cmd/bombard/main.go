// Command bombard sends N unique authorizations to /authorize with a fixed worker pool and
// writes a small JSON summary. Used by bench/chaos.sh (vegeta drives the latency sweep).
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/authorize", "endpoint")
	n := flag.Int("n", 1_000_000, "number of authorizations")
	workers := flag.Int("workers", 64, "concurrent clients")
	accounts := flag.Int("accounts", 1000, "accounts to spread across (acc-0..)")
	run := flag.String("run", "run", "run id used in idempotency keys and card ids")
	out := flag.String("out", "", "write JSON summary here")
	flag.Parse()

	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *workers}}
	// post retries with the same Idempotency-Key, which is safe by design.
	post := func(i int) (string, error) {
		body, _ := json.Marshal(map[string]any{
			"account_id": fmt.Sprintf("acc-%d", i%*accounts), "card_id": fmt.Sprintf("%s-card-%d", *run, i%500_000),
			"amount_cents": 100 + i%900, "merchant": "m-1"})
		var err error
		for try := 0; try < 5; try++ {
			var req *http.Request
			req, _ = http.NewRequest("POST", *url, bytes.NewReader(body))
			req.Header.Set("Idempotency-Key", fmt.Sprintf("%s-%d", *run, i))
			var resp *http.Response
			if resp, err = client.Do(req); err != nil {
				continue
			}
			var d struct {
				Decision string `json:"decision"`
			}
			if resp.StatusCode == 200 {
				err = json.NewDecoder(resp.Body).Decode(&d)
			} else {
				err = fmt.Errorf("status %d", resp.StatusCode)
			}
			resp.Body.Close()
			if err == nil {
				return d.Decision, nil
			}
		}
		return "", err
	}

	var next, ok, failed atomic.Int64
	var mu sync.Mutex
	counts := map[string]int{}
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= *n {
					return
				}
				dec, err := post(i)
				if err != nil {
					failed.Add(1)
					continue
				}
				ok.Add(1)
				mu.Lock()
				counts[dec]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	secs := time.Since(start).Seconds()
	sum := map[string]any{"sent": *n, "ok": ok.Load(), "failed": failed.Load(), "decisions": counts,
		"seconds": secs, "rps": float64(ok.Load()) / secs}
	b, _ := json.MarshalIndent(sum, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			log.Fatal(err)
		}
	}
}
