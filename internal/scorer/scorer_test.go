package scorer

import (
	"encoding/json"
	"math"
	"os"
	"testing"
)

type golden struct {
	FeatureNames []string `json:"feature_names"`
	Rows         []struct {
		Features []*float64 `json:"features"` // null = missing
		Score    float64    `json:"score"`
	} `json:"rows"`
}

// TestGoldenParity checks Go scores against the Python scores on the 200 golden rows.
func TestGoldenParity(t *testing.T) {
	raw, err := os.ReadFile("../../results/golden_scores.json")
	if err != nil {
		t.Fatal(err)
	}
	var g golden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Rows) != 200 {
		t.Fatalf("want 200 golden rows, got %d", len(g.Rows))
	}
	s, err := Load("../../checkpoints/model.txt", g.FeatureNames)
	if err != nil {
		t.Skipf("model not available (run train.py): %v", err)
	}
	var maxDiff float64
	for i, r := range g.Rows {
		x := make([]float64, len(r.Features))
		for j, f := range r.Features {
			if f == nil {
				x[j] = math.NaN()
			} else {
				x[j] = *f
			}
		}
		maxDiff = math.Max(maxDiff, math.Abs(s.Score(x)-r.Score))
		if d := math.Abs(s.Score(x) - r.Score); d >= 1e-6 {
			t.Fatalf("row %d: go=%v python=%v diff=%g", i, s.Score(x), r.Score, d)
		}
	}
	t.Logf("max abs diff over %d rows: %g", len(g.Rows), maxDiff)
}
