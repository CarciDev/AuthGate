// Package scorer runs the offline-trained LightGBM fraud model in-process (pure Go, via leaves).
package scorer

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"os"

	"github.com/dmitryikh/leaves"
)

// FeatureNames is the model's input order, as written by train.py.
var FeatureNames = []string{"price", "avg_rating", "rating_number", "verified_purchase", "n_images",
	"n_categories", "n_features", "hour", "dow", "category_id"}

// Scorer wraps a LightGBM model loaded from model.txt.
type Scorer struct {
	model *leaves.Ensemble
	names []string
}

// Load reads a LightGBM text model. featureNames fixes the order of the model's input vector
// (the same order train.py wrote to golden_scores.json).
func Load(path string, featureNames []string) (*Scorer, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Current LightGBM writes "version=v4"; leaves only accepts v3. The tree text format is
	// unchanged for non-linear trees, and TestGoldenParity guards this assumption.
	raw = bytes.Replace(raw, []byte("version=v4"), []byte("version=v3"), 1)
	m, err := leaves.LGEnsembleFromReader(bufio.NewReader(bytes.NewReader(raw)), true)
	if err != nil {
		return nil, fmt.Errorf("load model: %w", err)
	}
	if m.NFeatures() != len(featureNames) {
		return nil, fmt.Errorf("model has %d features, got %d names", m.NFeatures(), len(featureNames))
	}
	return &Scorer{model: m, names: featureNames}, nil
}

// Score returns the fraud probability for a feature vector; NaN marks a missing value.
func (s *Scorer) Score(features []float64) float64 {
	return s.model.PredictSingle(features, 0)
}

// ScoreMap builds the vector from named features (absent names are missing/NaN) and scores it.
func (s *Scorer) ScoreMap(f map[string]float64) float64 {
	v := make([]float64, len(s.names))
	for i, n := range s.names {
		if x, ok := f[n]; ok {
			v[i] = x
		} else {
			v[i] = math.NaN()
		}
	}
	return s.Score(v)
}
