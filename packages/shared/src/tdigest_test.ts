// Apache License 2.0
// https://github.com/influxdata/tdigest

package tdigest_test

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/influxdata/tdigest"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

const (
	N     = 1e6
	Mu    = 10
	Sigma = 3

	seed = 42
)

// NormalData is a slice of N random values that are normaly distributed with mean Mu and standard deviation Sigma.
var NormalData []float64
var UniformData []float64

var NormalDigest *tdigest.TDigest
var UniformDigest *tdigest.TDigest

func init() {
	dist := distuv.Normal{
		Mu:    Mu,
		Sigma: Sigma,
		Src:   rand.New(rand.NewSource(seed)),
	}
	uniform := rand.New(rand.NewSource(seed))

	UniformData = make([]float64, N)
	UniformDigest = tdigest.NewWithCompression(1000)

	NormalData = make([]float64, N)
	NormalDigest = tdigest.NewWithCompression(1000)

	for i := range NormalData {
		NormalData[i] = dist.Rand()
		NormalDigest.Add(NormalData[i], 1)

		UniformData[i] = uniform.Float64() * 100
		UniformDigest.Add(UniformData[i], 1)
	}
}

// Compares the quantile results of two digests, and fails if the
// fractional err exceeds maxErr.
// Always fails if the total count differs.
func compareQuantiles(td1, td2 *tdigest.TDigest, maxErr float64) error {
	if td1.Count() != td2.Count() {
		return fmt.Errorf("counts are not equal, %d vs %d", int64(td1.Count()), int64(td2.Count()))
	}
	for q := 0.05; q < 1; q += 0.05 {
		if math.Abs(td1.Quantile(q)-td2.Quantile(q))/td1.Quantile(q) > maxErr {
			return fmt.Errorf("quantile %g differs, %g vs %g", q, td1.Quantile(q), td2.Quantile(q))
		}
	}
	return nil
}

// All Add methods should yield equivalent results.
func TestTdigest_AddFuncs(t *testing.T) {
	centroids := NormalDigest.Centroids(nil)

	addDigest := tdigest.NewWithCompression(100)
	addCentroidDigest := tdigest.NewWithCompression(100)
	addCentroidListDigest := tdigest.NewWithCompression(100)

	for _, c := range centroids {
		addDigest.Add(c.Mean, c.Weight)
		addCentroidDigest.AddCentroid(c)
	}
	addCentroidListDigest.AddCentroidList(centroids)

	if err := compareQuantiles(addDigest, addCentroidDigest, 0.01); err != nil {
		t.Errorf("AddCentroid() differs from from Add(): %s", err.Error())
	}
	if err := compareQuantiles(addDigest, addCentroidListDigest, 0.01); err != nil {
		t.Errorf("AddCentroidList() differs from from Add(): %s", err.Error())
	}
}

func TestTdigest_Count(t *testing.T) {
	tests := []struct {
		name   string
		data   []float64
		digest *tdigest.TDigest
		want   float64
	}{
		{
			name: "empty",
			data: []float64{},
			want: 0,
		},
		{
			name: "not empty",
			data: []float64{5, 4},
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := tt.digest
			if td == nil {
				td = tdigest.NewWithCompression(1000)
				for _, x := range tt.data {
					td.Add(x, 1)
				}
			}
			got := td.Count()
			if got != tt.want {
				t.Errorf("unexpected count, got %g want %g", got, tt.want)
			}
		})
	}

	got := NormalDigest.Count()
	want := float64(len(NormalData))
	if got != want {
		t.Errorf("unexpected count for NormalDigest, got %g want %g", got, want)
	}

	got = UniformDigest.Count()
	want = float64(len(UniformData))
	if got != want {
		t.Errorf("unexpected count for UniformDigest, got %g want %g", got, want)
	}
}

func TestTdigest_Quantile(t *testing.T) {
	tests := []struct {
		name     string
		data     []float64
		digest   *tdigest.TDigest
		quantile float64
		want     float64
	}{
		{
			name:     "increasing",
			quantile: 0.5,
			data:     []float64{1, 2, 3, 4, 5},
			want:     3,
		},
		{
			name:     "data in decreasing order",
			quantile: 0.25,
			data:     []float64{555.349107, 432.842597},
			want:     432.842597,
		},
		{
			name:     "small",
			quantile: 0.5,
			data:     []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1},
			want:     3,
		},
		{
			name:     "small 99 (max)",
			quantile: 0.99,
			data:     []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1},
			want:     5,
		},
		{
			name:     "normal 50",
			quantile: 0.5,
			digest:   NormalDigest,
			want:     10.000673533707138,
		},
		{
			name:     "normal 90",
			quantile: 0.9,
			digest:   NormalDigest,
			want:     13.842132136909889,
		},
		{
			name:     "uniform 50",
			quantile: 0.5,
			digest:   UniformDigest,
			want:     49.992502345843555,
		},
		{
			name:     "uniform 90",
			quantile: 0.9,
			digest:   UniformDigest,
			want:     89.98281777095822,
		},
		{
			name:     "uniform 99",
			quantile: 0.99,
			digest:   UniformDigest,
			want:     98.98503400959562,
		},
		{
			name:     "uniform 99.9",
			quantile: 0.999,
			digest:   UniformDigest,
			want:     99.90103781043621,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := tt.digest
			if td == nil {
				td = tdigest.NewWithCompression(1000)
				for _, x := range tt.data {
					td.Add(x, 1)
				}
			}
			got := td.Quantile(tt.quantile)
			if got != tt.want {
				t.Errorf("unexpected quantile %f, got %g want %g", tt.quantile, got, tt.want)
			}
		})
	}
}

func TestTdigest_CDFs(t *testing.T) {
	tests := []struct {
		name   string
		data   []float64
		digest *tdigest.TDigest
		cdf    float64
		want   float64
	}{
		{
			name: "increasing",
			cdf:  3,
			data: []float64{1, 2, 3, 4, 5},
			want: 0.5,
		},
		{
			name: "small",
			cdf:  4,
			data: []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1},
			want: 0.75,
		},
		{
			name: "small max",
			cdf:  5,
			data: []float64{1, 2, 3, 4, 5, 5, 4, 3, 2, 1},
			want: 1,
		},
		{
			name: "normal mean",
			cdf:  10,
			data: NormalData,
			want: 0.4999156505250766,
		},
		{
			name: "normal high",
			cdf:  -100,
			data: NormalData,
			want: 0,
		},
		{
			name: "normal low",
			cdf:  110,
			data: NormalData,
			want: 1,
		},
		{
			name: "uniform 50",
			cdf:  50,
			data: UniformData,
			want: 0.5000756133965755,
		},
		{
			name: "uniform min",
			cdf:  0,
			data: UniformData,
			want: 0,
		},
		{
			name: "uniform max",
			cdf:  100,
			data: UniformData,
			want: 1,
		},
		{
			name: "uniform 10",
			cdf:  10,
			data: UniformData,
			want: 0.09987932577650871,
		},
		{
			name: "uniform 90",
			cdf:  90,
			data: UniformData,
			want: 0.9001667885256108,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := tt.digest
			if td == nil {
				td = tdigest.NewWithCompression(1000)
				for _, x := range tt.data {
					td.Add(x, 1)
				}
			}
			got := td.CDF(tt.cdf)
			if got != tt.want {
				t.Errorf("unexpected CDF %f, got %g want %g", tt.cdf, got, tt.want)
			}
		})
	}
}

func TestTdigest_Reset(t *testing.T) {
	td := tdigest.New()
	for _, x := range NormalData {
		td.Add(x, 1)
	}
	q1 := td.Quantile(0.9)

	td.Reset()
	for _, x := range NormalData {
		td.Add(x, 1)
	}
	if q2 := td.Quantile(0.9); q2 != q1 {
		t.Errorf("unexpected quantile, got %g want %g", q2, q1)
	}
}

func TestTdigest_OddInputs(t *testing.T) {
	td := tdigest.New()
	td.Add(math.NaN(), 1)
	td.Add(1, math.NaN())
	td.Add(1, 0)
	td.Add(1, -1000)
	if td.Count() != 0 {
		t.Error("invalid value was alloed to be added")
	}

	// Infinite values are allowed.
	td.Add(1, 1)
	td.Add(2, 1)
	td.Add(math.Inf(1), 1)
	if q := td.Quantile(0.5); q != 2 {
		t.Errorf("expected median value 2, got %f", q)
	}
	if q := td.Quantile(0.9); !math.IsInf(q, 1) {
		t.Errorf("expected median value 2, got %f", q)
	}
}

func TestTdigest_Merge(t *testing.T) {
	// Repeat merges enough times to ensure we call compress()
	numRepeats := 20
	addDigest := tdigest.New()
	for i := 0; i < numRepeats; i++ {
		for _, c := range NormalDigest.Centroids(nil) {
			addDigest.AddCentroid(c)
		}
		for _, c := range UniformDigest.Centroids(nil) {
			addDigest.AddCentroid(c)
		}
	}

	mergeDigest := tdigest.New()
	for i := 0; i < numRepeats; i++ {
		mergeDigest.Merge(NormalDigest)
		mergeDigest.Merge(UniformDigest)
	}

	if err := compareQuantiles(addDigest, mergeDigest, 0.001); err != nil {
		t.Errorf("AddCentroid() differs from from Merge(): %s", err.Error())
	}

	// Empty merge does nothing and has no effect on underlying centroids.
	c1 := addDigest.Centroids(nil)
	addDigest.Merge(tdigest.New())
	c2 := addDigest.Centroids(nil)
	if !reflect.DeepEqual(c1, c2) {
		t.Error("Merging an empty digest altered data")
	}
}

var quantiles = []float64{0.1, 0.5, 0.9, 0.99, 0.999}

func BenchmarkTDigest_Add(b *testing.B) {
	for n := 0; n < b.N; n++ {
		td := tdigest.NewWithCompression(1000)
		for _, x := range NormalData {
			td.Add(x, 1)
		}
	}
}

func BenchmarkTDigest_AddCentroid(b *testing.B) {
	centroids := make(tdigest.CentroidList, len(NormalData))
	for i := range centroids {
		centroids[i].Mean = NormalData[i]
		centroids[i].Weight = 1
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		td := tdigest.NewWithCompression(1000)
		for i := range centroids {
			td.AddCentroid(centroids[i])
		}
	}
}

func BenchmarkTDigest_AddCentroidList(b *testing.B) {
	centroids := make(tdigest.CentroidList, len(NormalData))
	for i := range centroids {
		centroids[i].Mean = NormalData[i]
		centroids[i].Weight = 1
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		td := tdigest.NewWithCompression(1000)
		td.AddCentroidList(centroids)
	}
}

func BenchmarkTDigest_Merge(b *testing.B) {
	b.Run("AddCentroid", func(b *testing.B) {
		var cl tdigest.CentroidList
		td := tdigest.New()
		for n := 0; n < b.N; n++ {
			cl = NormalDigest.Centroids(cl[:0])
			for i := range cl {
				td.AddCentroid(cl[i])
			}
		}
	})
	b.Run("Merge", func(b *testing.B) {
		td := tdigest.New()
		for n := 0; n < b.N; n++ {
			td.Merge(NormalDigest)
		}
	})
}

func BenchmarkTDigest_Quantile(b *testing.B) {
	td := tdigest.NewWithCompression(1000)
	for _, x := range NormalData {
		td.Add(x, 1)
	}
	b.ResetTimer()
	var x float64
	for n := 0; n < b.N; n++ {
		for _, q := range quantiles {
			x += td.Quantile(q)
		}
	}
}

func TestTdigest_Centroids(t *testing.T) {
	tests := []struct {
		name   string
		data   []float64
		digest *tdigest.TDigest
		want   tdigest.CentroidList
	}{
		{
			name: "increasing",
			data: []float64{1, 2, 3, 4, 5},
			want: tdigest.CentroidList{
				tdigest.Centroid{
					Mean:   1.0,
					Weight: 1.0,
				},

				tdigest.Centroid{
					Mean:   2.5,
					Weight: 2.0,
				},

				tdigest.Centroid{
					Mean:   4.0,
					Weight: 1.0,
				},

				tdigest.Centroid{
					Mean:   5.0,
					Weight: 1.0,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got tdigest.CentroidList
			td := tt.digest
			if td == nil {
				td = tdigest.NewWithCompression(3)
				for _, x := range tt.data {
					td.Add(x, 1)
				}
			}
			got = td.Centroids(got[:0])
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("unexpected list got %g want %g", got, tt.want)
			}
		})
	}
}