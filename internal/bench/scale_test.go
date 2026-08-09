package bench

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// TestScale measures how query latency tracks group count as the corpus
// grows -- the concrete scaling question, since every query linearly scans
// the groups a time window selects. It builds groups directly (no JSON) with
// a high-cardinality trace column and a planted needle, disk-backed so the
// group files land on disk (the Readers stay in RAM -- there is no mmap yet,
// so this measures the cache-resident regime honestly). Run:
//
//	SIMDLOGS_SCALE=1 go test -run TestScale -v -timeout 30m ./internal/bench/
//	SIMDLOGS_SCALE=1 SIMDLOGS_SCALE_SIZES=3000000,30000000,100000000 ...
func TestScale(t *testing.T) {
	if os.Getenv("SIMDLOGS_SCALE") == "" {
		t.Skip("set SIMDLOGS_SCALE=1 to run the scaling test")
	}
	sizes := []int{3_000_000, 30_000_000, 100_000_000}
	if v := os.Getenv("SIMDLOGS_SCALE_SIZES"); v != "" {
		sizes = nil
		for _, p := range strings.Split(v, ",") {
			if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
				sizes = append(sizes, n)
			}
		}
	}
	services := []string{"api", "auth", "billing", "cache", "db", "gateway", "worker", "scheduler"}
	const needle = "NEEDLEc0ffee42scale"

	dirBase := os.Getenv("SIMDLOGS_SCALE_DIR")
	if dirBase == "" {
		dirBase = "/var/tmp"
	}

	for _, N := range sizes {
		dir, err := os.MkdirTemp(dirBase, "simdlogs-scale-")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir) // clean up even if the build fails/OOMs mid-way
		s, err := storage.OpenStore(dir)
		if err != nil {
			t.Fatal(err)
		}

		t0 := time.Now()
		var ts []int64
		var svc, tr []string
		flush := func() {
			if len(ts) == 0 {
				return
			}
			sd := storage.BuildDict(svc)
			td := storage.BuildDict(tr)
			if _, err := s.AppendGroup(&storage.Group{Rows: len(ts), Columns: []storage.Column{
				{Name: "_time", Type: storage.ColTimestamp, Ts: ts},
				{Name: "service", Type: storage.ColDict, Dict: &sd},
				{Name: "trace", Type: storage.ColDict, Dict: &td},
			}}); err != nil {
				t.Fatal(err)
			}
			ts, svc, tr = ts[:0], svc[:0], tr[:0]
		}
		for i := 0; i < N; i++ {
			ts = append(ts, int64(i+1))
			svc = append(svc, services[i%len(services)])
			v := strconv.FormatInt(int64(i), 16) // unique -> high cardinality
			if i == N-1000 {
				v = needle
			}
			tr = append(tr, v)
			if len(ts) >= storage.MaxRows {
				flush()
			}
		}
		flush()
		ingest := time.Since(t0)
		groups := s.Len()
		lo, hi := int64(1), int64(N)
		selFrom := lo + (hi-lo)/2
		selTo := selFrom + (hi-lo)/50

		needleQ := &query.Query{From: 0, To: int64(1) << 62, Preds: []query.Pred{{Field: "trace", Kind: query.Eq, Value: needle}}}
		selQ := &query.Query{From: selFrom, To: selTo, Preds: []query.Pred{{Field: "service", Kind: query.Eq, Value: "auth"}}}
		fullQ := &query.Query{From: lo, To: hi + 1, Preds: []query.Pred{{Field: "service", Kind: query.Eq, Value: "auth"}}}

		nT := timeIt(func() { query.Run(s, needleQ) })
		sT := timeIt(func() { query.Run(s, selQ) })
		cT := timeIt(func() { query.Count(s, fullQ) })

		t.Logf("N=%9d groups=%5d ingest=%7v (%.2fM rec/s) | needle=%9v (%.0fns/group) selective=%9v fullcount=%9v",
			N, groups, ingest.Round(time.Millisecond), float64(N)/ingest.Seconds()/1e6,
			nT, float64(nT.Nanoseconds())/float64(groups), sT, cT)
		s.Close() // release the mmaps before the next size
		os.RemoveAll(dir)
	}
}

func timeIt(fn func()) time.Duration {
	fn() // warmup
	best := time.Hour
	for i := 0; i < 7; i++ {
		s := time.Now()
		fn()
		if d := time.Since(s); d < best {
			best = d
		}
	}
	return best
}
