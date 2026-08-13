package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// TestVLParityFilters covers the added filter kinds: range, len_range,
// string_range, i (case-insensitive).
func TestVLParityFilters(t *testing.T) {
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	status := storage.BuildDict([]string{"200", "404", "500", "201"})
	level := storage.BuildDict([]string{"Error", "info", "WARN", "error"})
	if _, err := s.AppendGroup(&storage.Group{Rows: 4, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1, 2, 3, 4}},
		{Name: "status", Type: storage.ColDict, Dict: &status},
		{Name: "level", Type: storage.ColDict, Dict: &level},
	}}); err != nil {
		t.Fatal(err)
	}
	count := func(q string) int {
		pq, err := ParseLogsQL(q)
		if err != nil {
			t.Fatalf("parse %q: %v", q, err)
		}
		pq.From, pq.To = 0, int64(1)<<62
		return len(RunPipeline(s, pq))
	}

	// range(a, b) is an OPEN interval, as in LogsQL: the parentheses exclude the
	// bounds, so 200 does not match and only 201 does. Verified against the real
	// VictoriaLogs binary by the compat differential (internal/bench/compat_test.go),
	// where treating it as inclusive returned one row more than VL for every query.
	if n := count(`status:range(200, 299)`); n != 1 { // 201 only; 200 is the excluded bound
		t.Errorf("range(200,299) = %d rows, want 1", n)
	}
	if n := count(`level:i("error")`); n != 2 { // Error, error
		t.Errorf(`i("error") = %d rows, want 2`, n)
	}
	if n := count(`level:string_range(a, m)`); n != 2 { // info, error (byte-wise a<=v<m)
		t.Errorf("string_range(a,m) = %d rows, want 2", n)
	}
	if n := count(`level:len_range(4, 4)`); n != 2 { // info, WARN
		t.Errorf("len_range(4,4) = %d rows, want 2", n)
	}
	if n := count(`status:range(0, 999)`); n != 4 {
		t.Errorf("range(0,999) = %d rows, want 4", n)
	}
}
