package storage

import (
	"fmt"
	"testing"
)

// What the group COUNT costs a query, which is what compaction is for.
//
// The window skip is per group: a query that matches nothing still asks every
// group whether its span overlaps. A client sending one row per request makes
// that a decision per row, forever, for every query.
//
// Measured, 5,000 one-row groups against the same rows in one group. What is
// reproducible here is the RATIO; the absolute nanoseconds are not.
//
//	six interleaved runs, 2000x, load average 1.9:
//	  small groups  10,414 - 11,182 ns   min 10,414   spread 1.07x
//	  compacted         25.33 - 41.39    min  25.33   spread 1.63x
//	  -> 411x on the minimums, 252x on the least favourable pairing
//
//	an independent measurement, different load: 284x and 274x
//
// An earlier version of this comment claimed 582x from three runs at 200-300x
// benchtime, whose spreads were 1.65x and 2.2x -- a 2.2x spread makes a
// minimum-of-three meaningless at this repo's 8.3% floor, and its small-groups
// figure was four times what a longer run measures. The benchtime was the
// problem, not the machine. The band is 250-410x across two sessions and two
// loads; nothing narrower survives a second session.
func BenchmarkGroupsWindowScan(b *testing.B) {
	for _, compacted := range []bool{false, true} {
		name := "small-groups"
		if compacted {
			name = "compacted"
		}
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			s, err := OpenStore(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			for i := 0; i < 5000; i++ {
				if _, err := s.AppendGroup(benchOneRowGroup(int64(i+1)*1000, i)); err != nil {
					b.Fatal(err)
				}
			}
			if compacted {
				if _, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 8192}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(s.Groups(0, 1<<62))), "groups")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// A window that matches nothing: the cost is the skip decision
				// per group and nothing else.
				if got := len(s.Groups(4_000_000, 4_001_000)); got > 5000 {
					b.Fatal(got)
				}
			}
		})
	}
}

// BenchmarkCompactGroups is the write side: what one pass costs.
func BenchmarkCompactGroups(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		s, err := OpenStore(dir)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 2000; j++ {
			if _, err := s.AppendGroup(benchOneRowGroup(int64(j+1)*1000, j)); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
		if _, err := s.CompactGroups(CompactOptions{MinGroups: 2, MaxRowsPerOutput: 8192}); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		s.Close()
		b.StartTimer()
	}
}

func benchOneRowGroup(ts int64, i int) *Group {
	g := &Group{Rows: 1}
	g.Columns = append(g.Columns, Column{Name: "_time", Type: ColTimestamp, Ts: []int64{ts}})
	svc := BuildDict([]string{fmt.Sprintf("svc-%d", i%7)})
	g.Columns = append(g.Columns, Column{Name: "service", Type: ColDict, Dict: &svc})
	msg := BuildDict([]string{fmt.Sprintf("message %d", i)})
	g.Columns = append(g.Columns, Column{Name: "_msg", Type: ColDict, Dict: &msg})
	return g
}
