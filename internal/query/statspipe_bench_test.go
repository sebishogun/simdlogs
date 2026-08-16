package query

import (
	"fmt"
	"testing"
)

// StatsPipe over a multi-field group-by.
//
// A review measured +18.5% instructions on this shape when a per-row
// hasAllFields scan was added, which scanned r.Fields once per by-field and
// threw the value away before rowField scanned again for the same key. That
// scan is gone; this is here so the claim that it is gone can be checked.
func benchStatsRows(n int) []Row {
	out := make([]Row, n)
	for i := range out {
		out[i] = Row{NoTime: true, Fields: []Field{
			{"_msg", fmt.Sprintf("request %d", i)},
			{"svc", fmt.Sprintf("svc-%d", i%50)},
			{"level", []string{"info", "warn", "error"}[i%3]},
			{"host", fmt.Sprintf("host-%d", i%20)},
			{"region", "eu-west-1"},
		}}
	}
	return out
}

func BenchmarkStatsPipeThreeByFields(b *testing.B) {
	rows := benchStatsRows(200000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := &StatsPipe{By: []string{"svc", "level", "host"},
			Aggs: []Agg{{Kind: AggCount, Alias: "c"}}}
		sinkStatsRows = p.apply(rows)
	}
}

func BenchmarkStatsPipeOneByField(b *testing.B) {
	rows := benchStatsRows(200000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := &StatsPipe{By: []string{"svc"}, Aggs: []Agg{{Kind: AggCount, Alias: "c"}}}
		sinkStatsRows = p.apply(rows)
	}
}

func BenchmarkTopPipeOneByField(b *testing.B) {
	rows := benchStatsRows(200000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := &TopPipe{By: []string{"svc"}, N: 10}
		sinkStatsRows = p.apply(rows)
	}
}

var sinkStatsRows []Row
