package query

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// The contract: an execution budget never changes a successful query's answer.
//
// A budget may REFUSE a query. It may not answer a different one. The failure
// this file exists to prevent is the second: -search.maxRows was set by the
// HTTP layer for every non-projecting pipe chain and reported for exactly one
// of them -- a bare select -- so `| sort`, `| offset`, `| rename`, `| delete`,
// `| format`, `| join` and `| union` each had their input silently cut by the
// scan and then answered from the truncated set. A sort of the first N rows is
// not the first N of the sort, and nothing said so: the client got 200 and a
// plausible answer.
//
// Every case below is that shape. Each runs a query whose input exceeds the
// row budget and asserts a typed error rather than a short answer.

// limitStore builds `rows` single-group rows with a `seq` field descending in
// value order but ascending in time, so a sort by seq must see every row to be
// right -- a truncated input gives a different, plausible answer.
func limitStore(t *testing.T, rows int) *storage.Store {
	t.Helper()
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	times := make([]int64, rows)
	seqs := make([]string, rows)
	msgs := make([]string, rows)
	svcs := make([]string, rows)
	for i := 0; i < rows; i++ {
		times[i] = int64(i + 1)
		seqs[i] = fmt.Sprintf("%06d", rows-i) // descending: the last row sorts first
		msgs[i] = fmt.Sprintf("body %d", i)
		svcs[i] = fmt.Sprintf("svc-%d", i%7)
	}
	sd := storage.BuildDict(seqs)
	md := storage.BuildDict(msgs)
	vd := storage.BuildDict(svcs)
	if _, err := s.AppendGroup(&storage.Group{Rows: rows, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: times},
		{Name: "_msg", Type: storage.ColDict, Dict: &md},
		{Name: "seq", Type: storage.ColDict, Dict: &sd},
		{Name: "service", Type: storage.ColDict, Dict: &vd},
	}}); err != nil {
		t.Fatal(err)
	}
	return s
}

func limitQuery(t *testing.T, src string, lim Limits) *Query {
	t.Helper()
	q, err := ParseLogsQL(src)
	if err != nil {
		t.Fatalf("%s: %v", src, err)
	}
	q.From, q.To = 0, math.MaxInt64
	q.MatAll = true
	q.Bind(context.Background(), lim)
	return q
}

// Every pipe the HTTP layer used to bound-and-not-report. Each query's input
// is 200 rows against a 10-row budget, so each one used to answer from 11 rows
// and return 200 OK.
func TestABudgetRefusesRatherThanChangingTheAnswer(t *testing.T) {
	const rows, budget = 200, 10
	s := limitStore(t, rows)

	for _, src := range []string{
		"* | sort by (seq)",
		"* | sort by (seq) | limit 3", // the limit is not first, so it proves nothing
		"* | offset 5",
		"* | rename seq as n",
		"* | delete service",
		"* | format \"<seq>\" as f",
		"* | union (*)",
		"* | join by (service) (*)",
	} {
		t.Run(src, func(t *testing.T) {
			q := limitQuery(t, src, Limits{MaxRows: budget})
			out := RunPipeline(s, q)
			err := q.StopErr()
			if err == nil {
				t.Fatalf("no error: %d rows returned from an input of %d against a %d-row budget",
					len(out), rows, budget)
			}
			if !errors.Is(err, ErrRowLimit) && !errors.Is(err, ErrPipeRowLimit) {
				t.Fatalf("StopErr = %v, want a typed row-limit error", err)
			}
		})
	}
}

// An explicit LogsQL limit proves bounded semantics, so it is answered rather
// than refused: `| limit N` as the FIRST pipe is pushed into the scan, which
// then stops at N by construction instead of overrunning a budget.
func TestAnExplicitLimitIsAnsweredNotRefused(t *testing.T) {
	const rows, budget = 200, 10
	s := limitStore(t, rows)

	q := limitQuery(t, "* | limit 5", Limits{MaxRows: budget})
	out := RunPipeline(s, q)
	if err := q.StopErr(); err != nil {
		t.Fatalf("an explicitly bounded query was refused: %v", err)
	}
	if len(out) != 5 {
		t.Fatalf("%d rows, want 5", len(out))
	}
}

// A projecting aggregate is not bounded by MaxRows at all -- its input is the
// scan and its output is small -- so it keeps answering.
func TestAnAggregateIsNotBoundedByTheRowBudget(t *testing.T) {
	s := limitStore(t, 200)
	q := limitQuery(t, "* | stats count() n", Limits{MaxGroupKeys: 100})
	out := RunPipeline(s, q)
	if err := q.StopErr(); err != nil {
		t.Fatalf("a stats query was refused: %v", err)
	}
	if len(out) != 1 || rowField(out[0], "n") != "200" {
		t.Fatalf("stats answered %v", out)
	}
}

// Aggregate cardinality has its own ceiling, because nothing else measures it:
// MaxRows counts the scan's rows, of which a high-cardinality aggregate may
// read few, and MaxBytes counts materialized row bytes, which an aggregate
// does not accumulate. The map it builds is proportional to the key space
// alone.
func TestAggregateCardinalityIsBounded(t *testing.T) {
	s := limitStore(t, 200)
	for _, src := range []string{
		"* | stats by (seq) count() n", // 200 distinct keys
		"* | uniq by (seq)",
		"* | top 5 (seq)",
	} {
		t.Run(src, func(t *testing.T) {
			q := limitQuery(t, src, Limits{MaxGroupKeys: 10})
			out := RunPipeline(s, q)
			if err := q.StopErr(); !errors.Is(err, ErrTooManyGroupKeys) {
				t.Fatalf("StopErr = %v (%d rows), want ErrTooManyGroupKeys", err, len(out))
			}
		})
	}
	// And a key space inside the ceiling still answers.
	q := limitQuery(t, "* | stats by (service) count() n", Limits{MaxGroupKeys: 10})
	out := RunPipeline(s, q)
	if err := q.StopErr(); err != nil {
		t.Fatalf("7 distinct services against a ceiling of 10 was refused: %v", err)
	}
	if len(out) != 7 {
		t.Fatalf("%d groups, want 7", len(out))
	}
}

// A join multiplies. 200 outer rows joined to a right side with the same
// non-unique key produce far more rows than either side, which is a result no
// other budget bounds: both inputs passed MaxRows.
func TestJoinFanoutIsBounded(t *testing.T) {
	s := limitStore(t, 200)
	q := limitQuery(t, "* | join by (service) (*)", Limits{MaxPipeRows: 500})
	out := RunPipeline(s, q)
	if err := q.StopErr(); !errors.Is(err, ErrPipeRowLimit) {
		t.Fatalf("StopErr = %v (%d rows), want ErrPipeRowLimit", err, len(out))
	}
	if out != nil {
		t.Errorf("a refused join returned %d rows", len(out))
	}
}

// A union appends, so its output is the sum of two results each of which was
// inside the budget on its own.
func TestUnionRowsAreBounded(t *testing.T) {
	s := limitStore(t, 200)
	q := limitQuery(t, "* | union (*)", Limits{MaxPipeRows: 300})
	out := RunPipeline(s, q)
	if err := q.StopErr(); !errors.Is(err, ErrPipeRowLimit) {
		t.Fatalf("StopErr = %v (%d rows), want ErrPipeRowLimit", err, len(out))
	}
}

// stream_context needs the whole window in time order, so a window it cannot
// hold is a query to refuse. It used to cap the context scan with a Limit and
// compute context over the first two million rows -- dropping neighbours that
// exist because rows elsewhere in the window spent the budget, with nothing
// saying so.
func TestStreamContextRefusesAWindowItCannotHold(t *testing.T) {
	s := limitStore(t, 200)
	q := limitQuery(t, "body 5 | stream_context before 2 after 2", Limits{MaxPipeRows: 50})
	out := RunPipeline(s, q)
	if err := q.StopErr(); !errors.Is(err, ErrPipeRowLimit) {
		t.Fatalf("StopErr = %v (%d rows), want ErrPipeRowLimit", err, len(out))
	}
	if err := q.StopErr(); !strings.Contains(err.Error(), "narrow the window") {
		t.Errorf("the refusal does not say what to do: %v", err)
	}

	// With room, it answers.
	q2 := limitQuery(t, "body 5 | stream_context before 2 after 2", Limits{MaxPipeRows: 5000})
	out2 := RunPipeline(s, q2)
	if err := q2.StopErr(); err != nil {
		t.Fatalf("a window that fits was refused: %v", err)
	}
	if len(out2) == 0 {
		t.Fatal("no context rows")
	}
}

// A subquery's stop reason reaches the outer query. It used to record on a
// Query the caller threw away, so every subquery failure was reported as the
// generic "time or byte budget" -- including a cancelled client, which is not
// a budget at all.
func TestASubqueryStopReasonReachesTheOuterQuery(t *testing.T) {
	s := limitStore(t, 200)
	ctx, cancel := context.WithCancel(context.Background())
	q := limitQuery(t, "* | union (*)", Limits{})
	q.Bind(ctx, Limits{})
	cancel()
	RunPipeline(s, q)
	if err := q.StopErr(); !errors.Is(err, ErrCanceled) {
		t.Fatalf("StopErr = %v, want ErrCanceled", err)
	}
}

// The zero budget bounds nothing, so an internal caller with its own
// accounting keeps the behaviour it had before any of this existed.
func TestTheZeroBudgetChangesNothing(t *testing.T) {
	s := limitStore(t, 200)
	for _, src := range []string{
		"* | sort by (seq)",
		"* | stats by (seq) count() n",
		"* | union (*)",
		"* | join by (service) (*)",
	} {
		q := limitQuery(t, src, Limits{})
		out := RunPipeline(s, q)
		if err := q.StopErr(); err != nil {
			t.Errorf("%s was refused under a zero budget: %v", src, err)
		}
		if len(out) == 0 {
			t.Errorf("%s returned nothing", src)
		}
	}
}
