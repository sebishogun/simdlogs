package query

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A store with enough groups that a scan has somewhere to be cancelled.
func execStore(t *testing.T, groups, rowsPer int) *storage.Store {
	t.Helper()
	s, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ts := int64(1)
	for g := 0; g < groups; g++ {
		times := make([]int64, rowsPer)
		msgs := make([]string, rowsPer)
		svcs := make([]string, rowsPer)
		for i := range times {
			times[i] = ts
			ts++
			msgs[i] = fmt.Sprintf("message %d-%d with some text in it", g, i)
			svcs[i] = fmt.Sprintf("svc-%d", i%5)
		}
		md := storage.BuildDict(msgs)
		sd := storage.BuildDict(svcs)
		if _, err := s.AppendGroup(&storage.Group{Rows: rowsPer, Columns: []storage.Column{
			{Name: "_time", Type: storage.ColTimestamp, Ts: times},
			{Name: "_msg", Type: storage.ColDict, Dict: &md},
			{Name: "service", Type: storage.ColDict, Dict: &sd},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func allRows(q *Query) *Query {
	q.From, q.To = 0, 1<<62
	return q
}

// A cancelled context ends the scan, and the caller is told which of the two
// context causes it was. The distinction matters: a client hanging up and a
// deadline elapsing call for different responses.
func TestExecuteHonoursCancellation(t *testing.T) {
	s := execStore(t, 40, 200)
	e := &Executor{Store: s}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: the first checkpoint must see it

	var got int
	err := e.Execute(ctx, allRows(&Query{MatAll: true}), func(rows []Row) error {
		got += len(rows)
		return nil
	})
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("%v, want ErrCanceled", err)
	}
	if got != 0 {
		t.Errorf("the sink received %d rows from a cancelled query; a partial result was "+
			"delivered before the error", got)
	}
	if HTTPStatus(err) != 499 {
		t.Errorf("status %d, want 499", HTTPStatus(err))
	}
}

// A deadline that has already passed is reported as a deadline, not as a
// cancellation, even though Go's context reports both through Err().
func TestExecuteDistinguishesDeadlineFromCancel(t *testing.T) {
	s := execStore(t, 40, 200)
	e := &Executor{Store: s}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := e.Execute(ctx, allRows(&Query{MatAll: true}), func([]Row) error { return nil })
	if !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("%v, want ErrDeadlineExceeded", err)
	}
	if errors.Is(err, ErrCanceled) {
		t.Error("a deadline was reported as a cancellation")
	}
	if HTTPStatus(err) != http.StatusGatewayTimeout {
		t.Errorf("status %d, want 504", HTTPStatus(err))
	}
	if !Retryable(err) {
		t.Error("a deadline is retryable")
	}
}

// The executor's own timeout applies when the caller set none.
func TestExecuteAppliesItsOwnTimeout(t *testing.T) {
	s := execStore(t, 200, 500)
	e := &Executor{Store: s, Limits: Limits{Timeout: time.Nanosecond}}
	err := e.Execute(context.Background(), allRows(&Query{MatAll: true}), func([]Row) error { return nil })
	if !errors.Is(err, ErrDeadlineExceeded) {
		t.Fatalf("%v, want ErrDeadlineExceeded", err)
	}
}

// Each ceiling reports itself, and none of them truncates silently.
func TestExecuteLimits(t *testing.T) {
	s := execStore(t, 40, 200)
	for _, tc := range []struct {
		name   string
		lim    Limits
		want   error
		status int
	}{
		{"rows", Limits{MaxRows: 10}, ErrRowLimit, http.StatusRequestEntityTooLarge},
		{"bytes", Limits{MaxBytes: 1}, ErrByteLimit, http.StatusRequestEntityTooLarge},
		{"groups", Limits{MaxGroups: 2}, ErrTooManyGroups, http.StatusRequestEntityTooLarge},
		{"memory", Limits{MaxMemory: 1}, ErrMemoryLimit, http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Executor{Store: s, Limits: tc.lim}
			delivered := 0
			err := e.Execute(context.Background(), allRows(&Query{MatAll: true}),
				func(rows []Row) error { delivered += len(rows); return nil })
			if !errors.Is(err, tc.want) {
				t.Fatalf("%v, want %v", err, tc.want)
			}
			if delivered != 0 {
				t.Errorf("%d rows were delivered before the limit error", delivered)
			}
			if got := HTTPStatus(err); got != tc.status {
				t.Errorf("status %d, want %d", got, tc.status)
			}
		})
	}
}

// A query inside the budget answers normally, with the rows the pipes produce
// rather than the filter's -- the executor runs the pipeline, not the bare
// scan.
func TestExecuteRunsThePipeChain(t *testing.T) {
	s := execStore(t, 4, 50)
	e := &Executor{Store: s}
	q, err := ParseLogsQL(`* | stats count() as n`)
	if err != nil {
		t.Fatal(err)
	}
	allRows(q)
	var out []Row
	if err := e.Execute(context.Background(), q, func(rows []Row) error {
		out = append(out, rows...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("%d rows from a stats pipe, want 1: the executor ran the bare scan", len(out))
	}
	got := ""
	for _, f := range out[0].Fields {
		if f.Key == "n" {
			got = f.Value
		}
	}
	if got != "200" {
		t.Fatalf("count() = %q, want 200 (fields %+v)", got, out[0].Fields)
	}
}

// A sink that fails ends the query with the sink's own error, unchanged: a
// caller writing to a hung-up connection has to be able to tell its own
// failure from the engine's.
func TestASinkErrorReachesTheCaller(t *testing.T) {
	s := execStore(t, 4, 10)
	e := &Executor{Store: s}
	boom := errors.New("the sink went away")
	err := e.Execute(context.Background(), allRows(&Query{MatAll: true}),
		func([]Row) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("%v, want the sink's error", err)
	}
}

// Cancellation reaches the PARALLEL scan, which is a different code path with
// its own budget accounting and its own workers.
func TestCancellationReachesTheParallelScan(t *testing.T) {
	// parallelMinGroups groups with no Limit is what selects the parallel path.
	s := execStore(t, parallelMinGroups*3, 300)
	e := &Executor{Store: s}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := e.Execute(ctx, allRows(&Query{MatAll: true}), func([]Row) error { return nil })
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("%v, want ErrCanceled from the parallel path", err)
	}
}

// The LastN path -- the log viewer's tail -- is a third scan loop and needs
// the same checkpoint.
func TestCancellationReachesTheNewestFirstScan(t *testing.T) {
	s := execStore(t, 40, 200)
	e := &Executor{Store: s}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := e.Execute(ctx, allRows(&Query{MatAll: true, LastN: 5}), func([]Row) error { return nil })
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("%v, want ErrCanceled from the newest-first path", err)
	}
}

// Counting is a separate path that materializes nothing, so it cannot be
// expressed as a sink that ignores rows -- and it needs its own cancellation.
func TestExecuteCount(t *testing.T) {
	s := execStore(t, 10, 100)
	e := &Executor{Store: s}
	n, err := e.ExecuteCountForTest(context.Background(), allRows(&Query{}))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1000 {
		t.Fatalf("count = %d, want 1000", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.ExecuteCountForTest(ctx, allRows(&Query{})); !errors.Is(err, ErrCanceled) {
		t.Fatalf("%v, want ErrCanceled", err)
	}
}

// The FIRST reason is reported, not the last. A cancelled context and an
// exhausted budget can both be true while the scan unwinds, and reporting the
// second sends the caller to fix the wrong thing.
func TestTheFirstStopReasonWins(t *testing.T) {
	s := execStore(t, 40, 200)
	// A byte ceiling of 1 trips on the first group; the context is cancelled
	// before the scan starts, so the context is what the first checkpoint sees.
	e := &Executor{Store: s, Limits: Limits{MaxBytes: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := e.Execute(ctx, allRows(&Query{MatAll: true}), func([]Row) error { return nil })
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("%v, want the context (the first thing checked), not the byte budget", err)
	}
}

// A Query reused across executions does not inherit the previous run's stop.
func TestAReusedQueryDoesNotInheritAStop(t *testing.T) {
	s := execStore(t, 4, 10)
	q := allRows(&Query{MatAll: true})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := &Executor{Store: s}
	if err := e.Execute(ctx, q, func([]Row) error { return nil }); !errors.Is(err, ErrCanceled) {
		t.Fatalf("%v, want ErrCanceled", err)
	}
	// Same Query, fresh context: it must run.
	got := 0
	if err := e.Execute(context.Background(), q, func(rows []Row) error {
		got += len(rows)
		return nil
	}); err != nil {
		t.Fatalf("the reused query failed: %v", err)
	}
	if got != 40 {
		t.Fatalf("%d rows, want 40", got)
	}
}

// A Query with no executor behaves exactly as before: no context, no ceilings,
// no stop reason. Every internal caller and every existing test relies on it.
func TestAnUnboundQueryIsUnchanged(t *testing.T) {
	s := execStore(t, 4, 10)
	q := allRows(&Query{MatAll: true})
	if got := len(Run(s, q)); got != 40 {
		t.Fatalf("%d rows, want 40", got)
	}
	if err := q.stopErr(); err != nil {
		t.Fatalf("an unbound query recorded a stop: %v", err)
	}
	// And stop() on an unbound query does not panic on the nil pointer.
	q.stop(ErrCanceled)
}

// The status map is stable, which is the requirement: a client that retries on
// one code and gives up on another has to get the same answer for the same
// cause every time.
func TestHTTPStatusMap(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{
		{nil, http.StatusOK},
		{ErrCanceled, 499},
		{ErrDeadlineExceeded, http.StatusGatewayTimeout},
		{ErrRowLimit, http.StatusRequestEntityTooLarge},
		{ErrByteLimit, http.StatusRequestEntityTooLarge},
		{ErrTooManyGroups, http.StatusRequestEntityTooLarge},
		{ErrMemoryLimit, http.StatusServiceUnavailable},
		{ErrRejected, http.StatusTooManyRequests},
		{errors.New("something else"), http.StatusInternalServerError},
	} {
		if got := HTTPStatus(tc.err); got != tc.want {
			t.Errorf("HTTPStatus(%v) = %d, want %d", tc.err, got, tc.want)
		}
		// And through a wrap, because every producer wraps with context.
		if tc.err != nil {
			wrapped := fmt.Errorf("scanning: %w", tc.err)
			if got := HTTPStatus(wrapped); got != tc.want {
				t.Errorf("HTTPStatus(wrapped %v) = %d, want %d", tc.err, got, tc.want)
			}
		}
	}
}

// An executor with no store refuses rather than panicking.
func TestExecutorWithoutAStoreRefuses(t *testing.T) {
	var e *Executor
	if err := e.Execute(context.Background(), &Query{}, nil); !errors.Is(err, ErrRejected) {
		t.Fatalf("%v, want ErrRejected", err)
	}
	e2 := &Executor{}
	if _, err := e2.ExecuteCountForTest(context.Background(), &Query{}); !errors.Is(err, ErrRejected) {
		t.Fatalf("%v, want ErrRejected", err)
	}
}
