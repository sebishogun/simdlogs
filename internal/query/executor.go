package query

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// The cancellable query executor.
//
// # Why a context was not enough on its own
//
// Go does not abort a running handler when its request context is cancelled.
// A client that hangs up, a proxy that times out, a `-search.maxDuration` that
// elapses -- none of them stop a scan that is already walking groups. Before
// this, `Run` took no context at all and the budget fields on Query were the
// only thing that could end a scan early, which is why they exist and why
// their own comment names this task as the missing half.
//
// So cancellation is not a wrapper around the engine, it is threaded into it.
// The engine already had a per-group checkpoint -- `q.exceeded(bytes)`, called
// after every group in every scan loop and from every parallel worker -- and
// that checkpoint is where the context is read. Every call site that already
// stopped for a byte budget now also stops for a cancelled context, a deadline
// or a group ceiling, without a new checkpoint having to be remembered at each
// of them.
//
// # Why the reason is recorded rather than returned
//
// `exceeded` returns a bool and the scan functions return rows. Threading an
// error return through every one of them would touch the whole package for a
// value only the top of the call stack reads. Instead the FIRST stop records
// why, on the Query, and the executor reads it after the scan returns. First,
// not last: a cancelled context and an exhausted budget can both become true
// while the scan unwinds, and reporting the second one tells the caller to fix
// the wrong thing.
//
// # What a caller gets
//
// A typed error per cause, so an HTTP layer can map it to a stable status and
// an operator can tell "you asked for too much" from "we ran out of time" from
// "you went away". A short result presented as a complete one is the failure
// this replaces: every limit here ends the scan, and none of them truncates
// silently.

// The reasons a query stops early. Each is a different thing for the caller to
// do about it, which is why they are separate values rather than one error
// with a string.
var (
	// ErrCanceled is the caller's context, cancelled. Usually the client hung
	// up, in which case nobody is left to read the answer.
	ErrCanceled = errors.New("query: canceled")

	// ErrDeadlineExceeded is the query's time budget.
	ErrDeadlineExceeded = errors.New("query: deadline exceeded")

	// ErrRowLimit is more matching rows than the caller allowed. Distinct from
	// a `| limit N` pipe, which is a result the caller asked for; this is a
	// refusal to materialize what they asked for.
	ErrRowLimit = errors.New("query: row limit exceeded")

	// ErrByteLimit is the materialized rows' byte budget.
	ErrByteLimit = errors.New("query: byte limit exceeded")

	// ErrMemoryLimit is the working-set budget: what the scan holds at once,
	// as opposed to what it has produced in total.
	ErrMemoryLimit = errors.New("query: memory limit exceeded")

	// ErrTooManyGroups is more row groups surviving the time and footer prune
	// than the caller allowed. It is the cheapest limit to hit and the only
	// one that fires before any column is decoded, which makes it the one that
	// protects the machine rather than the answer.
	ErrTooManyGroups = errors.New("query: too many row groups")

	// ErrRejected is admission control: the server declined to start this
	// query at all. Task 6.2 sets it; the executor carries it so the HTTP
	// mapping has one place to live.
	ErrRejected = errors.New("query: rejected")
)

// Limits is one query's budget. The zero value is unbounded, which is what an
// internal caller with its own accounting wants; the HTTP layer fills it from
// configuration.
type Limits struct {
	// MaxRows bounds materialized rows. The scan stops once it has produced
	// more, and the caller gets ErrRowLimit rather than a short answer.
	MaxRows int

	// MaxBytes bounds the materialized rows' size.
	MaxBytes int64

	// MaxGroups bounds the row groups a query may scan AFTER the time window
	// and the footer prune. Before either, every query on a large store would
	// trip it.
	MaxGroups int

	// Timeout is the wall-clock budget, measured from Execute. A context
	// deadline the caller already set is honoured too, and the earlier of the
	// two wins -- a caller who set both meant both.
	Timeout time.Duration

	// MaxMemory bounds the working set the scan holds at once. Distinct from
	// MaxBytes, which is cumulative: a query that streams a terabyte through a
	// small buffer is fine by this one and not by that one.
	MaxMemory int64
}

// Sink receives matching rows in batches.
//
// Batches rather than one row per call: a per-row callback on a scan that
// produces millions is a call per row on the hot path, and the engine already
// produces rows a group at a time. A sink that returns an error stops the
// scan, and that error reaches the caller unchanged -- so a sink writing to a
// hung-up connection ends the query rather than filling a buffer nobody reads.
//
// The slice is only valid for the duration of the call. A sink that keeps rows
// copies them.
type Sink func(rows []Row) error

// Executor runs queries against a store under a budget.
type Executor struct {
	Store  Store
	Limits Limits
}

// Execute runs q and delivers matching rows to sink.
//
// It returns nil only if the query ran to completion and the sink accepted
// everything. Any other outcome is a typed error: the scan never reports a
// truncated result as a complete one.
func (e *Executor) Execute(ctx context.Context, q *Query, sink Sink) error {
	if e == nil || e.Store == nil {
		return fmt.Errorf("%w: no store", ErrRejected)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cancel := func() {}
	if e.Limits.Timeout > 0 {
		// WithTimeout takes the earlier of its own deadline and the parent's,
		// so a caller who set a shorter one keeps it.
		ctx, cancel = context.WithTimeout(ctx, e.Limits.Timeout)
	}
	defer cancel()

	// The budget travels on the Query because that is what every scan
	// function already receives. Copied rather than mutated in place: a
	// caller reusing a Query across executions must not inherit the previous
	// run's stop reason.
	run := *q
	run.bindContext(ctx, e.Limits)

	// RunPipeline, not Run: a query with a pipe chain answers through the
	// pipes, and an executor that called the bare scan would return the
	// filter's rows with every stats, sort and limit silently dropped.
	// RunPipeline falls through to Run when there are no pipes.
	rows := RunPipeline(e.Store, &run)
	// The stop reason is read BEFORE the rows are delivered. A scan that
	// stopped early produced a prefix of the answer, and handing that prefix
	// to the sink and then erroring is how a caller ends up with a partial
	// result it has already written to the client.
	if err := run.stopErr(); err != nil {
		return err
	}
	// The row ceiling is signalled by the scan producing MORE than it, not by
	// a stop reason: that check lives in the scan loops and predates the
	// executor. Reported here rather than left to the caller, because a caller
	// that forgot to compare would present the overflow as a complete answer,
	// which is the failure this whole type exists to prevent.
	if run.MaxRows > 0 && len(rows) > run.MaxRows {
		return fmt.Errorf("%w: more than %d rows matched", ErrRowLimit, run.MaxRows)
	}
	if len(rows) == 0 {
		return nil
	}
	return sink(rows)
}

// ExecuteCount is Execute for a query whose answer is a count.
//
// Separate rather than a flag, because the engine's count path never
// materializes a row and so cannot be expressed as a sink that ignores them.
func (e *Executor) ExecuteCount(ctx context.Context, q *Query) (int, error) {
	if e == nil || e.Store == nil {
		return 0, fmt.Errorf("%w: no store", ErrRejected)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cancel := func() {}
	if e.Limits.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, e.Limits.Timeout)
	}
	defer cancel()

	run := *q
	run.bindContext(ctx, e.Limits)
	n := Count(e.Store, &run)
	if err := run.stopErr(); err != nil {
		return 0, err
	}
	return n, nil
}

// HTTPStatus maps a query error to a stable status code.
//
// Stable is the requirement: a client that retries on 503 and gives up on 400
// has to get the same answer for the same cause every time, and an operator
// reading a status distribution has to be able to tell a client asking for too
// much from a server that ran out of time.
func HTTPStatus(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrCanceled):
		// 499 is nginx's, not the RFC's, and it is the only code that says
		// "the client went away" rather than blaming either side. Nothing
		// reads the body -- the connection is gone -- so the code exists for
		// the access log.
		return 499
	case errors.Is(err, ErrDeadlineExceeded):
		return http.StatusGatewayTimeout
	case errors.Is(err, ErrRowLimit), errors.Is(err, ErrByteLimit),
		errors.Is(err, ErrTooManyGroups):
		// The request is well-formed and asks for more than the server will
		// give: 413 says which side has to change and that retrying the same
		// request will not help.
		return http.StatusRequestEntityTooLarge
	case errors.Is(err, ErrMemoryLimit):
		// A memory ceiling is about this server right now, not about the
		// request, so it is retryable and 503 says so.
		return http.StatusServiceUnavailable
	case errors.Is(err, ErrRejected):
		return http.StatusTooManyRequests
	}
	return http.StatusInternalServerError
}

// Retryable reports whether the same request could succeed later. It exists so
// a client library does not have to encode the status table above.
func Retryable(err error) bool {
	return errors.Is(err, ErrMemoryLimit) || errors.Is(err, ErrRejected) ||
		errors.Is(err, ErrDeadlineExceeded)
}
