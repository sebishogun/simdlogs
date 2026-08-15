package ingest

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"syscall"
	"time"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// What a client is told when a write fails.
//
// A failed write used to be one undifferentiated 503 carrying the underlying
// error's text, which answers neither of the two questions a log shipper
// actually has:
//
//	is retrying this worth anything?
//	if I retry, do I store some of these records twice?
//
// Both are answerable here and nowhere else. The first is a property of the
// failure -- a full disk clears when someone frees space; a read-only mount or
// a group that will not read back needs an operator first. The second is a
// property of THIS request's batch accounting: the row buffer is shared by
// every request and every syslog connection on a tenant, a caller's rows are
// routinely spread across several flush jobs, and if one of those jobs landed
// while another failed then part of the payload is already durable. There is
// no idempotency key on the ingest path, so a blind retry of that payload
// stores the landed part a second time.
//
// Saying so is the entire point. A shipper told "retryable, and a retry
// duplicates" can choose; a shipper told "503" cannot.

// RetryClass is what a client should do with a failed write.
type RetryClass int

const (
	// RetrySoon: transient. A writer closing during shutdown, momentary
	// resource pressure, a descriptor limit. Seconds, not minutes.
	RetrySoon RetryClass = iota + 1

	// RetryAfterRepair: the store cannot accept writes until a human acts --
	// the filesystem is full, over quota, read-only, or not writable by this
	// process. Retrying in a tight loop accomplishes nothing except filling
	// the log with the same error, and on a full disk it is actively harmful.
	RetryAfterRepair
)

func (c RetryClass) String() string {
	switch c {
	case RetrySoon:
		return "soon"
	case RetryAfterRepair:
		return "after-repair"
	}
	return "unknown"
}

// Retry-After values. Deliberately coarse: the number is advice to a shipper's
// backoff, not a schedule, and a precise-looking value would imply the server
// knows when the disk will be freed.
const (
	retryAfterSoon   = 1 * time.Second
	retryAfterRepair = 30 * time.Second
)

// WriteError is a flush failure with the two facts a client needs.
//
// It is what Flush and FlushMark return, so `err != nil` keeps its old meaning
// for every caller that only tests for nil.
type WriteError struct {
	// Err is the first underlying failure in the window. First, not worst:
	// a batch records the error that arrived first and the rest are the same
	// failure seen by another group in almost every real case.
	Err error

	// Class says whether retrying can succeed.
	Class RetryClass

	// Partial reports that some groups in this caller's window reached the
	// store and some did not, so part of the payload may already be durable.
	//
	// It cannot be narrowed to "your rows specifically": the buffer is shared,
	// so a caller does not own the groups its rows end up in. "Some of what
	// you sent may be stored" is the strongest true statement, and it is the
	// one that decides whether a retry duplicates.
	Partial bool

	// FailedGroups and TotalGroups are the units of work in the window.
	// Reported so an operator reading a log line can tell one unlucky group
	// from a store that is refusing every write.
	FailedGroups int
	TotalGroups  int

	// Unit names what the two counts count. "groups" on the ordinary path,
	// "shard writers" on the parallel one, where a request is split across
	// several writers and a shard is what succeeds or fails as a whole.
	// Empty means groups.
	Unit string
}

// Units is Unit with its default applied: what FailedGroups and TotalGroups
// count. Exported because the JSON body reports the counts and a client
// parsing them cannot otherwise tell groups from shard writers.
func (e *WriteError) Units() string {
	if e.Unit == "" {
		return "groups"
	}
	return e.Unit
}

func (e *WriteError) Error() string {
	if e.TotalGroups == 0 {
		// Nothing was handed to the pool -- a closed writer, an empty flush.
		// "0 of 0 groups failed" is noise around a message that already says
		// what happened.
		return e.Err.Error()
	}
	if e.Partial {
		return fmt.Sprintf("ingest: %d of %d %s failed to store (partial; a retry may duplicate records): %v",
			e.FailedGroups, e.TotalGroups, e.Units(), e.Err)
	}
	return fmt.Sprintf("ingest: %d of %d %s failed to store: %v",
		e.FailedGroups, e.TotalGroups, e.Units(), e.Err)
}

func (e *WriteError) Unwrap() error { return e.Err }

// Retryable reports whether another attempt can succeed.
//
// Always true today, and the reason there is no never-retry class is worth
// stating rather than leaving as an absence.
//
// There was one: a group that failed its own bounds and checksum checks the
// instant after being written was classified never-retry, on the reasoning
// that the bytes are a pure function of the payload so every attempt
// reproduces it. That reasoning is not sound. ReadGroup validates a CRC32C
// over a blob that was just handed to the filesystem, and a mismatch there is
// at least as likely to be the storage returning different bytes than the ones
// written -- which is not deterministic and IS fixed by a retry, or by
// replacing the disk. Answering it 500 with retryable=false told the shipper
// to drop data that a media error had corrupted.
//
// It also inverted this file's own stated bias: an unrecognised failure
// defaults to retryable because telling a client to give up on something
// transient loses data, while a needless retry only duplicates it. So the
// class is gone rather than misapplied. If a genuinely deterministic write
// failure turns up, it comes back with the evidence that made it deterministic.
func (e *WriteError) Retryable() bool { return true }

// DuplicatesOnRetry reports whether retrying the same payload can store some
// of its records a second time. True exactly when the failure was partial:
// with every group in the window failed, nothing landed and a retry is clean.
func (e *WriteError) DuplicatesOnRetry() bool { return e.Partial }

// RetryAfter is how long a client should wait, or 0 when it should not retry.
func (e *WriteError) RetryAfter() time.Duration {
	switch e.Class {
	case RetryAfterRepair:
		return retryAfterRepair
	default:
		return retryAfterSoon
	}
}

// HTTPStatus is 503. Every write failure this server can produce is either
// transient or needs an operator; see Retryable for why there is no third
// answer.
func (e *WriteError) HTTPStatus() int { return http.StatusServiceUnavailable }

// newWriteError classifies a flush failure. failed and total are the flush
// jobs in the caller's window.
func newWriteError(err error, failed, total int) *WriteError {
	return &WriteError{
		Err:          err,
		Class:        classify(err),
		Partial:      failed > 0 && failed < total,
		FailedGroups: failed,
		TotalGroups:  total,
	}
}

// classify maps a storage failure to what a client should do about it.
//
// The errno is where the information is, and it survives: os wraps the raw
// syscall.Errno, so errors.Is reaches it through however many layers of
// fmt.Errorf sit in between.
//
// The default is RetrySoon rather than the longer interval. An unrecognised
// failure is one this classification has not met yet, and making a client wait
// thirty seconds for something that had already cleared costs a shipper its
// buffer.
func classify(err error) RetryClass {
	switch {
	case err == nil:
		return RetrySoon
	case errors.Is(err, ErrWriterClosed):
		// The writer is going away, but the STORE is not: a restarted server
		// accepts the same payload. Shutdown is measured in seconds.
		return RetrySoon
	case errors.Is(err, storage.ErrCorruptGroup):
		// A group that will not read back the instant after it was written
		// means the bytes that came out are not the bytes that went in. That
		// is a filesystem or a disk, not a payload, and it needs an operator
		// before the next attempt -- see Retryable for why this is not a
		// never-retry.
		return RetryAfterRepair
	}
	if c, ok := classifyErrno(err); ok {
		return c
	}
	if errors.Is(err, fs.ErrPermission) {
		return RetryAfterRepair
	}
	return RetrySoon
}

// classifyErrno is the errno half, kept separate because the set of defined
// errno values is not the same on every platform this builds for.
func classifyErrno(err error) (RetryClass, bool) {
	switch {
	case errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.EROFS),
		errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		// Nobody fixes these by trying again in a second. A full disk needs
		// space freed or retention run; a read-only or unwritable directory
		// needs an operator.
		return RetryAfterRepair, true
	case errors.Is(err, syscall.EIO):
		// Possibly a dying disk, possibly a transient bus error. Treated as
		// needing attention, because if it is the first the retry loop is
		// what turns a failing write into a failing service.
		return RetryAfterRepair, true
	case errors.Is(err, syscall.EMFILE), errors.Is(err, syscall.ENFILE),
		errors.Is(err, syscall.ENOMEM), errors.Is(err, syscall.EINTR),
		errors.Is(err, syscall.EAGAIN):
		// Resource pressure that clears on its own.
		return RetrySoon, true
	}
	return 0, false
}

// ErrDurabilityUnknown reports that a caller's mark is older than anything the
// writer still remembers, so it cannot say whether those rows were stored.
//
// It exists because the alternative was a nil. FlushMark answered from a
// 64-entry ring of batches; once a caller's batch aged out of it -- which 64
// flushes from any other request on the tenant would do -- the question went
// unanswered and the caller was told success. A retained outcome log makes
// that essentially unreachable, and this is what happens past even that: the
// writer says it does not know, which a caller must treat as a possible
// failure. A wrong nil is the thing this whole mechanism exists to prevent.
var ErrDurabilityUnknown = errors.New("ingest: the writer no longer knows whether these rows were stored")
