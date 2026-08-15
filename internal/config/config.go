// Package config holds the server's typed configuration. It exists so that
// every bound in the system has a name, a default, and one place to be
// validated -- rather than being a literal at a call site or, as several
// were, absent entirely.
package config

import (
	"fmt"
	"time"
)

// Limits bounds everything a request can consume. Every field is finite by
// default.
//
// Zero does not mean unlimited. It used to: search.maxRows defaulted to 0 and
// that was read as "no cap", so one query could materialize an entire store.
// A zero here means "use the default", and a caller that genuinely wants no
// bound has to say so with Unlimited, which is loud at the call site and
// greppable in a config file.
type Limits struct {
	// Ingest.
	MaxBodyBytes       int64 // compressed request body
	MaxDecompressed    int64 // after gzip; bounds a decompression bomb
	MaxLineBytes       int   // one NDJSON line / syslog frame
	MaxFieldsPerRecord int
	MaxFieldNameBytes  int
	MaxFieldValueBytes int

	// Query.
	MaxQueryRows     int
	MaxQueryBytes    int64
	MaxQueryDuration time.Duration

	// Concurrency and tenancy.
	MaxConcurrentQuery int
	MaxConcurrentWrite int
	// MaxConcurrentTail bounds live tails. They are not charged the query
	// budget -- an open tail is an idle connection, and charging it meant a
	// few of them returned 429 for every other read including /metrics --
	// but "not charged" is not "unbounded": each one holds a connection, a
	// goroutine and a poll timer, and with no bound an anonymous client
	// opened as many as it liked.
	MaxConcurrentTail int
	MaxOpenTenants    int

	// MaxQueriesPerTenant bounds reads in flight for ONE tenant.
	// MaxConcurrentQuery is process-wide and cannot express this: with only
	// that, the tenant with the most aggressive dashboard takes every slot and
	// every other tenant is refused work the server had room to do. 0 is
	// unbounded.
	MaxQueriesPerTenant int

	// QueryQueueWait is how long a read may wait for a slot before being
	// refused. 0 refuses immediately, which is right for an interactive
	// endpoint: a client that would have waited would rather be told now, and
	// a queue deeper than the client's patience does work nobody collects.
	QueryQueueWait time.Duration

	// MaxGroupKeys bounds an aggregate's distinct `by` keys. Nothing else
	// measures it: MaxQueryRows counts the scan's rows, of which a
	// high-cardinality aggregate may read few, and MaxQueryBytes counts
	// materialized row bytes, which an aggregate does not accumulate. The map
	// it builds is proportional to the key space and to nothing else. 0 is
	// unbounded.
	MaxGroupKeys int

	// MaxPipeRows bounds the rows one pipe may produce. Joins are why: a left
	// join on a key that is not unique on the right multiplies, so two results
	// each inside MaxQueryRows become an output no budget covered. 0 is
	// unbounded.
	MaxPipeRows int

	// MaxScanWorkers bounds the goroutines every concurrent scan draws from,
	// in total rather than each. 0 means GOMAXPROCS, which is the right number
	// for the machine and was previously the number taken by each query.
	MaxScanWorkers int
}

// Storage is the disk budget. The zero value enforces nothing, which is the
// behaviour a deployment had before these fields existed.
//
// Bytes to keep free rather than a percentage used: a percentage is the wrong
// unit for what is being protected. 5% of a 40 TB array is 2 TB of slack
// nobody needs; 5% of a 20 GB volume is less than one large group plus the
// manifest rewrite that follows it. What matters is that the RECOVERY -- a
// retention pass, which has to write a manifest record before it can unlink
// anything -- still has room.
type Storage struct {
	// ReserveWarnBytes degrades readiness while still accepting writes.
	ReserveWarnBytes int64
	// ReserveRejectBytes refuses new writes. Must be below the warn level.
	ReserveRejectBytes int64
	// MaxTenantBytes bounds one tenant's own bytes on disk.
	MaxTenantBytes int64
}

// Unlimited is the explicit opt-out for a numeric bound. It is negative so it
// cannot be reached by accident: a forgotten field is zero, which means
// "default", and an overflowing computation is positive.
const Unlimited = -1

// DefaultLimits are the production defaults. They are deliberately generous
// enough that no reasonable deployment trips them, and finite enough that a
// hostile or broken client cannot exhaust the process.
func DefaultLimits() Limits {
	return Limits{
		MaxBodyBytes:       64 << 20,  // 64 MiB
		MaxDecompressed:    512 << 20, // 512 MiB: 8x the body limit
		MaxLineBytes:       1 << 20,   // 1 MiB
		MaxFieldsPerRecord: 1024,
		MaxFieldNameBytes:  1 << 10,
		MaxFieldValueBytes: 1 << 20,

		MaxQueryRows:     1_000_000,
		MaxQueryBytes:    256 << 20,
		MaxQueryDuration: 30 * time.Second,

		MaxConcurrentQuery: 32,
		MaxConcurrentWrite: 32,
		MaxConcurrentTail:  64,
		MaxOpenTenants:     1024,
	}
}

// TestLimits are small deterministic bounds for tests that want to observe a
// limit being hit without building a large body.
func TestLimits() Limits {
	l := DefaultLimits()
	l.MaxBodyBytes = 1 << 16
	l.MaxDecompressed = 1 << 18
	l.MaxLineBytes = 4096
	l.MaxQueryRows = 1000
	l.MaxQueryBytes = 1 << 20
	l.MaxQueryDuration = 2 * time.Second
	l.MaxConcurrentQuery = 4
	l.MaxConcurrentWrite = 4
	l.MaxConcurrentTail = 4
	l.MaxOpenTenants = 8
	return l
}

// field describes one limit for the shared default/validate walk, so a new
// limit cannot be added to the struct and forgotten by both.
type field struct {
	name string
	get  func(*Limits) int64
	set  func(*Limits, int64)
}

func fields() []field {
	return []field{
		{"max-body-bytes", func(l *Limits) int64 { return l.MaxBodyBytes }, func(l *Limits, v int64) { l.MaxBodyBytes = v }},
		{"max-decompressed-bytes", func(l *Limits) int64 { return l.MaxDecompressed }, func(l *Limits, v int64) { l.MaxDecompressed = v }},
		{"max-line-bytes", func(l *Limits) int64 { return int64(l.MaxLineBytes) }, func(l *Limits, v int64) { l.MaxLineBytes = int(v) }},
		{"max-fields-per-record", func(l *Limits) int64 { return int64(l.MaxFieldsPerRecord) }, func(l *Limits, v int64) { l.MaxFieldsPerRecord = int(v) }},
		{"max-field-name-bytes", func(l *Limits) int64 { return int64(l.MaxFieldNameBytes) }, func(l *Limits, v int64) { l.MaxFieldNameBytes = int(v) }},
		{"max-field-value-bytes", func(l *Limits) int64 { return int64(l.MaxFieldValueBytes) }, func(l *Limits, v int64) { l.MaxFieldValueBytes = int(v) }},
		{"max-query-rows", func(l *Limits) int64 { return int64(l.MaxQueryRows) }, func(l *Limits, v int64) { l.MaxQueryRows = int(v) }},
		{"max-query-bytes", func(l *Limits) int64 { return l.MaxQueryBytes }, func(l *Limits, v int64) { l.MaxQueryBytes = v }},
		{"max-concurrent-query", func(l *Limits) int64 { return int64(l.MaxConcurrentQuery) }, func(l *Limits, v int64) { l.MaxConcurrentQuery = int(v) }},
		{"max-concurrent-write", func(l *Limits) int64 { return int64(l.MaxConcurrentWrite) }, func(l *Limits, v int64) { l.MaxConcurrentWrite = int(v) }},
		{"max-concurrent-tail", func(l *Limits) int64 { return int64(l.MaxConcurrentTail) }, func(l *Limits, v int64) { l.MaxConcurrentTail = int(v) }},
		{"max-open-tenants", func(l *Limits) int64 { return int64(l.MaxOpenTenants) }, func(l *Limits, v int64) { l.MaxOpenTenants = int(v) }},
	}
}

// Normalize fills zero fields from the defaults and returns an error for any
// value that is neither positive nor Unlimited.
//
// A negative other than Unlimited is rejected rather than clamped: it is a
// typo or a bad computation, and silently turning -5 into a default hides the
// bug until the limit matters.
func (l *Limits) Normalize() error {
	d := DefaultLimits()
	for _, f := range fields() {
		v := f.get(l)
		switch {
		case v == 0:
			f.set(l, f.get(&d))
		case v < 0 && v != Unlimited:
			return fmt.Errorf("config: %s is %d; use a positive value or config.Unlimited", f.name, v)
		}
	}
	switch {
	case l.MaxQueryDuration == 0:
		l.MaxQueryDuration = d.MaxQueryDuration
	case l.MaxQueryDuration < 0 && l.MaxQueryDuration != time.Duration(Unlimited):
		return fmt.Errorf("config: max-query-duration is %s; use a positive value or config.Unlimited", l.MaxQueryDuration)
	}
	if l.MaxDecompressed != Unlimited && l.MaxBodyBytes != Unlimited && l.MaxDecompressed < l.MaxBodyBytes {
		return fmt.Errorf("config: max-decompressed-bytes (%d) is below max-body-bytes (%d); an uncompressed body would be rejected after being accepted",
			l.MaxDecompressed, l.MaxBodyBytes)
	}
	return nil
}

// BodyLimit is MaxBodyBytes as an absolute cap, or -1 when unlimited, in the
// form http.MaxBytesReader wants.
func (l Limits) BodyLimit() int64 { return l.MaxBodyBytes }

// Config is the whole server configuration.
type Config struct {
	Dir          string
	Limits       Limits
	Storage      Storage
	StreamFields []string
	Compact      bool

	// CorruptionPolicy is what a tenant store does with a committed group it
	// cannot read: "fail" (the default) refuses to open it, "quarantine" moves
	// the group aside and opens degraded.
	//
	// A string rather than the storage package's enum, because config must not
	// depend on storage -- the dependency runs the other way -- and because
	// this value comes from a flag or a file, where it is a string anyway. The
	// server parses it once at startup, so a typo is a startup failure and not
	// a silent fall back to the default.
	CorruptionPolicy string

	// DirRereadInterval is how often the readiness probe re-reads the store
	// directories of degraded tenants that are not open, to notice that an
	// operator has dealt with the evidence.
	//
	// Zero means the built-in default (250ms); negative means every call. A
	// deployment probing every 30 seconds can afford a larger window, and one
	// running a recovery drill wants no window at all.
	DirRereadInterval time.Duration
}

// Default returns a Config with production limits and no data directory.
func Default() Config { return Config{Limits: DefaultLimits()} }

// Validate normalizes the limits and checks the rest.
func (c *Config) Validate() error {
	if c.Dir == "" {
		return fmt.Errorf("config: no data directory")
	}
	// The corruption policy is NOT checked here at all.
	//
	// storage.ParseCorruptionPolicy owns the accepted set, and config must not
	// import storage -- the dependency runs the other way. Two checks meant
	// two answers: first they duplicated the set, so a third policy would have
	// been accepted by one and rejected by the other; then a shape check here
	// rejected the surrounding whitespace the parser deliberately trims, so
	// `-corruption-policy=" quarantine "` was a startup error for a value
	// storage considers valid.
	//
	// NewServerConfig calls Validate and then the parser, so an unknown policy
	// is still a startup failure -- from the one place that knows the set.
	return c.Limits.Normalize()
}
