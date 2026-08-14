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
	MaxOpenTenants     int
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
	StreamFields []string
	Compact      bool
}

// Default returns a Config with production limits and no data directory.
func Default() Config { return Config{Limits: DefaultLimits()} }

// Validate normalizes the limits and checks the rest.
func (c *Config) Validate() error {
	if c.Dir == "" {
		return fmt.Errorf("config: no data directory")
	}
	return c.Limits.Normalize()
}
