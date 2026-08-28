package ingest

import (
	"encoding/binary"
	"errors"
	"math"
	"strings"
)

// IngestJournald parses the systemd journal export format (what
// systemd-journal-upload sends): entries are blocks of fields separated by a
// blank line. A field is either `NAME=value\n` (text) or `NAME\n` followed by
// a little-endian uint64 length and that many raw bytes then a newline
// (binary, so a value may contain newlines -- which is why this parses bytes,
// not lines). __REALTIME_TIMESTAMP (microseconds) sets the time, MESSAGE
// becomes _msg, and other names are lowercased with a leading underscore
// stripped (_HOSTNAME -> hostname).
func IngestJournald(w *Writer, data []byte, fallback func() int64) (Result, error) {
	return IngestJournaldOpts(w, data, fallback, nil)
}

// IngestJournaldOpts is IngestJournald with the request's field mappings applied.
func IngestJournaldOpts(w *Writer, data []byte, fallback func() int64, opts *Options) (Result, error) {
	var res Result
	mapped := !opts.Empty()
	fields := map[string]string{}
	var ts int64
	haveTS := false
	// A __REALTIME_TIMESTAMP that parses and cannot be stored refuses the
	// ENTRY, the way every other protocol in this package refuses it. It is
	// cleared by reset() with the rest of the entry's state.
	var tsErr error
	// The RECORD ordinal, for Result.RejectedAt. Distinct from Warning.Offset,
	// which is a BYTE offset -- the two fields answer different questions and
	// the sites below use each for its own. Four rejections here were recorded
	// with no position at all, so a client mapping them onto its own batch had
	// nothing to map.
	ordinal := 0
	reset := func() {
		for k := range fields {
			delete(fields, k)
		}
		ts, haveTS, tsErr = 0, false, nil
	}
	emit := func() {
		if tsErr != nil {
			// See ErrTimeOutOfRange. `us*1000` overflowed for any journal
			// timestamp past 9.2e15 microseconds -- the year 2262 -- and the
			// wrapped product filed the entry in the distant past at HTTP 200,
			// counted as ingested. systemd's own field is an unsigned 64-bit
			// microsecond count, so every value above that bound is a legal
			// journal value and not a malformed one.
			res.Reject(ordinal)
			res.WarnAt(ordinal, "%v", tsErr)
			ordinal++
			reset()
			return
		}
		if len(fields) == 0 {
			// An entry whose fields all failed to store -- most often one
			// carrying only a __REALTIME_TIMESTAMP -- was dropped with no
			// count, so a sender saw a 202 for records that were not there.
			if haveTS {
				res.Reject(ordinal)
				// The BYTE offset is unknown here: the entry ended at a blank
				// line and its start is not carried. The RECORD ordinal is
				// known, and it is what a client matches its batch against.
				res.WarnAt(ordinal, "entry carries a timestamp and no storable field")
				ordinal++
			}
			reset()
			return
		}
		if !haveTS {
			ts = fallback()
		}
		if mapped {
			opts.apply(fields)
		}
		addOrReject(w, ts, fields, opts, &res, ordinal)
		ordinal++
		reset()
	}
	set := func(name string, val []byte) {
		if name == "__REALTIME_TIMESTAMP" {
			t, ok, terr := journalMicros(val)
			switch {
			case terr != nil:
				tsErr = terr
			case ok:
				ts, haveTS = t, true
			}
			return
		}
		key := strings.ToLower(strings.TrimLeft(name, "_"))
		if key == "message" {
			key = "_msg"
		}
		if key != "" {
			fields[key] = string(val)
		}
	}

	i, n := 0, len(data)
	for i < n {
		if data[i] == '\n' { // blank line: entry boundary
			emit()
			i++
			continue
		}
		ns := i
		for i < n && data[i] != '\n' && data[i] != '=' {
			i++
		}
		name := string(data[ns:i])
		switch {
		case i < n && data[i] == '=': // text field
			i++
			vs := i
			for i < n && data[i] != '\n' {
				i++
			}
			set(name, data[vs:i])
			if i < n {
				i++ // consume newline
			}
		case i < n && data[i] == '\n': // binary field: length-prefixed value
			i++
			// A malformed length DISCARDS THE REST OF THE UPLOAD, so it must
			// be reported. Both of these branches used to set i = n and fall
			// out of the loop with no rejection count and no warning: every
			// entry after the bad field was lost and the request answered
			// success. IngestJournald could not return a failure at all, which
			// is why the listener's error handling was unreachable.
			if i+8 > n {
				res.Reject(ordinal)
				res.Warn(int64(i), "binary field %q: %d bytes of length prefix, need 8; "+
					"the remainder of the upload is not parseable", name, n-i)
				return res, envelopeErr(errJournaldTruncated)
			}
			ln := binary.LittleEndian.Uint64(data[i : i+8])
			i += 8
			// Compared as uint64 against the REMAINING bytes, so a length near
			// 2^64 cannot wrap when it is narrowed to int on a 32-bit build.
			if ln > uint64(n-i) {
				res.Reject(ordinal)
				res.Warn(int64(i), "binary field %q declares %d bytes, %d remain; "+
					"the remainder of the upload is not parseable", name, ln, n-i)
				return res, envelopeErr(errJournaldTruncated)
			}
			set(name, data[i:i+int(ln)])
			i += int(ln)
			if i < n && data[i] == '\n' {
				i++
			}
		default:
			// A name that ends at EOF with neither '=' nor a newline: the
			// upload was cut mid-field. Same treatment -- reported, not
			// silently dropped.
			//
			// AND THE ENTRY IS REJECTED ONCE, NOT TWICE. This called emit()
			// after its own Reject, and emit rejects the same ordinal again
			// through whichever of its two refusal branches the entry lands
			// in -- the `haveTS && len(fields) == 0` one, or the tsErr one.
			// 29 bytes reach it: "__REALTIME_TIMESTAMP=1\nORPHAN" answered
			// Accepted=0, Rejected=2, RejectedAt=[0 0] for ONE record, which
			// is `{"accepted":0,"rejected":2}` on /insert/journald and a
			// "rejected positions are not increasing" failure in
			// FuzzIngestJournald's envelope check. With storable fields in
			// the entry it was worse: Accepted=1 AND Rejected=1 for the one
			// record, both counting the same thing.
			//
			// The two binary-field branches above already do it this way:
			// reject the entry at its ordinal and return, with no emit. A
			// truncated field is a truncated ENTRY, so its earlier fields do
			// not get stored on their own.
			if len(trimSpace(data[ns:])) > 0 {
				res.Reject(ordinal)
				res.Warn(int64(ns), "field %q ends without a value separator; "+
					"the upload is truncated", name)
				return res, envelopeErr(errJournaldTruncated)
			}
			i = n
		}
	}
	emit() // trailing entry with no closing blank line
	return res, nil
}

var errJournaldTruncated = errors.New("journal export is truncated")

// journalMicros reads systemd's __REALTIME_TIMESTAMP: an UNSIGNED decimal
// microsecond count since the epoch.
//
// THE SIGNED PARSE THREW AWAY THE HALF OF THE DOMAIN systemd CAN EMIT.
// It was `strconv.ParseInt(string(val), 10, 64)` guarded by `err == nil`, so
// two different out-of-range values took two different wrong paths and neither
// was reported:
//
//	value                    ParseInt          before            want
//	9223372036854775808      ErrRange          field ignored,    refused
//	  (2^63 us, year 294247)                   entry stamped
//	                                           with the
//	                                           RECEIVER'S CLOCK
//	9300000000000000         parses            refused (the      refused
//	  (year 2264)                              us*1000 check)
//
// The first is the fabrication `parseTime`'s own doc comment forbids -- "a row
// whose `_time` says 9999 is not a row with no timestamp, and stamping it
// `now` files it under an instant nobody sent just as surely as wrapping did"
// -- reached through the one field type where the value is legal on the wire.
// A journal export's field IS a uint64, so everything from 2^63 to 2^64-1 is a
// value systemd's own format can carry and ParseInt refuses every one of them.
// `parseTime` grew the same ErrRange arm in round 18; this file kept the
// fall-through.
//
// A BYTE SCAN, AND NOT BECAUSE OF ALLOCATIONS -- that argument was written
// here first and the measurement contradicted it. `strconv.ParseInt(string(val),
// 10, 64)` is allocation-free for this input: `go build -gcflags=-m` says
// "string(b) does not escape", so the conversion uses the compiler's 32-byte
// stack buffer, and both forms benchmark at 0 B/op, 0 allocs/op. What the scan
// buys is that it reads the bytes with no conversion at all and reports the
// three outcomes -- a count, not a number, out of range -- directly, instead of
// through an `errors.Is(err, strconv.ErrRange)` on an error value built to be
// thrown away. (A value longer than 32 digits would fall off the stack buffer;
// no journal emits one.)
//
// A LEADING SIGN IS READ, BECAUSE ParseInt READ ONE. The first byte scan
// tested every byte for `'0' <= c <= '9'`, so `-1` and `+5` -- both of which
// ParseInt accepted -- became "not a decimal count", and that answer is the
// one branch of this function that stamps the entry with the RECEIVER'S
// CLOCK. Measured through /insert/journald on `__REALTIME_TIMESTAMP=-1`:
//
//	                 ts stored                    counted
//	ParseInt          -1000 ns (1969-12-31)       accepted
//	first byte scan   the receiver's clock, 202   accepted, no warning
//
// That is the same fabrication the range arm was written to remove, reached
// through the one input shape the rewrite was supposed to leave alone. The
// magnitude bound is symmetric: MinInt64/1000 truncates toward zero to
// -9223372036854775, so the same maxUS bounds both signs.
//
// Returns (ns, ok, err). `ok=false` with a nil error means the value is not a
// decimal count at all, which is the outcome the caller already had for that
// case: no timestamp, and the entry falls back to the receiver's clock exactly
// as an entry with no __REALTIME_TIMESTAMP does. Only the RANGE case changed,
// because only the range case is a timestamp the client did send.
func journalMicros(b []byte) (int64, bool, error) {
	neg := false
	switch {
	case len(b) == 0:
		return 0, false, nil
	case b[0] == '-':
		neg, b = true, b[1:]
	case b[0] == '+':
		b = b[1:]
	}
	if len(b) == 0 {
		return 0, false, nil // a bare sign is not a count
	}
	// The largest microsecond magnitude whose product with 1000 is still an
	// int64. MinInt64/1000 truncates toward zero to -maxUS, so one bound
	// serves both signs.
	const maxUS = uint64(math.MaxInt64) / 1000
	var us uint64
	over := false
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false, nil // not a decimal count
		}
		d := uint64(c - '0')
		// Keep scanning past the bound rather than returning: "12345abc" is
		// not a number, and stopping early would report it as out of range.
		if over || us > (maxUS-d)/10 {
			over = true
			continue
		}
		us = us*10 + d
	}
	if over {
		return 0, false, tsRangeErr(neg, b)
	}
	ns := int64(us) * 1000
	if neg {
		ns = -ns
	}
	return ns, true, nil
}

// tsRangeErr builds the out-of-range refusal.
//
// IT IS A STRUCT BECAUSE `fmt.Errorf("%w: %s ...", ...)` MADE THE ONLY PATH
// THIS FUNCTION WAS REWRITTEN FOR THE SLOWEST OF THE THREE. Round 18's note
// said "both forms benchmark at 0 B/op, 0 allocs/op", which is true on the
// SUCCESS path and on that path only. Interleaved, one session, minimum of
// three, `-benchmem`:
//
//	input          ParseInt          scan + fmt.Errorf   scan + this struct
//	in-range       16.77 ns  0/0     12.51 ns  0/0       13.07 ns  0/0
//	out of range   53.09 ns 72B/2   146.0  ns 200B/3      38.44 ns 48B/2
//	non-numeric    37.15 ns 64B/2     1.565 ns  0/0        1.736 ns  0/0
//
// Three allocations for 200 bytes against ParseInt's two and 72: the boxed
// []byte argument, the formatted message, and the wrapper. The message is
// built in Error() instead, which the ingest path calls once per rejected
// entry through WarnAt -- so the cost is paid where the string is read rather
// than on every construction. (ns/op was measured on a machine at load
// average ~22; the allocation counts and byte totals do not depend on that.)
//
// Round 20: tsRangeError is SHARED with parseTime, which had the fmt.Errorf
// form this replaced -- 5 allocs / 264.5 B on the arm every out-of-range
// decimal `_time` in a _bulk reaches. The unit moved onto the struct, so this
// path is 2 allocs / 56 B rather than 2 / 40.
//
// SIXTEEN BYTES, NOT EIGHT. This first said "2 / 48 -> 2 / 56: eight bytes",
// which is the disclosed cost understated 2x. `unit` is a string HEADER --
// 16 bytes -- and it takes the struct from 16 to 32, which is size class 16
// to size class 32; the value string is a 24-byte class either way. Measured
// on BenchmarkJournalMicros's own out-of-range input, exact
// runtime.MemStats deltas over 200,000 constructions, three interleaved A/B
// pairs, identical across all of them and identical for both signs:
//
//	tsRangeErr(false, "9223372036854775808")   2 / 40.0 B -> 2 / 56.0 B
//	tsRangeErr(true,  "9223372036854775808")   2 / 40.0 B -> 2 / 56.0 B
//
// Sixteen bytes on the journald path buys parseTime two allocations and
// 160.5 bytes on every out-of-range `_time` a _bulk carries, which is the
// trade -- but the number a record discloses has to be the measured one.
func tsRangeErr(neg bool, digits []byte) error {
	sign := ""
	if neg {
		sign = "-"
	}
	return &tsRangeError{value: sign + string(digits), unit: "us"}
}
