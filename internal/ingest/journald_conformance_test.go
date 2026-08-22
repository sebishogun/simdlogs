package ingest

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/query"
)

// systemd journal export conformance.
//
// The format is what systemd-journal-upload sends: entries are blocks of
// fields separated by a blank line, and a field is either `NAME=value\n` or
// `NAME\n` followed by a little-endian uint64 length and that many RAW bytes.
// The binary form exists so a value can contain newlines, which is why this
// parses bytes rather than lines.
//
// The defect this pins: a malformed length DISCARDED THE REST OF THE UPLOAD.
// Both the "fewer than 8 bytes of length prefix" and the "length exceeds what
// remains" branches set the cursor to the end and fell out of the loop with no
// rejection count and no warning, so every entry after the bad field was lost
// and the request was answered 202. IngestJournald could not return a failure
// at all, which is why the listener's error handling for it was unreachable.

// jfield builds a text field.
func jfield(name, val string) string { return name + "=" + val + "\n" }

// jbinary builds a binary field with a correct length prefix.
func jbinary(name string, val []byte) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('\n')
	var ln [8]byte
	binary.LittleEndian.PutUint64(ln[:], uint64(len(val)))
	b.Write(ln[:])
	b.Write(val)
	b.WriteByte('\n')
	return b.String()
}

// jbinaryDeclaring builds a binary field whose declared length is a lie.
func jbinaryDeclaring(name string, declared uint64, val []byte) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('\n')
	var ln [8]byte
	binary.LittleEndian.PutUint64(ln[:], declared)
	b.Write(ln[:])
	b.Write(val)
	return b.String()
}

func jRows(t *testing.T, body string) ([]string, Result, error) {
	t.Helper()
	st := openTestStore(t)
	w := NewWriter(st)
	res, err := IngestJournaldOpts(w, []byte(body), func() int64 { return 1 }, nil)
	w.Close()
	return storeRows(t, st), res, err
}

// The shapes a real upload carries.
func TestJournaldWellFormed(t *testing.T) {
	entry := jfield("__REALTIME_TIMESTAMP", "1714521600000000") +
		jfield("MESSAGE", "payment declined") +
		jfield("_HOSTNAME", "h1") +
		jfield("PRIORITY", "3")

	rows, res, err := jRows(t, entry+"\n")
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if res.Accepted != 1 {
		t.Fatalf("accepted %d, want 1", res.Accepted)
	}
	got := fieldsOfRow(rows[0])
	for k, v := range map[string]string{
		"_msg": "payment declined", "hostname": "h1", "priority": "3",
	} {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

// A binary field's value may contain newlines -- the whole reason the binary
// form exists -- and must not be split.
func TestJournaldBinaryFieldWithNewlines(t *testing.T) {
	multi := []byte("line one\nline two\nline three")
	body := jfield("__REALTIME_TIMESTAMP", "1714521600000000") +
		jbinary("MESSAGE", multi) +
		jfield("_HOSTNAME", "h1") + "\n"

	rows, res, err := jRows(t, body)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if res.Accepted != 1 {
		t.Fatalf("accepted %d, want 1 -- a binary value's newlines split the entry", res.Accepted)
	}
	if got := fieldsOfRow(rows[0])["_msg"]; got != string(multi) {
		t.Errorf("_msg = %q, want the whole multi-line value", got)
	}
}

// A malformed binary length is REPORTED. The entries before it are kept -- they
// parsed -- and the caller is told the rest is unreadable rather than being
// answered success for data that was discarded.
func TestJournaldMalformedBinaryLengthIsReported(t *testing.T) {
	good := jfield("__REALTIME_TIMESTAMP", "1714521600000000") +
		jfield("MESSAGE", "kept") + "\n"

	for _, tc := range []struct {
		name string
		bad  string
	}{
		{
			// Fewer than the 8 bytes of length prefix.
			name: "truncated length prefix",
			bad:  "MESSAGE\n\x01\x02\x03",
		},
		{
			// A length larger than what remains.
			name: "length exceeds the remainder",
			bad:  jbinaryDeclaring("MESSAGE", 1<<20, []byte("only a few bytes")),
		},
		{
			// The largest possible declared length: must be refused on the
			// comparison, never narrowed to int first.
			name: "length is 2^64-1",
			bad:  jbinaryDeclaring("MESSAGE", ^uint64(0), []byte("x")),
		},
		{
			name: "length is 2^63",
			bad:  jbinaryDeclaring("MESSAGE", 1<<63, []byte("x")),
		},
		{
			// A name that ends at EOF with no separator.
			name: "field name cut at EOF",
			bad:  "MESSAG",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, res, err := jRows(t, good+tc.bad)
			if err == nil {
				t.Errorf("no error for %s: the rest of the upload was discarded silently", tc.name)
			}
			if res.Rejected == 0 {
				t.Errorf("rejected 0 for %s: nothing counted what was dropped", tc.name)
			}
			if len(res.Warnings) == 0 {
				t.Errorf("no warning for %s: the operator cannot see what happened", tc.name)
			}
			// The entry BEFORE the malformed field parsed and is kept.
			if len(rows) < 1 {
				t.Errorf("the good entry before the malformed field was lost")
			} else if got := fieldsOfRow(rows[0])["_msg"]; got != "kept" {
				t.Errorf("_msg = %q, want kept", got)
			}
		})
	}
}

// Everything AFTER a malformed length is unreadable, and the count says so
// rather than the parser pretending the upload ended cleanly.
func TestJournaldTruncationDoesNotSilentlySwallowLaterEntries(t *testing.T) {
	var b strings.Builder
	// Three good entries, then a bad length, then three more that can never
	// be reached.
	for i := 0; i < 3; i++ {
		b.WriteString(jfield("__REALTIME_TIMESTAMP", fmt.Sprintf("171452160000000%d", i)))
		b.WriteString(jfield("MESSAGE", fmt.Sprintf("good %d", i)))
		b.WriteString("\n")
	}
	b.WriteString(jbinaryDeclaring("MESSAGE", 1<<30, []byte("short")))
	for i := 0; i < 3; i++ {
		b.WriteString(jfield("MESSAGE", fmt.Sprintf("unreachable %d", i)))
		b.WriteString("\n")
	}

	rows, res, err := jRows(t, b.String())
	if err == nil {
		t.Fatal("no error: three unreachable entries were discarded under a success")
	}
	if res.Accepted != 3 {
		t.Errorf("accepted %d, want the 3 entries that parsed before the bad field", res.Accepted)
	}
	if len(rows) != 3 {
		t.Errorf("stored %d rows, want 3", len(rows))
	}
	for _, r := range rows {
		if strings.Contains(fieldsOfRow(r)["_msg"], "unreachable") {
			t.Errorf("an entry after the malformed length was stored: %q", r)
		}
	}
}

// An entry carrying a timestamp and nothing else is rejected and reported. It
// used to be dropped with no count, so a sender saw a 202 for records that
// were not there.
func TestJournaldEntryWithNoStorableFieldIsReported(t *testing.T) {
	body := jfield("__REALTIME_TIMESTAMP", "1714521600000000") + "\n" +
		jfield("__REALTIME_TIMESTAMP", "1714521600000001") +
		jfield("MESSAGE", "kept") + "\n"

	rows, res, err := jRows(t, body)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if res.Accepted != 1 {
		t.Errorf("accepted %d, want 1", res.Accepted)
	}
	if res.Rejected != 1 {
		t.Errorf("rejected %d, want 1 (the timestamp-only entry)", res.Rejected)
	}
	if len(rows) != 1 || fieldsOfRow(rows[0])["_msg"] != "kept" {
		t.Errorf("stored %v, want just the good entry", rows)
	}
}

// A zero-length binary field is legal and carries an empty value.
func TestJournaldZeroLengthBinaryField(t *testing.T) {
	body := jfield("__REALTIME_TIMESTAMP", "1714521600000000") +
		jbinary("MESSAGE", nil) +
		jfield("_HOSTNAME", "h1") + "\n"

	rows, res, err := jRows(t, body)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if res.Accepted != 1 {
		t.Fatalf("accepted %d, want 1", res.Accepted)
	}
	if got := fieldsOfRow(rows[0])["hostname"]; got != "h1" {
		t.Errorf("the field after a zero-length binary value was lost: hostname = %q", got)
	}
}

// The name mapping: __REALTIME_TIMESTAMP is the time, MESSAGE is _msg, and a
// leading underscore is stripped and the rest lowercased.
func TestJournaldFieldNameMapping(t *testing.T) {
	body := jfield("__REALTIME_TIMESTAMP", "1714521600000000") +
		jfield("MESSAGE", "m") +
		jfield("_SYSTEMD_UNIT", "nginx.service") +
		jfield("SYSLOG_IDENTIFIER", "nginx") + "\n"

	rows, _, err := jRows(t, body)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	got := fieldsOfRow(rows[0])
	for k, v := range map[string]string{
		"_msg": "m", "systemd_unit": "nginx.service", "syslog_identifier": "nginx",
	} {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	// The timestamp is consumed, not stored as a field.
	if _, ok := got["_realtime_timestamp"]; ok {
		t.Error("__REALTIME_TIMESTAMP was stored as a field as well as consumed")
	}
}

// ONE RECORD IS ONE COUNT, and a truncated field used to make it two.
//
// The `default` branch of the field scan -- a name that ends at EOF with
// neither '=' nor a newline -- called `res.Reject(ordinal)` and then `emit()`,
// and emit rejects the SAME ordinal again through either of its two refusal
// branches. 29 bytes reach it. `make fuzz` runs this shape through
// `oneEnvelope`, whose "rejected positions are not increasing" check is the
// one that fires, and on the wire /insert/journald answered
// `{"accepted":0,"rejected":2}` for a ONE-record upload.
//
// The rows below are the three ways into emit's refusal branches plus the one
// where the entry had storable fields, which double-counted in the other
// direction: Accepted=1 AND Rejected=1 for the same record.
func TestJournaldATruncatedFieldRejectsTheEntryOnce(t *testing.T) {
	for _, tc := range []struct {
		name, body         string
		accepted, rejected int
	}{
		{
			"a timestamp and a truncated field",
			jfield("__REALTIME_TIMESTAMP", "1") + "ORPHAN",
			0, 1,
		},
		{
			"storable fields and a truncated field",
			jfield("__REALTIME_TIMESTAMP", "1") + jfield("MESSAGE", "x") + "ORPHAN",
			0, 1,
		},
		{
			"an out-of-range timestamp and a truncated field",
			jfield("__REALTIME_TIMESTAMP", "99999999999999999999") + "ORPHAN",
			0, 1,
		},
		{
			"no timestamp and a truncated field",
			jfield("MESSAGE", "x") + "ORPHAN",
			0, 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, res, err := jRows(t, tc.body)
			if err == nil {
				t.Fatal("a truncated upload answered success")
			}
			if res.Accepted != tc.accepted || res.Rejected != tc.rejected {
				t.Errorf("accepted %d rejected %d, want %d and %d: this upload holds ONE record",
					res.Accepted, res.Rejected, tc.accepted, tc.rejected)
			}
			if len(res.RejectedAt) != res.Rejected {
				t.Errorf("RejectedAt %v against Rejected %d", res.RejectedAt, res.Rejected)
			}
			// The envelope invariant `make fuzz` enforces: strictly increasing.
			prev := int32(-1)
			for _, at := range res.RejectedAt {
				if at <= prev {
					t.Fatalf("rejected positions are not increasing: %v", res.RejectedAt)
				}
				prev = at
			}
			if res.Accepted != len(rows) {
				t.Errorf("reported %d accepted and stored %d rows", res.Accepted, len(rows))
			}
		})
	}
}

// A SIGNED __REALTIME_TIMESTAMP IS THE CLIENT'S TIMESTAMP, NOT A MISSING ONE.
//
// The byte scan that replaced ParseInt tested every byte for '0'..'9', so `-1`
// and `+5` -- both of which ParseInt read -- became "not a decimal count",
// which is the one branch that falls back to the RECEIVER'S CLOCK. The entry
// was then stored at ingest time under a 202 with no rejection and no warning:
// a fabricated instant, which is exactly what the range arm was added to stop.
func TestJournaldASignedTimestampIsNotStampedWithTheReceiversClock(t *testing.T) {
	const clock = int64(1_700_000_000_000_000_000) // the fallback, far from any row below
	at := func(t *testing.T, body string) (int64, Result, error) {
		t.Helper()
		st := openTestStore(t)
		w := NewWriter(st)
		res, err := IngestJournaldOpts(w, []byte(body), func() int64 { return clock }, nil)
		w.Close()
		q, perr := query.ParseLogsQL("*")
		if perr != nil {
			t.Fatal(perr)
		}
		q.From, q.To, q.MatAll = math.MinInt64, int64(1)<<62, true
		rows := query.RunPipeline(st, q)
		if len(rows) != 1 {
			return 0, res, err
		}
		return rows[0].Time, res, err
	}

	for _, tc := range []struct {
		name, ts string
		wantNs   int64
	}{
		{"a negative microsecond count", "-1", -1000},
		{"an explicit plus", "+5", 5000},
		{"unsigned, the control", "5", 5000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns, res, err := at(t, jfield("__REALTIME_TIMESTAMP", tc.ts)+jfield("MESSAGE", "m")+"\n")
			if err != nil {
				t.Fatalf("ingest: %v", err)
			}
			if res.Accepted != 1 {
				t.Fatalf("accepted %d, want 1", res.Accepted)
			}
			if ns == clock {
				t.Fatalf("__REALTIME_TIMESTAMP=%s was stamped with the receiver's clock; "+
					"the client sent a timestamp and the store recorded an instant nobody sent", tc.ts)
			}
			if ns != tc.wantNs {
				t.Errorf("__REALTIME_TIMESTAMP=%s stored %d ns, want %d", tc.ts, ns, tc.wantNs)
			}
		})
	}

	// A signed value too large to convert is REFUSED, not fallen back on --
	// the same treatment the unsigned arm gives 2^63.
	t.Run("a negative count past the domain", func(t *testing.T) {
		_, res, err := at(t, jfield("__REALTIME_TIMESTAMP", "-99999999999999999999")+
			jfield("MESSAGE", "m")+"\n")
		if err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if res.Accepted != 0 || res.Rejected != 1 {
			t.Fatalf("accepted %d rejected %d, want 0 and 1", res.Accepted, res.Rejected)
		}
	})

	// A bare sign is not a count: that IS the fall-back case, and it must stay
	// one rather than becoming a rejection.
	t.Run("a bare sign is not a timestamp", func(t *testing.T) {
		ns, res, err := at(t, jfield("__REALTIME_TIMESTAMP", "-")+jfield("MESSAGE", "m")+"\n")
		if err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if res.Accepted != 1 || res.Rejected != 0 {
			t.Fatalf("accepted %d rejected %d, want 1 and 0", res.Accepted, res.Rejected)
		}
		if ns != clock {
			t.Errorf("stored %d, want the receiver's clock %d", ns, clock)
		}
	})
}
