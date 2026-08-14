package ingest

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
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
