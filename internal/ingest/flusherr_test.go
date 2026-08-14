package ingest

import (
	"os"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A flush error must be reported once, not forever.
//
// flushErr was set with a CompareAndSwap from nil and nothing ever reset it,
// so one transient storage failure -- a full disk, a directory that briefly
// was not writable -- made every later Flush return that same error for the
// life of the writer. The rows of those later flushes were stored anyway, so
// the caller got a failure for a write that succeeded; a shipper that
// retries on failure duplicated its data indefinitely. The server's default
// tenant is never evicted, so its writer was never recreated either.
func TestFlushErrorIsReportedOnceNotForever(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	defer w.Close()

	add := func(ts int64) {
		t.Helper()
		if _, err := IngestJSONLines(w,
			[]byte(`{"_time":`+itoa(ts)+`,"a":"b"}`+"\n"),
			func() int64 { return ts }); err != nil {
			t.Fatal(err)
		}
	}

	// Remove the directory out from under the store rather than chmod it:
	// AppendGroup then fails with ENOENT for any user, on any filesystem.
	// The chmod version skipped for uid 0, which is every CI container.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	mark := w.Mark()
	add(1700000000000000001)
	if firstErr := w.FlushMark(mark); firstErr == nil {
		t.Fatal("a flush into a removed directory reported success")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Storage is healthy again. Every later flush must report success.
	for i := 0; i < 3; i++ {
		m := w.Mark()
		add(1700000000000000002 + int64(i))
		if err := w.FlushMark(m); err != nil {
			t.Fatalf("retry %d still reports the first failure: %v", i+1, err)
		}
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
