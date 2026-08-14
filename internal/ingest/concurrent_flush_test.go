package ingest

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A tenant's Writer is shared by every request and by every syslog
// connection, so Flush is called concurrently as a matter of course.
//
// It used to be a single sync.WaitGroup reused across callers: one
// goroutine's Add ran while another's Wait was in progress, which is the
// documented misuse and panics with "WaitGroup is reused before previous
// Wait has returned". handleSyslogConn flushes after every line with no
// recover above it, so two syslog senders -- needing no credential at all --
// killed the process. Over HTTP the same shape produced 46 panics in 640
// concurrent posts to one tenant.
func TestConcurrentFlushDoesNotPanic(t *testing.T) {
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	defer w.Close()

	const goroutines, each = 16, 40
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				line := fmt.Sprintf(`{"_time":%d,"g":"%d"}`+"\n",
					1700000000000000000+int64(g*each+i), g)
				if _, err := IngestJSONLines(w, []byte(line), func() int64 { return 1 }); err != nil {
					t.Errorf("ingest: %v", err)
					return
				}
				if err := w.Flush(); err != nil {
					t.Errorf("flush: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// A failing store must fail EVERY concurrent flush, not one of them.
//
// The first fix for the sticky-error bug reported the error once and
// cleared it -- but the error slot was one per writer, so with concurrent
// flushes exactly one caller took it and every other was told success. Under
// a store whose directory had been removed, 25 to 30 of 32 concurrent
// flushes returned nil. At the HTTP layer each of those is a 200, and a
// row count, for rows that were never stored.
func TestConcurrentFlushOnABrokenStoreFailsEveryCaller(t *testing.T) {
	dir := t.TempDir()
	st, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	w := NewWriter(st)
	defer w.Close()

	// Remove the directory out from under the store: every AppendGroup then
	// fails with ENOENT, for any user, on any filesystem.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Mark before adding, FlushMark after: the only way to ask
			// about THIS caller's rows. A plain Flush reports on whatever
			// batches it happened to wait for, and with a shared buffer
			// that is routinely someone else's rows.
			mark := w.Mark()
			line := fmt.Sprintf(`{"_time":%d,"i":"%d"}`+"\n", 1700000000000000000+int64(i), i)
			if _, err := IngestJSONLines(w, []byte(line), func() int64 { return 1 }); err != nil {
				errs[i] = err
				return
			}
			errs[i] = w.FlushMark(mark)
		}(i)
	}
	wg.Wait()

	falseSuccess := 0
	for _, e := range errs {
		if e == nil {
			falseSuccess++
		}
	}
	if falseSuccess > 0 {
		t.Fatalf("%d of %d flushes reported success against a store that cannot write", falseSuccess, n)
	}
}
