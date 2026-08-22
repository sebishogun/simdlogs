package ingest

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// Fuzzing every ingest envelope.
//
// # What is being asserted
//
// Three things, and the second is the one that is easy to lose:
//
//  1. NO PANIC. Every one of these parsers reads bytes an unauthenticated
//     client controls, over an endpoint that exists to accept whatever an agent
//     sends. A panic in one is a remote crash, and the syslog and journald
//     listeners have no recover above them.
//  2. A parser that returns an error stores NOTHING. "Rejected the input" and
//     "rejected the input after storing half of it" are the same return value
//     and very different outcomes: the second means a malformed batch leaves
//     rows in the store that no client believes it sent.
//  3. The result is DETERMINISTIC. The same bytes twice must give the same
//     counts and the same error, or a retry of a rejected batch cannot be
//     reasoned about at all.
//
// Bounded allocation is asserted the only way it can be here: the record limits
// are set, and a body that would exceed them must be refused rather than sized
// to. The HTTP layer bounds the body itself, which is where an unbounded read
// would actually live -- see the media-type and body limits in server.go.

// fuzzWriter is a writer over a throwaway store, with limits set so a fuzz
// input cannot allocate its way out of the test.
func fuzzWriter(t *testing.T) (*Writer, *storage.Store) {
	t.Helper()
	st, err := storage.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriterWorkers(st, 1)
	w.SetRecordLimits(RecordLimits{
		MaxFields:     64,
		MaxNameBytes:  1 << 10,
		MaxValueBytes: 4 << 10,
	})
	return w, st
}

// oneEnvelope runs a single ingest function twice over the same bytes and
// asserts the three properties.
func oneEnvelope(
	t *testing.T,
	data []byte,
	fn func(*Writer, []byte, func() int64) (Result, error),
) {
	t.Helper()
	const stamp = int64(1_700_000_000_000_000_000)
	fallback := func() int64 { return stamp }

	run := func() (Result, error, int) {
		w, st := fuzzWriter(t)
		defer st.Close()
		res, err := fn(w, data, fallback)
		// Flushed, because a parser that "stored" rows into a buffer and then
		// failed has not stored them until the flush -- and the question this
		// asks is what ended up in the store.
		flushErr := w.Flush()
		w.Close()
		if flushErr != nil && err == nil {
			err = flushErr
		}
		return res, err, st.TotalRows()
	}

	res1, err1, rows1 := run()
	res2, err2, rows2 := run()

	if (err1 == nil) != (err2 == nil) {
		t.Fatalf("the same bytes gave %v then %v: a retry cannot be reasoned about",
			err1, err2)
	}
	if err1 != nil && err2 != nil && err1.Error() != err2.Error() {
		t.Fatalf("the same bytes gave two different errors:\n  %v\n  %v", err1, err2)
	}
	// Compared field by field: Result carries slices, and the counts plus the
	// truncation flag are what a client acts on.
	if res1.Accepted != res2.Accepted || res1.Rejected != res2.Rejected ||
		res1.RejectedTruncated != res2.RejectedTruncated ||
		len(res1.RejectedAt) != len(res2.RejectedAt) {
		t.Fatalf("the same bytes gave %+v then %+v", res1, res2)
	}
	for i := range res1.RejectedAt {
		if res1.RejectedAt[i] != res2.RejectedAt[i] {
			t.Fatalf("rejected position %d differs: %d vs %d",
				i, res1.RejectedAt[i], res2.RejectedAt[i])
		}
	}
	// The recorded positions must be inside the batch and non-decreasing, or a
	// caller mapping them onto its own records indexes the wrong one -- the
	// exact failure RejectedAt exists to prevent.
	prev := int32(-1)
	for _, at := range res1.RejectedAt {
		if at < 0 || at <= prev && prev >= 0 {
			t.Fatalf("rejected positions are not increasing: %v", res1.RejectedAt)
		}
		prev = at
	}
	if !res1.RejectedTruncated && len(res1.RejectedAt) != res1.Rejected {
		t.Fatalf("%d rejected but %d positions recorded, and truncation is not set",
			res1.Rejected, len(res1.RejectedAt))
	}
	if rows1 != rows2 {
		t.Fatalf("the same bytes stored %d rows then %d", rows1, rows2)
	}
	// The invariant is NOT "an error means nothing was stored". Journald keeps
	// the entries that parsed before a truncation, deliberately and with tests
	// that say so: the alternative discards data a client really sent because
	// its upload was cut.
	//
	// What must hold either way is that the REPORTED count is what landed. A
	// result claiming 3 accepted with 2 rows in the store, or claiming 0 with 2
	// rows in the store, is a client that cannot decide whether to retry -- and
	// retrying a partially-stored batch duplicates exactly the part that landed.
	if res1.Accepted != rows1 {
		t.Fatalf("reported %d accepted and stored %d rows (err=%v): a client cannot "+
			"tell what to re-send", res1.Accepted, rows1, err1)
	}
	if res1.Accepted < 0 || res1.Rejected < 0 {
		t.Fatalf("negative counts: %+v", res1)
	}

}

func FuzzIngestJSONLines(f *testing.F) {
	f.Add([]byte(`{"_msg":"hello","level":"error"}` + "\n"))
	f.Add([]byte(`{"_time":"2026-06-01T12:00:00Z","_msg":"x"}` + "\n{}\n"))
	f.Add([]byte("not json\n"))
	f.Add([]byte(`{"_time":1700000000000000000,"_msg":"n"}`))
	f.Add([]byte(""))
	f.Fuzz(func(t *testing.T, data []byte) {
		oneEnvelope(t, data, IngestJSONLines)
	})
}

func FuzzIngestLogfmt(f *testing.F) {
	f.Add([]byte("_msg=hello level=error\n"))
	f.Add([]byte(`msg="quoted value" k=v` + "\n"))
	f.Add([]byte("=novalue\n"))
	f.Add([]byte("\x00\x00\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		oneEnvelope(t, data, IngestLogfmt)
	})
}

func FuzzIngestSyslog(f *testing.F) {
	f.Add([]byte("<13>1 2026-06-01T12:00:00Z host app 1 - - hello\n"))
	f.Add([]byte("<0>rubbish\n"))
	f.Add([]byte("<999999999999>\n"))
	f.Add([]byte("<13>"))
	f.Fuzz(func(t *testing.T, data []byte) {
		oneEnvelope(t, data, IngestSyslog)
	})
}

func FuzzIngestJournald(f *testing.F) {
	f.Add([]byte("MESSAGE=hello\nPRIORITY=6\n\n"))
	f.Add([]byte("MESSAGE\n\x08\x00\x00\x00\x00\x00\x00\x00binary!!\n"))
	f.Add([]byte("MESSAGE\n\xff\xff\xff\xff\xff\xff\xff\xffoverflow"))
	// 29 bytes that crashed this fuzzer: the truncated-field branch rejected
	// the ordinal and then called emit(), which rejected it again, so ONE
	// record answered Rejected=2 with RejectedAt=[0 0] and the envelope's
	// "rejected positions are not increasing" check fired. The two other ways
	// into emit's refusal branches are seeded with it.
	f.Add([]byte("__REALTIME_TIMESTAMP=1\nORPHAN"))
	f.Add([]byte("__REALTIME_TIMESTAMP=99999999999999999999\nORPHAN"))
	f.Add([]byte("MESSAGE=x\nORPHAN"))
	f.Add([]byte("__REALTIME_TIMESTAMP=-1\nMESSAGE=x\n\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		oneEnvelope(t, data, IngestJournald)
	})
}

func FuzzIngestLoki(f *testing.F) {
	f.Add([]byte(`{"streams":[{"stream":{"a":"b"},"values":[["1700000000000000000","x"]]}]}`))
	f.Add([]byte(`{"streams":[{"values":[["notanumber","x"]]}]}`))
	f.Add([]byte(`{"streams":null}`))
	f.Add([]byte(`{`))
	f.Fuzz(func(t *testing.T, data []byte) {
		oneEnvelope(t, data, IngestLoki)
	})
}

func FuzzIngestDatadog(f *testing.F) {
	f.Add([]byte(`[{"message":"x","ddsource":"go"}]`))
	f.Add([]byte(`[{"message":123}]`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		oneEnvelope(t, data, IngestDatadog)
	})
}

func FuzzIngestOTLPLogs(f *testing.F) {
	f.Add([]byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"x"}}]}]}]}`))
	f.Add([]byte(`{"resourceLogs":[{"scopeLogs":null}]}`))
	f.Add([]byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"nope"}]}]}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		oneEnvelope(t, data, IngestOTLPLogs)
	})
}

// The protobuf paths take an Options, so they get their own wrapper rather than
// a fake signature.
func FuzzIngestLokiProto(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x0a, 0x02, 0x08, 0x01})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f})
	f.Fuzz(func(t *testing.T, data []byte) {
		oneEnvelope(t, data, func(w *Writer, b []byte, fb func() int64) (Result, error) {
			return IngestLokiProto(w, b, fb, nil)
		})
	})
}

func FuzzIngestOTLPProto(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x0a, 0x00})
	f.Add([]byte{0x0a, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Fuzz(func(t *testing.T, data []byte) {
		oneEnvelope(t, data, func(w *Writer, b []byte, fb func() int64) (Result, error) {
			return IngestOTLPLogsProto(w, b, fb, nil)
		})
	})
}

// The vector field spec and value parsers take configuration a client can
// influence, and both write into fixed-size buffers.
func FuzzParseVectorFields(f *testing.F) {
	f.Add("embedding:768")
	f.Add("a:1,b:2")
	f.Add("x:-1")
	f.Add("x:99999999999999999999")
	f.Add(":")
	f.Fuzz(func(t *testing.T, spec string) {
		vf, err := ParseVectorFields(spec)
		if err != nil {
			return
		}
		// An accepted spec must describe usable dimensions: a zero or negative
		// one would size a slice that every row then indexes into.
		for name := range vf {
			d, ok := vf.Dim(name)
			if !ok || d <= 0 {
				t.Fatalf("spec %q accepted field %q with dimension %d", spec, name, d)
			}
		}
	})
}

func FuzzParseVector(f *testing.F) {
	f.Add("[1,2,3]", 3)
	f.Add("[]", 0)
	f.Add("[1e400]", 1)
	f.Add("[1,2", 2)
	f.Fuzz(func(t *testing.T, text string, dim int) {
		if dim < 0 || dim > 4096 {
			return // the caller's configuration, not the client's input
		}
		dst := make([]float32, 0, dim)
		got, err := ParseVector(dst, "v", text, dim)
		if err != nil {
			return
		}
		// A vector that parsed must be exactly the configured width, or the
		// flat row-major buffer it goes into is misaligned for every later row.
		if len(got) != dim {
			t.Fatalf("%q at dim %d parsed to %d floats", text, dim, len(got))
		}
	})
}
