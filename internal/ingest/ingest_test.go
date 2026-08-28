package ingest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

func TestIngestThenQuery(t *testing.T) {
	dir := t.TempDir()
	s, err := storage.OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(s)

	// A batch of NDJSON with a known number of errors.
	var sb strings.Builder
	wantErr := 0
	base := int64(1_700_000_000_000_000_000)
	for i := 0; i < 50_000; i++ {
		lvl := "info"
		if i%7 == 0 {
			lvl = "error"
			wantErr++
		}
		fmt.Fprintf(&sb, `{"_time":%d,"level":%q,"service":"api","_msg":"req %d"}`+"\n",
			base+int64(i)*1000, lvl, i)
	}
	var mono int64
	ingRes, _ := IngestJSONLines(w, []byte(sb.String()), func() int64 { mono++; return mono })
	if ingRes.Accepted != 50_000 || ingRes.Rejected != 0 {
		t.Fatalf("ingested %d skipped %d", ingRes.Accepted, ingRes.Rejected)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Query the ingested data: level:=error must return wantErr rows.
	rows := query.Run(s, &query.Query{
		From: 0, To: int64(1) << 62,
		Preds: []query.Pred{{Field: "level", Kind: query.Eq, Value: "error"}},
	})
	if len(rows) != wantErr {
		t.Fatalf("query got %d error rows, want %d", len(rows), wantErr)
	}
	// Malformed lines are skipped, not fatal.
	badRes, _ := IngestJSONLines(w, []byte("not json\n{bad\n"), func() int64 { return 0 })
	if badRes.Rejected == 0 {
		t.Fatal("malformed lines were not counted as skipped")
	}
}

// TestTimeStoredOnce guards the storage shape: a parsed _time belongs in the
// timestamp column and nowhere else. Keeping the field too wrote every record's
// time a second time as a near-unique dictionary string, 13.6% of the store.
func TestTimeStoredOnce(t *testing.T) {
	for _, tc := range []struct {
		name, body   string
		parse        func(*Writer, []byte, func() int64) (Result, error)
		wantTimeCol  int
		wantKeptText bool
	}{
		{
			name:        "jsonline parsed",
			body:        `{"_time":"2024-05-01T00:00:00Z","level":"error"}` + "\n",
			parse:       IngestJSONLines,
			wantTimeCol: 1,
		},
		{
			name:         "jsonline unparseable time is kept as data",
			body:         `{"_time":"not a time","level":"error"}` + "\n",
			parse:        IngestJSONLines,
			wantTimeCol:  2, // the timestamp column plus the value we could not read
			wantKeptText: true,
		},
		{
			name:        "logfmt parsed",
			body:        "_time=2024-05-01T00:00:00Z level=error\n",
			parse:       IngestLogfmt,
			wantTimeCol: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := storage.OpenStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			w := NewWriter(st)
			if r, _ := tc.parse(w, []byte(tc.body), func() int64 { return 1 }); r.Accepted != 1 {
				t.Fatalf("ingested %d records, want 1", r.Accepted)
			}
			w.Close()
			groups := st.Groups(0, int64(1)<<62)
			if len(groups) != 1 {
				t.Fatalf("groups = %d want 1", len(groups))
			}
			n := 0
			for _, name := range groups[0].ColumnNames() {
				if name == "_time" {
					n++
				}
			}
			if n != tc.wantTimeCol {
				t.Errorf("_time columns = %d want %d (columns: %v)", n, tc.wantTimeCol, groups[0].ColumnNames())
			}
		})
	}
}
