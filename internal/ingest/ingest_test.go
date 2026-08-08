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
	ing, skip := IngestJSONLines(w, []byte(sb.String()), func() int64 { mono++; return mono })
	if ing != 50_000 || skip != 0 {
		t.Fatalf("ingested %d skipped %d", ing, skip)
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
	_, skip = IngestJSONLines(w, []byte("not json\n{bad\n"), func() int64 { return 0 })
	if skip == 0 {
		t.Fatal("malformed lines were not counted as skipped")
	}
}
