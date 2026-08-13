package query

import (
	"testing"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// TestBadRegexNoPanic: a malformed regex must be a clean parse error, and a
// programmatically-built bad-regex predicate must not panic at eval.
func TestBadRegexNoPanic(t *testing.T) {
	if _, err := ParseLogsQL(`_msg:~"("`); err == nil {
		t.Error("bad regex filter: want parse error, got nil")
	}
	if _, err := ParseLogsQL(`* | replace_regexp ("(", "x")`); err != nil {
		t.Logf("replace_regexp bad pattern parse: %v", err) // parse may defer; must not panic at apply
	}
	s, _ := storage.OpenStore(t.TempDir())
	d := storage.BuildDict([]string{"abc"})
	s.AppendGroup(&storage.Group{Rows: 1, Columns: []storage.Column{
		{Name: "_time", Type: storage.ColTimestamp, Ts: []int64{1}},
		{Name: "_msg", Type: storage.ColDict, Dict: &d},
	}})
	// Programmatic bad-regex pred: must return no rows, not panic.
	q := &Query{From: 0, To: 1 << 62, Preds: []Pred{{Field: "_msg", Kind: Regexp, Value: "("}}}
	if n := len(Run(s, q)); n != 0 {
		t.Errorf("bad regex eval: want 0 rows, got %d", n)
	}
	// replace_regexp with a bad pattern at apply: must not panic, leaves rows.
	pq, err := ParseLogsQL(`* | replace_regexp ("[", "x")`)
	if err == nil {
		pq.From, pq.To = 0, 1<<62
		_ = RunPipeline(s, pq) // must not panic
	}
}

// TestParserNoPanicOnGarbage throws malformed queries at the full parser and
// confirms none panic -- every error must be returned, not thrown.
func TestParserNoPanicOnGarbage(t *testing.T) {
	bad := []string{
		``, `|`, `| stats`, `foo:`, `foo:in(`, `foo:range(`, `foo:range(a,b)`,
		`_time:`, `_time:[`, `_time:[a,b]`, `_time:xyz`, `_time:day_range[99:99]`,
		`_time:week_range[Xxx,Yyy]`, `* | stats row_max(`, `* | stats quantile(x)`,
		`* | join by (a)`, `* | join by (a) (`, `* | union (`, `* | unroll`,
		`* | sample`, `* | first`, `* | field_values`, `foo:ipv4_range(x,y)`,
		`foo:seq(`, `foo:eq_field(`, `* | stats count() if (`, `_stream_id:`,
		`* | extract_regexp`, `* | pack_json fields (`, `* | stats histogram(x) if(bad`,
		`* | replace_regexp (`, `foo:~"["`, `* | sort by (`, `* | math "`,
	}
	for _, q := range bad {
		func() {
			defer func() {
				if v := recover(); v != nil {
					t.Errorf("panic on %q: %v", q, v)
				}
			}()
			_, _ = ParseLogsQL(q) // error is fine; panic is not
		}()
	}
}
