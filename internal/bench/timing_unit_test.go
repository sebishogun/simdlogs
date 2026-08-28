package bench

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Unit tests for the benchmark harness itself. Deliberately not env-gated and
// not dependent on a staged VictoriaLogs binary: a harness that only runs when
// someone sets SIMDLOGS_OPS=1 is a harness whose defects reach a README table
// before anyone runs it. These execute in the ordinary `go test ./...`.

// The defect: TestHeadToHead stamped VL's ingest duration after a fixed
// `time.Sleep(3 * time.Second)` that existed to let VL flush. Against the old
// shape this test fails by roughly the readiness delay.
func TestTimeIngestExcludesReadinessWait(t *testing.T) {
	const (
		postCost  = 20 * time.Millisecond
		readyAt   = 200 * time.Millisecond
		slackHigh = 120 * time.Millisecond // generous: this must not flake under load
	)
	start := time.Now()
	tm, err := timeIngest(
		func() { time.Sleep(postCost) }, // bench:untimed -- the fake POST under test
		func() bool { return time.Since(start) >= readyAt },
		5*time.Millisecond, 5*time.Second)
	if err != nil {
		t.Fatalf("timeIngest: %v", err)
	}
	if tm.accept >= readyAt {
		t.Errorf("accept = %v, which is at or past the readiness delay %v: the wait is being counted as ingest",
			tm.accept, readyAt)
	}
	if tm.accept < postCost {
		t.Errorf("accept = %v, less than the work the POST did (%v)", tm.accept, postCost)
	}
	if tm.accept > postCost+slackHigh {
		t.Errorf("accept = %v, far past the POST's own cost %v", tm.accept, postCost)
	}
	if tm.queryable < readyAt {
		t.Errorf("queryable = %v, before the engine was ready (%v)", tm.queryable, readyAt)
	}
}

// A synchronous engine is ready on the first poll; its two numbers should be
// adjacent, and queryable is never before accept for any engine.
func TestTimeIngestSynchronousEngineIsQueryableAtAccept(t *testing.T) {
	polls := 0
	tm, err := timeIngest(
		func() { time.Sleep(5 * time.Millisecond) }, // bench:untimed -- fake POST
		func() bool { polls++; return true },
		time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("timeIngest: %v", err)
	}
	if polls != 1 {
		t.Errorf("ready polled %d times for an engine ready immediately, want 1", polls)
	}
	if tm.queryable < tm.accept {
		t.Errorf("queryable %v is before accept %v", tm.queryable, tm.accept)
	}
	if tm.queryable-tm.accept > 500*time.Millisecond {
		t.Errorf("queryable %v is far past accept %v though ready was true at once", tm.queryable, tm.accept)
	}
}

// An engine that never becomes queryable must end the sample, not the machine's
// afternoon.
func TestTimeIngestTimesOut(t *testing.T) {
	t0 := time.Now()
	_, err := timeIngest(func() {}, func() bool { return false },
		5*time.Millisecond, 80*time.Millisecond)
	if err == nil {
		t.Fatal("no error from an engine that never became ready")
	}
	if el := time.Since(t0); el > 3*time.Second {
		t.Errorf("took %v to give up on an 80ms limit", el)
	}
}

// The multiplication defect: timing an ingest with timeIt wrote the corpus
// eight times into one store -- and TestPerOperation did that for two
// formats, so each engine held sixteen times the corpus its report named. sampleIngest must call fresh before every pass,
// warmup included, so no sample ingests into a store a previous sample grew.
func TestSampleIngestUsesAFreshStorePerSample(t *testing.T) {
	const samples = 3
	fresh, posts := 0, 0
	rows := 0
	tm, err := sampleIngest(samples,
		func() error { fresh++; rows = 0; return nil },
		func() { posts++; rows += 100 },
		func() bool { return rows >= 100 },
		time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("sampleIngest: %v", err)
	}
	if want := samples + 1; fresh != want {
		t.Errorf("fresh called %d times for %d samples, want %d (one per sample plus warmup)", fresh, samples, want)
	}
	if posts != fresh {
		t.Errorf("%d posts against %d fresh stores: a sample ingested into a store it did not reset", posts, fresh)
	}
	if rows != 100 {
		t.Errorf("store holds %d rows after the run, want one sample's worth (100)", rows)
	}
	if tm.accept <= 0 || tm.queryable <= 0 {
		t.Errorf("zero timing returned: accept %v queryable %v", tm.accept, tm.queryable)
	}
}

// A fresh that fails must stop the run rather than let the remaining samples
// pile into a store nobody reset.
func TestSampleIngestStopsWhenFreshFails(t *testing.T) {
	calls := 0
	_, err := sampleIngest(4,
		func() error {
			calls++
			if calls == 2 {
				return fmt.Errorf("disk full")
			}
			return nil
		},
		func() {}, func() bool { return true }, time.Millisecond, time.Second)
	if err == nil {
		t.Fatal("no error when fresh failed")
	}
	if !strings.Contains(err.Error(), "disk full") {
		t.Errorf("error %q does not carry the cause", err)
	}
	if calls != 2 {
		t.Errorf("fresh called %d times, want 2 -- the run continued past the failure", calls)
	}
}

func TestSampleIngestRejectsZeroSamples(t *testing.T) {
	if _, err := sampleIngest(0, func() error { return nil }, func() {},
		func() bool { return true }, time.Millisecond, time.Second); err == nil {
		t.Fatal("no error for n=0")
	}
}

// rowCount is the check that would have caught the 8x corpus multiplication
// directly. It has to read both engines' answer shapes.
func TestRowCountParsesBothAnswerShapes(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       int
		wantErr    bool
	}{
		{"string count", "{\"n\":\"400000\"}\n", 400000, false},
		{"numeric count", "{\"n\":123}\n", 123, false},
		{"leading blank lines", "\n\n{\"n\":\"7\"}\n", 7, false},
		{"empty answer", "", 0, true},
		{"no field n", "{\"count\":\"5\"}\n", 0, true},
		{"not an integer", "{\"n\":\"lots\"}\n", 0, true},
		{"not JSON", "n=5\n", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if q := r.URL.Query().Get("query"); q != "* | stats count() n" {
					t.Errorf("query = %q", q)
				}
				w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			got, err := rowCount(srv.URL, 0, 1)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("no error, got %d", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("rowCount: %v", err)
			}
			if got != tc.want {
				t.Errorf("rowCount = %d, want %d", got, tc.want)
			}
		})
	}
}

// A non-200 is an error, not a zero row count: silently reporting zero rows
// would make a broken engine look like an empty one and let the read half time
// nothing at all.
func TestRowCountRejectsErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "query too heavy", http.StatusRequestEntityTooLarge)
	}))
	defer srv.Close()
	if n, err := rowCount(srv.URL, 0, 1); err == nil {
		t.Fatalf("no error for a 413, got %d rows", n)
	}
}

// readyAtLeast must not report ready when the engine errors or is short.
func TestReadyAtLeast(t *testing.T) {
	n := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "{\"n\":\"%d\"}\n", n)
	}))
	defer srv.Close()
	ready := readyAtLeast(srv.URL, 0, 1, 500)
	for _, tc := range []struct {
		have int
		want bool
	}{{0, false}, {499, false}, {500, true}, {501, true}} {
		n = tc.have
		if got := ready(); got != tc.want {
			t.Errorf("ready with %d/500 rows = %v, want %v", tc.have, got, tc.want)
		}
	}
}

func TestReadyAtLeastIsNotReadyWhenTheEngineErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "starting up", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	if readyAtLeast(srv.URL, 0, 1, 1)() {
		t.Error("ready reported true for an engine returning 503")
	}
}

// The source gate. A sleep in a timed region is how the three-second VL ingest
// figure happened, and the fix is only durable if the next one has to be
// argued for at the call site. Every time.Sleep in this package carries a
// `// bench:untimed` marker on its line saying it is outside a measurement.
//
// Parsed rather than grepped: a regex over the raw text also flags the two
// comments in this package that discuss the defect, and a gate that cries wolf
// gets a blanket marker rather than a reading.
func TestEveryBenchSleepIsAnnotated(t *testing.T) {
	// Both this package and its corpus subpackage: a wait added in the
	// corpus generator is just as capable of landing inside a timed
	// interval, and the old glob could not see it.
	files, err := filepath.Glob("*.go")
	sub, subErr := filepath.Glob("corpus/*.go")
	if subErr != nil {
		t.Fatal(subErr)
	}
	files = append(files, sub...)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no source files found; the gate would pass vacuously")
	}
	fset := token.NewFileSet()
	found := 0
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		lines := strings.Split(string(src), "\n")
		// The name(s) this file uses for the time package, alias included.
		timeNames := map[string]bool{}
		for _, imp := range file.Imports {
			if imp.Path == nil || imp.Path.Value != `"time"` {
				continue
			}
			if imp.Name != nil {
				timeNames[imp.Name.Name] = true
			} else {
				timeNames["time"] = true
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			// Every way to wait, not just time.Sleep. `<-time.After(3*time.Second)`
			// is one keystroke from the defect this gate exists to prevent, and
			// a gate that knows only the one spelling is a gate that gets
			// routed around.
			switch sel.Sel.Name {
			case "Sleep", "After", "Tick", "NewTimer", "NewTicker":
			default:
				return true
			}
			// The receiver must be the file's own name for the time package --
			// which is resolved from its import, so an aliased
			// `import stdtime "time"` is caught too. Requiring the package
			// rather than any receiver is what separates the package function
			// time.After from the METHOD time.Now().After(deadline), which is a
			// comparison and waits for nothing.
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || !timeNames[pkg.Name] {
				return true
			}
			found++
			pos := fset.Position(call.Pos())
			line := ""
			if pos.Line-1 < len(lines) {
				line = lines[pos.Line-1]
			}
			if !strings.Contains(line, "bench:untimed") {
				t.Errorf("%s:%d: %s with no `// bench:untimed` marker -- "+
					"if it is outside every timed interval say so on the line; "+
					"if it is inside one, it is a measurement defect:\n\t%s",
					f, pos.Line, sel.Sel.Name, strings.TrimSpace(line))
			}
			return true
		})
	}
	if found == 0 {
		t.Error("no wait call found anywhere in the package; the gate is not testing anything")
	}
}
