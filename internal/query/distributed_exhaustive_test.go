package query

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every pipe in the language is classified explicitly, and the default is
// unreachable rather than load-bearing.
//
// ClassifyPipe ends in `return PipeCoordinatorOnly`, and four of its cases
// return the same thing -- so those cases look inert, and a test asserting
// ClassifyPipe(&JoinPipe{}) == PipeCoordinatorOnly passes with the case
// deleted. The cases are not inert: they are where the reasoning for each pipe
// is written down, and the default exists only because Go requires the function
// to return.
//
// What actually matters is that a NEW pipe cannot reach the default. A pipe
// added to the language without a case here silently becomes coordinator-only:
// correct, slower than it needs to be, and invisible -- nobody would find out
// until someone wondered why a filter stopped being pushed down. So the set of
// pipes is taken from the package's own source and every one has to appear.
func TestEveryPipeIsClassifiedExplicitly(t *testing.T) {
	applies := regexp.MustCompile(`func \(\w+ \*(\w+Pipe)\) apply\(`)
	cases := regexp.MustCompile(`\*(\w+Pipe)`)

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	pipes := map[string]string{} // type -> file it is declared in
	var classifyBody string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		for _, m := range applies.FindAllStringSubmatch(src, -1) {
			pipes[m[1]] = f
		}
		if i := strings.Index(src, "func ClassifyPipe("); i >= 0 {
			rest := src[i:]
			if j := strings.Index(rest, "\n}\n"); j > 0 {
				classifyBody = rest[:j]
			}
		}
	}
	if classifyBody == "" {
		t.Fatal("ClassifyPipe was not found, so this test measures nothing")
	}
	if len(pipes) == 0 {
		t.Fatal("no pipe implementations were found, so this test measures nothing")
	}

	classified := map[string]bool{}
	for _, m := range cases.FindAllStringSubmatch(classifyBody, -1) {
		classified[m[1]] = true
	}
	for p, f := range pipes {
		if !classified[p] {
			t.Errorf("%s (%s) implements Pipe and has no case in ClassifyPipe, so it "+
				"falls through to the default and runs at the coordinator with no "+
				"record of anyone having decided that", p, f)
		}
	}
	// And nothing named in the switch that is not a pipe: a case for a type
	// that no longer exists reads as coverage and is dead.
	for c := range classified {
		if _, ok := pipes[c]; !ok {
			t.Errorf("ClassifyPipe has a case for %s, which implements no apply "+
				"method in this package", c)
		}
	}
	t.Logf("%d pipe types, all classified", len(pipes))
}
