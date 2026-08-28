package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// The two recompaction flags quote the same measured figure.
//
// -recompact-after claimed "~17% smaller" for flate dictionaries while
// -recompact-drop-postings, one line above, said "8% for flate alone". The
// measurement in docs/wrong.md ("Tiered storage closes most of the disk gap")
// is -8.1% on the realistic 100k corpus, so the operator-facing number for the
// flag that DOES the flate re-encode was the wrong one of the two.
//
// A source-level check because the flags are declared inside main(), where a
// test cannot reach their usage strings without running the program. It is the
// same shape as any other check on text: put it where the text is.
func TestTheRecompactionFlagsAgreeOnTheMeasuredFigure(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	text := string(src)

	// docs/wrong.md, "Tiered storage closes most of the disk gap":
	//	flate dictionaries only          9464KB   -8.1%
	//	flate + drop the inverted index  6648KB  -35.4%
	const flatePct = "8"
	const bothPct = "35"

	after := helpOf(t, text, "recompact-after")
	drop := helpOf(t, text, "recompact-drop-postings")

	pcts := regexp.MustCompile(`(\d+)%`)
	afterNums := pcts.FindAllStringSubmatch(after, -1)
	if len(afterNums) != 1 || afterNums[0][1] != flatePct {
		t.Errorf("-recompact-after help quotes %v, and the measurement for "+
			"flate dictionaries alone is -%s.1%%:\n  %s", pctList(afterNums), flatePct, after)
	}
	dropNums := pcts.FindAllStringSubmatch(drop, -1)
	if len(dropNums) != 2 || dropNums[0][1] != bothPct || dropNums[1][1] != flatePct {
		t.Errorf("-recompact-drop-postings help quotes %v, and the measurement "+
			"is -%s.4%% for both levers against -%s.1%% for flate alone:\n  %s",
			pctList(dropNums), bothPct, flatePct, drop)
	}
	// And the pair must agree with each other, which is the failure that
	// actually happened: one said 17 and the other said 8 about the same thing.
	if !strings.Contains(drop, flatePct+"% for flate alone") {
		t.Errorf("-recompact-drop-postings no longer states the flate-alone "+
			"figure the other flag has to match:\n  %s", drop)
	}
}

// helpOf returns the usage string of the named flag as it is written in the
// source. It fails rather than returning empty: a rename that this cannot find
// is a check that has stopped checking.
func helpOf(t *testing.T, src, flagName string) string {
	t.Helper()
	i := strings.Index(src, `"`+flagName+`"`)
	if i < 0 {
		t.Fatalf("no flag named %q in main.go; this test was pinning its help "+
			"text and can no longer find it", flagName)
	}
	rest := src[i:]
	// The usage string is the last quoted argument before the closing paren of
	// the flag declaration, and every one of these fits on one logical line.
	end := strings.Index(rest, ")\n")
	if end < 0 {
		t.Fatalf("could not find the end of the %q declaration", flagName)
	}
	decl := rest[:end]
	quoted := regexp.MustCompile(`"([^"]*)"`).FindAllStringSubmatch(decl, -1)
	if len(quoted) < 2 {
		t.Fatalf("the %q declaration has no usage string: %s", flagName, decl)
	}
	return quoted[len(quoted)-1][1]
}

func pctList(m [][]string) []string {
	out := make([]string, 0, len(m))
	for _, g := range m {
		out = append(out, g[1]+"%")
	}
	return out
}
