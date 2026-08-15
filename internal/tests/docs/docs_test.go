// Package docs holds gates over the documentation's claims about the code.
//
// The recurring defect these catch is a claim that was written and never
// executed: a table row naming a test that guarantees something, where no test
// of that name exists. It reads as coverage and is prose.
package docs

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// repoRoot is two levels up from internal/tests/docs.
const repoRoot = "../../.."

// Every test named in the active documents exists.
//
// docs/wrong.md is EXCLUDED, and deliberately: it is a historical record, and
// an entry describing a test that was rewritten before it landed is accurate
// about what happened. Rewriting those entries to name today's tests is exactly
// what CLAUDE.md forbids. Three such names were found there on 2026-08-15 and
// answered with an appended correction naming the test that exists, leaving the
// entry itself alone.
//
// docs/plans is excluded because it is future work, not a claim about the code.
func TestEveryTestNamedInTheDocsExists(t *testing.T) {
	declared := declaredTestNames(t)
	if len(declared) == 0 {
		t.Fatal("no test declarations were found, so this gate measures nothing")
	}

	cited := regexp.MustCompile("`((?:Test|Fuzz|Benchmark)[A-Za-z0-9_]+)`")
	checked := 0
	for _, doc := range activeDocs(t) {
		b, err := os.ReadFile(doc)
		if err != nil {
			t.Fatalf("%s: %v", doc, err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			for _, m := range cited.FindAllStringSubmatch(line, -1) {
				checked++
				if !declared[m[1]] {
					t.Errorf("%s:%d names %s, which no test in this repository "+
						"declares: the guarantee on that line is prose",
						doc, i+1, m[1])
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("no test names were found in any document, so this gate measures nothing")
	}
	t.Logf("%d citations across the active documents, all declared", checked)
}

// declaredTestNames is every Test/Fuzz/Benchmark function in the repository.
func declaredTestNames(t *testing.T) map[string]bool {
	t.Helper()
	decl := regexp.MustCompile(`(?m)^func ((?:Test|Fuzz|Benchmark)[A-Za-z0-9_]+)\(`)
	out := map[string]bool{}
	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if name := d.Name(); name == ".git" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range decl.FindAllStringSubmatch(string(b), -1) {
			out[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// activeDocs is every markdown file that makes a claim about the code as it is
// now: the repository root and docs/, minus the historical record and the
// plans.
func activeDocs(t *testing.T) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "plans", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".md") || filepath.Base(path) == "wrong.md" {
			return nil
		}
		out = append(out, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("no documents were found, so this gate measures nothing")
	}
	return out
}
