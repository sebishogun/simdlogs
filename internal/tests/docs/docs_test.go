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

func TestResolvedDocumentationDefectsDoNotReturn(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(repoRoot, path))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	t.Run("wrong record has unique numbered headings", func(t *testing.T) {
		headings := regexp.MustCompile(`(?m)^## ([0-9]+)\.`).FindAllStringSubmatch(
			read("docs/wrong.md"), -1)
		seen := make(map[string]bool, len(headings))
		for _, heading := range headings {
			if seen[heading[1]] {
				t.Errorf("docs/wrong.md repeats numbered heading %s", heading[1])
			}
			seen[heading[1]] = true
		}
	})

	t.Run("retired plan task IDs stay retired", func(t *testing.T) {
		roadmap := read("docs/roadmap.md")
		for _, id := range []string{"Task B.2", "Task G.1"} {
			if strings.Contains(roadmap, id) {
				t.Errorf("docs/roadmap.md still cites nonexistent %s", id)
			}
		}
	})

	t.Run("historical record references are disambiguated", func(t *testing.T) {
		wrong := read("docs/wrong.md")
		const correction = "The `entry 37` references in entries 38-40 name the " +
			"unnumbered review note immediately before entry 38"
		if !strings.Contains(strings.Join(strings.Fields(wrong), " "), correction) {
			t.Errorf("docs/wrong.md does not disambiguate the three historical entry-37 references")
		}

		storage := read("docs/lld/storage.md")
		if strings.Contains(storage, "17%") {
			t.Error("docs/lld/storage.md still contains the retired 17% help claim")
		}
	})

	t.Run("source comments describe the shipped paths", func(t *testing.T) {
		es := read("internal/api/es.go")
		before, _, ok := strings.Cut(es, "type esQuery struct")
		if !ok {
			t.Fatal("internal/api/es.go no longer declares esQuery")
		}
		for _, clause := range []string{
			"bool", "must", "filter", "must_not", "should", "term", "terms",
			"range", "exists", "match", "prefix", "match_all",
		} {
			if !regexp.MustCompile(`\b` + clause + `\b`).MatchString(before) {
				t.Errorf("internal/api/es.go header does not name supported %s clause", clause)
			}
		}

		scale := read("internal/bench/scale_test.go")
		if strings.Contains(scale, "there is no mmap yet") {
			t.Error("internal/bench/scale_test.go still says there is no mmap")
		}
	})
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
