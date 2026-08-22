package docs

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// THE RELEASE DOCUMENT'S PACKAGE COUNT IS THE COUNT ON DISK.
//
// `docs/release-readiness.md` states "N packages ok" against five separate
// gates -- test, race, purego, fuzz, crash/recovery. It said NINE while the
// module had TEN packages carrying tests, so every one of those rows described
// a run that had not happened for one package, and the document that exists to
// say the release is ready was the thing carrying the stale number.
//
// Nothing checked it. The count is exactly the kind of fact that rots on the
// commit that adds a package, and the release gate is the worst place for a
// number nobody verifies.
//
// COUNTED FROM THE TREE, compared against the DOCUMENT. Neither is written
// down twice: a constant here and a number there can disagree forever.
func TestTheReleaseDocumentsPackageCountIsReal(t *testing.T) {
	pkgs := map[string]bool{}
	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "testdata", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			pkgs[filepath.Dir(path)] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(pkgs) == 0 {
		t.Fatal("no packages with tests found; the walk has lost the tree " +
			"rather than the tree having no tests")
	}

	b, err := os.ReadFile(repoRoot + "/docs/release-readiness.md")
	if err != nil {
		t.Fatalf("reading the document whose claim this gates: %v", err)
	}
	m := regexp.MustCompile(`(\d+) packages ok`).FindAllSubmatch(b, -1)
	if len(m) == 0 {
		t.Fatal(`docs/release-readiness.md no longer says "N packages ok". ` +
			`This gate reads that phrase; if the wording changed, repoint the ` +
			`gate rather than leaving it green over nothing`)
	}
	for _, hit := range m {
		claimed, err := strconv.Atoi(string(hit[1]))
		if err != nil {
			t.Fatal(err)
		}
		if claimed != len(pkgs) {
			t.Errorf("the release document claims %d packages ok; %d packages "+
				"carry tests. A gate row that names the wrong number describes "+
				"a run that did not cover everything it says it did",
				claimed, len(pkgs))
		}
	}
	t.Logf("%d packages with tests, %d rows in the document", len(pkgs), len(m))
}
