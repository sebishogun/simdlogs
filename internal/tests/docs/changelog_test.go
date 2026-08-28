package docs

import (
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The commit counts in CHANGELOG.md are the real ones, at the sha they name.
//
// A bare count goes stale on the next commit and then reads as current. The
// previous version of that line said "132 chore commits"; exactly one commit is
// prefixed `chore:`, so the number matched nothing and had matched nothing for
// a long time. Pinning the counts to a sha makes them a fixed historical fact,
// and this recomputes them from the history so the fact stays checked.
//
// Skips when git is unavailable or the sha is not in the repository -- a source
// tarball has neither, and failing there would be a gate on the packaging
// rather than on the claim.
func TestTheChangelogCommitCountsAreReal(t *testing.T) {
	b, err := os.ReadFile(repoRoot + "/CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)

	shaRe := regexp.MustCompile("As of `([0-9a-f]{7,40})` the history is ([0-9]+) commits")
	m := shaRe.FindStringSubmatch(text)
	if m == nil {
		t.Fatal("CHANGELOG.md no longer carries an `As of <sha> the history is N commits` " +
			"line; this gate measures nothing without one")
	}
	sha, wantTotal := m[1], mustAtoi(t, m[2])

	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	if err := run(t, "git", "rev-parse", "--git-dir"); err != nil {
		t.Skip("not a git repository (a source tarball)")
	}

	// A SHALLOW checkout fails rather than skips.
	//
	// The skip branch exists for a source tarball, which genuinely has no
	// history. A shallow clone is a different thing: it is a CI configuration
	// that CAN have the history and was not asked for it. Every checkout in
	// this repository's workflows was depth-1, so this gate -- written to stop
	// the changelog counts going stale -- ran nowhere except a developer's own
	// clone, and reported SKIP while doing it. Skipping quietly is how a gate
	// stops being a gate.
	if out, err := output(t, "git", "rev-parse", "--is-shallow-repository"); err == nil &&
		strings.TrimSpace(out) == "true" {
		t.Fatal("this is a shallow checkout, so the sha CHANGELOG.md pins cannot " +
			"be resolved and this gate cannot run. Set `fetch-depth: 0` on the " +
			"checkout step. A tarball with no .git at all skips above; a clone " +
			"that could have the history and does not is a misconfiguration")
	}
	if err := run(t, "git", "cat-file", "-e", sha+"^{commit}"); err != nil {
		t.Fatalf("%s is not in this repository, and it is not a shallow clone or "+
			"a tarball: the sha CHANGELOG.md pins does not exist here", sha)
	}

	out, err := output(t, "git", "log", "--pretty=%s", sha)
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	subjects := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(subjects) != wantTotal {
		t.Errorf("CHANGELOG.md says %d commits at %s; there are %d",
			wantTotal, sha, len(subjects))
	}

	// Every "<n> `prefix`" pair in that paragraph, checked against the history.
	para := text[strings.Index(text, m[0]):]
	if e := strings.Index(para, "\n\n"); e > 0 {
		para = para[:e]
	}
	pairRe := regexp.MustCompile("([0-9]+) `([a-z]+)`")
	pairs := pairRe.FindAllStringSubmatch(para, -1)
	if len(pairs) == 0 {
		t.Fatal("no `<n> `prefix`` pairs found in the paragraph; this gate measures nothing")
	}
	for _, p := range pairs {
		want, prefix := mustAtoi(t, p[1]), p[2]
		got := 0
		for _, s := range subjects {
			if strings.HasPrefix(s, prefix+":") || strings.HasPrefix(s, prefix+"(") {
				got++
			}
		}
		if got != want {
			t.Errorf("CHANGELOG.md says %d %q commits at %s; there are %d",
				want, prefix, sha, got)
		}
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("%q: %v", s, err)
	}
	return n
}

func run(t *testing.T, name string, args ...string) error {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = repoRoot
	return cmd.Run()
}

func output(t *testing.T, name string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = repoRoot
	b, err := cmd.Output()
	return string(b), err
}
