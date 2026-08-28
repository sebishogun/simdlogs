package bench

import (
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"os"
	"regexp"
	"strconv"
	"testing"
)

// The README's parity numbers are the ones the checks actually run.
//
// "40 of 40 identical" is a hand-copied number. The differential prints its own
// "%d/%d identical" at run time and nobody compares the two, so adding a query
// to the list leaves the README claiming 40 while the check runs 41 -- and the
// number a reader trusts is the smaller, older one. That is the same shape as
// simdmetrics' README claiming 16 shards against a constant of 64: neither was
// wrong when written, and nothing connected them.
//
// It counts the CASES from the syntax tree rather than re-running the
// differential, so it needs no reference binary and runs in the ordinary lane --
// the run that would catch the drift is the one that only happens when someone
// stages victoria-logs.
//
// It fails if the claim disappears from the README too. A gate matching nothing
// is not a gate.
func TestTheReadmeParityCountsAreTheOnesRun(t *testing.T) {
	b, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Skip("no README (a source tarball)")
	}
	re := regexp.MustCompile(`against the reference binary \| ([0-9]+) of ([0-9]+) identical`)
	m := re.FindSubmatch(b)
	if m == nil {
		t.Fatal("README.md no longer carries an `N of N identical` parity claim; " +
			"this gate measures nothing without one")
	}
	claimedIdentical, err := strconv.Atoi(string(m[1]))
	if err != nil {
		t.Fatal(err)
	}
	claimedTotal, err := strconv.Atoi(string(m[2]))
	if err != nil {
		t.Fatal(err)
	}
	if claimedIdentical != claimedTotal {
		t.Errorf("README.md claims %d of %d identical: a differing answer is a FAILURE "+
			"in this differential, so the two numbers cannot legitimately differ",
			claimedIdentical, claimedTotal)
	}

	got := countCompatQueries(t)
	if got != claimedTotal {
		t.Errorf("README.md says the differential compares %d queries; compat_test.go "+
			"lists %d", claimedTotal, got)
	}
}

// countCompatQueries is the number of entries in TestLogsQLCompat's query table,
// from the syntax tree -- a composite literal element, not a line matching a
// regex, so a commented-out case or a query containing a brace does not count.
func countCompatQueries(t *testing.T) int {
	t.Helper()
	fset := gotoken.NewFileSet()
	f, err := parser.ParseFile(fset, "compat_test.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing compat_test.go: %v", err)
	}
	n := 0
	found := false
	ast.Inspect(f, func(node ast.Node) bool {
		as, ok := node.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 {
			return true
		}
		id, ok := as.Lhs[0].(*ast.Ident)
		if !ok || id.Name != "queries" {
			return true
		}
		cl, ok := as.Rhs[0].(*ast.CompositeLit)
		if !ok {
			return true
		}
		found = true
		n = len(cl.Elts)
		return false
	})
	if !found {
		t.Fatal("compat_test.go no longer assigns a `queries` composite literal, so this " +
			"gate cannot count what the differential runs")
	}
	if n == 0 {
		t.Fatal("the differential's query table is empty")
	}
	return n
}
