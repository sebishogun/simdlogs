package query

import (
	"go/ast"
	"go/importer"
	"go/parser"
	gotoken "go/token"
	"go/types"
	"path/filepath"
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
// is written down. What matters is that a NEW pipe cannot reach the default: it
// would silently become coordinator-only -- correct, slower than it needs to
// be, and invisible until someone wondered why a filter stopped being pushed
// down.
//
// # Why this type-checks the package instead of matching source text
//
// The first version of this gate found pipes with
// `func \(\w+ \*(\w+Pipe)\) apply\(` and cases with `\*(\w+Pipe)`, and a
// reviewer showed three ways past it, each of which produced a real pipe with
// no case while the test logged "all classified":
//
//   - a type whose name does not end in "Pipe" (`WindowOp`)
//   - a VALUE receiver (`func (w WindowPipe) apply(...)`)
//   - the type named in a COMMENT inside ClassifyPipe's body, which the case
//     regex counted as a case. That one is a single edit away: this commit's
//     own change ADDED prose to ClassifyPipe's doc comment.
//
// The compiler's own view has none of those gaps: implementing Pipe is what
// makes a type a pipe, and a case is an ast.CaseClause, not a word.
func TestEveryPipeIsClassifiedExplicitly(t *testing.T) {
	fset := gotoken.NewFileSet()
	files, pkgFiles := parsePackage(t, fset)

	conf := types.Config{Importer: importer.ForCompiler(fset, "source", nil)}
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}}
	pkg, err := conf.Check("github.com/sebishogun/simdlogs/internal/query", fset, files, info)
	if err != nil {
		t.Fatalf("type-checking the package: %v", err)
	}

	pipeIface := lookupInterface(t, pkg, "Pipe")

	// Every named type in the package that implements Pipe, by pointer or by
	// value. Both, because a value receiver satisfies the interface too and the
	// old gate missed exactly that.
	pipes := map[string]bool{}
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		tn, isType := scope.Lookup(name).(*types.TypeName)
		if !isType || tn.IsAlias() {
			continue
		}
		named, isNamed := tn.Type().(*types.Named)
		if !isNamed {
			continue
		}
		// Pipe itself satisfies Pipe. So would any other interface embedding
		// it: an interface is not a pipe, it is a way of naming them.
		if _, isIface := named.Underlying().(*types.Interface); isIface {
			continue
		}
		if types.Implements(named, pipeIface) ||
			types.Implements(types.NewPointer(named), pipeIface) {
			pipes[name] = true
		}
	}
	if len(pipes) == 0 {
		t.Fatal("no type in this package implements Pipe, so this gate measures nothing")
	}

	classified := classifiedIn(t, fset, pkgFiles)
	if len(classified) == 0 {
		t.Fatal("ClassifyPipe has no type-switch cases, so this gate measures nothing")
	}

	for p := range pipes {
		if !classified[p] {
			t.Errorf("%s implements Pipe and has no case in ClassifyPipe, so it falls "+
				"through to the default and runs at the coordinator with no record of "+
				"anyone having decided that", p)
		}
	}
	for c := range classified {
		if !pipes[c] {
			t.Errorf("ClassifyPipe has a case for %s, which does not implement Pipe", c)
		}
	}
	// Reported as a count, not as a verdict: t.Errorf above decides whether
	// they are all classified, and a log line saying so unconditionally is
	// exactly the reassuring output this repository has been bitten by.
	t.Logf("%d types implement Pipe; %d cases in ClassifyPipe", len(pipes), len(classified))
}

// parsePackage parses every non-test file of this package.
func parsePackage(t *testing.T, fset *gotoken.FileSet) ([]*ast.File, []*ast.File) {
	t.Helper()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, n := range names {
		if strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("%s: %v", n, err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		t.Fatal("no source files found, so this gate measures nothing")
	}
	return files, files
}

func lookupInterface(t *testing.T, pkg *types.Package, name string) *types.Interface {
	t.Helper()
	obj := pkg.Scope().Lookup(name)
	if obj == nil {
		t.Fatalf("%s is not declared in this package, so this gate measures nothing", name)
	}
	iface, ok := obj.Type().Underlying().(*types.Interface)
	if !ok {
		t.Fatalf("%s is not an interface", name)
	}
	return iface
}

// classifiedIn is the set of type names appearing as a case in ClassifyPipe's
// type switch -- from the syntax tree, so a name in a comment is not a case.
func classifiedIn(t *testing.T, fset *gotoken.FileSet, files []*ast.File) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	found := false
	for _, f := range files {
		for _, d := range f.Decls {
			fn, isFunc := d.(*ast.FuncDecl)
			if !isFunc || fn.Name.Name != "ClassifyPipe" || fn.Recv != nil {
				continue
			}
			found = true
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				cc, isCase := n.(*ast.CaseClause)
				if !isCase {
					return true
				}
				for _, e := range cc.List {
					if name := typeNameOf(e); name != "" {
						out[name] = true
					}
				}
				return true
			})
		}
	}
	if !found {
		t.Fatal("ClassifyPipe was not found, so this gate measures nothing")
	}
	return out
}

// typeNameOf is the identifier of `*T` or `T` in a case clause.
func typeNameOf(e ast.Expr) string {
	if star, isStar := e.(*ast.StarExpr); isStar {
		e = star.X
	}
	if id, isIdent := e.(*ast.Ident); isIdent {
		return id.Name
	}
	return ""
}
