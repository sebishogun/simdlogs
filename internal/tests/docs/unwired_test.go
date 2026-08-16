package docs

import (
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Anything whose documentation names a failure it prevents must have a caller.
//
// Five review rounds asked whether the code matched its comment. None asked
// whether anything CALLS the thing the comment describes, and round six found
// four mechanisms that were built, documented, and wired to nothing:
//
//	PeerResponse.HighWatermark   defined, documented for exactly the
//	                             lagging-replica short read, populated on the
//	                             wire, parsed by the client, read by no read path
//	Writer.ValidateVector        exported, its doc names the silent failure it
//	                             prevents, no caller
//	X-Simdlogs-Complete          enforced by the read path, ignored four lines
//	                             from where the backup path parses it
//	ValidateClusterBackup        no production caller
//
// The reviewer's own words: a grep for callers of every field and function
// whose doc names a failure it prevents is cheaper than another live cluster,
// and it would have found three of these before the servers were started.
//
// This is that grep, as a gate. It is NAME-based rather than type-resolved,
// which makes it conservative in one direction only: a name used anywhere in
// the module counts as wired, so it under-reports and never invents a finding.
func TestDocumentedMechanismsHaveCallers(t *testing.T) {
	// A doc comment that claims to prevent something, in the words this
	// repository actually uses.
	claims := regexp.MustCompile(`(?i)\b(prevents?|exists to|exists for|would otherwise|` +
		`otherwise would|so a caller can tell|so an operator can tell|guards? against|` +
		`stops? a|is what lets a caller tell)\b`)

	fset := gotoken.NewFileSet()
	type decl struct {
		name, file string
		line       int
	}
	var documented []decl
	uses := map[string]int{}

	err := filepath.WalkDir(repoRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == ".git" || n == "testdata" || n == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			return nil // a file that does not parse is not this gate's subject
		}
		isTest := strings.HasSuffix(path, "_test.go")

		// Every identifier used anywhere counts as a reference, including in
		// tests: a mechanism exercised only by a test is still wired to
		// something, and this gate is about mechanisms wired to NOTHING.
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				uses[x.Sel.Name]++
			case *ast.Ident:
				uses[x.Name]++
			}
			return true
		})
		if isTest {
			return nil // a documented test helper is not a shipped mechanism
		}

		rel, _ := filepath.Rel(repoRoot, path)
		record := func(name string, doc *ast.CommentGroup, pos gotoken.Pos) {
			if doc == nil || name == "" || name == "_" {
				return
			}
			if !claims.MatchString(doc.Text()) {
				return
			}
			documented = append(documented, decl{name, rel, fset.Position(pos).Line})
		}
		for _, d := range f.Decls {
			switch x := d.(type) {
			case *ast.FuncDecl:
				record(x.Name.Name, x.Doc, x.Pos())
			case *ast.GenDecl:
				for _, sp := range x.Specs {
					switch s := sp.(type) {
					case *ast.TypeSpec:
						if st, ok := s.Type.(*ast.StructType); ok {
							for _, fld := range st.Fields.List {
								for _, nm := range fld.Names {
									record(nm.Name, fld.Doc, nm.Pos())
								}
							}
						}
					case *ast.ValueSpec:
						doc := s.Doc
						if doc == nil {
							doc = x.Doc
						}
						for _, nm := range s.Names {
							record(nm.Name, doc, nm.Pos())
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(documented) == 0 {
		t.Fatal("no declaration in this repository documents a failure it prevents, " +
			"which cannot be true -- this gate is matching nothing")
	}

	var unwired []decl
	for _, d := range documented {
		// One use is the declaration itself.
		if uses[d.name] <= 1 {
			unwired = append(unwired, d)
		}
	}
	for _, d := range unwired {
		t.Errorf("%s:%d %s documents a failure it prevents and is referenced nowhere "+
			"else in this module: the mechanism was built and wired to nothing",
			d.file, d.line, d.name)
	}
	t.Logf("%d declarations document a failure they prevent; %d unreferenced",
		len(documented), len(unwired))
}
