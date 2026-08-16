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
	claims := regexp.MustCompile(`(?i)(\bprevents?\b|\bexists to\b|\bexists for\b|` +
		`\bwould otherwise\b|\botherwise would\b|\bso a caller can tell\b|` +
		`\bso an operator can tell\b|\bguards? against\b|\bstops? a\b|` +
		`\bis what lets a caller tell\b|\brather than\b|\bsilently\b|` +
		`\bwithout (?:anyone|a caller|an operator) (?:noticing|being told)\b|` +
		// ValidateClusterBackup's doc -- "a restore that discovers the mismatch
		// halfway has already written some of it" -- named a failure it prevents
		// in none of the words above, so the gate never considered the very
		// declaration its own header cites as one of the four it exists to
		// catch. Chasing phrases is whack-a-mole; these are the shapes this
		// repository actually writes.
		`\bhas already\b|\bwould have\b|\bcalled BEFORE\b|\bbefore anything\b|` +
		`\btoo late\b|\bhalfway\b|\bunrecoverable\b|\bcannot be undone\b)`)

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
		// A file this build EXCLUDES is not a reader.
		//
		// parser.ParseFile ignores build constraints, so a mechanism whose only
		// caller sits in a //go:build plan9 file counted as wired on linux. This
		// repository has twelve build-tagged production files, including
		// windows-only and plan9-only ones.
		if buildExcluded(f) {
			return nil
		}
		isTest := strings.HasSuffix(path, "_test.go")

		// PRODUCTION reads only.
		//
		// Two tightenings over the first version of this gate, which counted any
		// identifier anywhere and consequently reported zero unreferenced
		// declarations while a reviewer was naming four unwired mechanisms:
		//
		//   - a test reference does not wire a mechanism to production.
		//     ValidateClusterBackup had test callers and no production one, and
		//     "it has a test" is exactly the reassurance that let it ship
		//     unused.
		//   - an assignment LHS and a composite-literal key are WRITES. A field
		//     that is only ever written is the shape this gate is named for:
		//     PeerResponse.HighWatermark was parsed into on every response and
		//     branched on nowhere.
		//
		// The count is of read positions in non-test files, minus the
		// declaration itself.
		if !isTest {
			countReads(f, uses)
		}
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
		// One read is the declaration's own name in its signature or spec.
		if _, ok := exempt[d.name]; uses[d.name] <= 1 && !ok {
			unwired = append(unwired, d)
		}
	}
	for _, d := range unwired {
		t.Errorf("%s:%d %s documents a failure it prevents and is READ nowhere in "+
			"this module's non-test code: the mechanism was built and wired to "+
			"nothing. Wire it, or add it to `exempt` with the reason",
			d.file, d.line, d.name)
	}
	// The other direction: an exemption that is no longer needed silently
	// exempts its name from every future run.
	for name, why := range exempt {
		if uses[name] <= 1 {
			continue
		}
		t.Errorf("exempt names %q, which now HAS a production reader. Delete the "+
			"entry -- an exemption nobody removes is how the next unwired "+
			"mechanism gets in under it: %s", name, why)
	}
	t.Logf("%d declarations document a failure they prevent; %d with no production reader",
		len(documented), len(unwired))
}

// exempt is the BASELINE this gate was retrofitted over: everything that had no
// production reader on the day it was written, with what each one is.
//
// It is a ratchet in both directions. A name NOT here that has no production
// reader fails, so nothing new gets in. A name here that GAINS one also fails,
// so an entry cannot outlive its reason and quietly cover the next unwired
// mechanism. Empty is the goal state.
//
// It is a baseline and not an excuse: several of these are the same defect the
// reviewer named, sitting in the open. Task #431 works them down.
var exempt = map[string]string{
	// DELIBERATE test-only hooks. These exist to let a test reach a failure
	// path that production must never take, so "no production reader" is the
	// design and not a defect. They are the reason this gate cannot simply
	// fail on everything it finds.
	"SetFaultHookForTest":     "fault injection: a test-only hook by name and by design",
	"SetDiskUsageForTest":     "same -- a test forces a disk-usage reading production reads from the OS",
	"NonWriteFaultPointNames": "the fault-point inventory a test enumerates; production injects nothing",
	"FailAt":                  "arms a fault point; production never arms one",

	// GENUINELY UNWIRED. Each is a mechanism that was built, documented with
	// the failure it prevents, and never connected -- which is what this gate
	// was written to surface. Listed so the gate can run today rather than
	// staying off until every one is fixed.
	// ReplicasConsulted is NOT here any more, and the reason is worth keeping.
	// Its entry said "read by nothing", which was false under the rule this gate
	// now applies: encoding/json reads it by reflection at cluster_backup.go:196
	// and it ships in cluster.json. Accurately: no Go code BRANCHES on it. The
	// gate cannot see reflection and does not pretend to.
	"ValidateClusterBackup": "no production caller. Its doc says it is 'called BEFORE anything is unpacked' because 'a restore that discovers the mismatch halfway has already written some of it' -- and the restore path does not call it. The example this gate's own header cites, which the first two versions of the gate could not see",
	"routeCount":            "the audit DOES call it -- route_audit_test.go:38, route_count_test.go:23, contracts_test.go:333 -- and the audit is a test, which is the actual reason. The doc block at server.go:602 describes registeredPaths and is attached to routeCount, which is why the gate sees this name at all",
	"ExecuteCount":          "an Executor method with no caller",
	"AppendGroupIdempotent": "one of TWO idempotent-write entry points, not the only one: production deliberately takes CommittedWrite + FlushWithReceipt -> Store.CommitReceipt (writer.go:678), and receipts.go:192-201 says the split is intentional. It is still the one that commits the receipt in the SAME record as the group, which is the guarantee production does not get -- docs/wrong.md entry 54, task #433",
	"QuarantinedGroups":     "the COUNT reaches production (countQuarantined -> the simdlogs_storage_quarantined_groups gauge); only the LISTING -- which group, why, how many bytes, when -- has no reader, so an operator can alert on the number and cannot ask what it is",
	"SnapshotAll":           "superseded by SnapshotAllWithSeq, which is what callers use",
	"RestoreTar":            "the older unstaged restore path; protocols.go:27 says the staged one replaced it",
	"SetMaxRows":            "a dead exported setter: -search.maxRows reaches the server through config (server.go:300), so the LIMIT works and this way of setting it has no caller",
	"SetDirRereadInterval":  "the same, one flag over: -readiness-reread-interval arrives via config.DirRereadInterval (server.go:244) and this setter is unused",
	"readiness":             "server.go:2045 is a SUPERSEDED readiness handler; /-/ready goes to s.healthHandler(healthReady) at server.go:716. The earlier reason blamed 'a name shared with unrelated prose', which describes a mechanism this gate does not have -- comments never contribute to uses",
	"FieldRequestID":        "a log field constant no log line uses",
	"FieldStatus":           "same",
	"FieldDurationMS":       "same",
	"FieldRows":             "same",
}

// buildExcluded reports whether f carries a //go:build line naming a GOOS or
// GOARCH other than this one. Deliberately crude: it is a filter on "this file
// is compiled for a different platform", not a constraint evaluator, and it
// errs towards INCLUDING a file, so a reader is never wrongly discounted.
func buildExcluded(f *ast.File) bool {
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			line, ok := strings.CutPrefix(c.Text, "//go:build ")
			if !ok {
				continue
			}
			for _, plat := range []string{"windows", "plan9", "darwin", "js", "wasip1",
				"aix", "solaris", "freebsd", "openbsd", "netbsd", "ios", "android"} {
				if strings.Contains(line, plat) && !strings.Contains(line, "linux") {
					return true
				}
			}
		}
	}
	return false
}

// countReads adds every READ of an identifier in f to uses.
//
// A read is any appearance except: the left-hand side of an assignment, a
// composite-literal key, and a struct field's own declaration. Those are writes
// and declarations, and a mechanism that is only ever written to is precisely
// what this gate exists to find -- PeerResponse.HighWatermark was assigned on
// every peer response and read by no branch.
//
// Name-based and therefore conservative in one direction only: two different
// types with a same-named field share a count, so it under-reports and never
// invents a finding. It also cannot see WHICH path reads a name, so a field read
// by one subsystem and ignored by another -- HighWatermark again, read by the
// backup path and not the read path -- is invisible to it. That is a real limit
// and not a claim this gate makes.
func countReads(f *ast.File, uses map[string]int) {
	writes := map[ast.Node]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, lhs := range x.Lhs {
				writes[lhs] = true
			}
			// A KeyValueExpr key is NOT reliably a write.
			//
			// In a STRUCT literal it names a field and is one. In a MAP literal
			// -- and an indexed array or slice literal -- the key is a value
			// expression and is a READ, so a documented constant used only as a
			// map key was reported as unwired while a production function read
			// it. Telling the two apart needs the literal's type, which this
			// gate does not resolve, so it takes the conservative branch: keys
			// count as reads, and a field that is only ever assigned inside a
			// struct literal is no longer detected. A missed detection beats a
			// gate that fails on correct code.
		}
		return true
	})
	ast.Inspect(f, func(n ast.Node) bool {
		if writes[n] {
			// The value being assigned INTO is a write; an index or a receiver
			// inside it is still read (`m[k].F = v` reads m, k and F).
			switch x := n.(type) {
			case *ast.Ident:
				return false
			case *ast.SelectorExpr:
				ast.Inspect(x.X, func(inner ast.Node) bool {
					switch y := inner.(type) {
					case *ast.SelectorExpr:
						uses[y.Sel.Name]++
					case *ast.Ident:
						uses[y.Name]++
					}
					return true
				})
				return false
			}
		}
		switch x := n.(type) {
		case *ast.SelectorExpr:
			uses[x.Sel.Name]++
		case *ast.Ident:
			uses[x.Name]++
		}
		return true
	})
}
