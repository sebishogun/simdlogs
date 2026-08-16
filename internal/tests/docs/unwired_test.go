package docs

import (
	"go/ast"
	"go/build"
	"go/parser"
	gotoken "go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
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
		if buildExcluded(path) {
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
		//   - an assignment LHS is a WRITE. A composite-literal KEY is NOT --
		//     it is a write only in a struct literal, and telling that from a
		//     map or indexed-array literal needs the literal's type, which this
		//     gate does not resolve. So the founding shape is caught when the
		//     field is written by assignment and MISSED when it is written
		//     inside a composite literal, which is how PeerResponse.HighWatermark
		//     and ReplicasConsulted were both written. See the note above
		//     countReads; the substitute is a reader, not a smarter regex.
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
	//
	// QuarantinedGroups is NOT here any more either. /admin/storage/quarantine
	// serves it through Store.Quarantined, so the LISTING an operator could not
	// reach -- which group, why, how many bytes, when -- is reachable now. Its
	// entry said the count reaches production and only the listing does not,
	// which was exactly right and exactly the gap.
	//
	// ValidateClusterBackup is NOT here any more. It has a production caller:
	// cmd/simdlogs/restore.go runs it when the archive it was handed turns out
	// to be a cluster one, before quoting what the manifest claims. That was
	// the example this gate's own header cites, and it is the first entry the
	// ratchet has removed.
	// ReplicasConsulted is NOT here any more, and the reason is worth keeping.
	// Its entry said "read by nothing", which was false under the rule this gate
	// now applies: encoding/json reads it by reflection at cluster_backup.go:196
	// and it ships in cluster.json. Accurately: no Go code BRANCHES on it. The
	// gate cannot see reflection and does not pretend to.
	"routeCount":           "the audit DOES call it -- route_audit_test.go:38, route_count_test.go:23, contracts_test.go:333 -- and the audit is a test, which is the actual reason. The doc block at server.go:602 describes registeredPaths and is attached to routeCount, which is why the gate sees this name at all",
	"ExecuteCount":         "an Executor method with no PRODUCTION caller -- executor_test.go:221, :231 and :329 do call it, and an earlier version of this note said \"no caller\" flat",
	"SnapshotAll":          "superseded by SnapshotAllWithSeq, which is what callers use",
	"RestoreTar":           "the older unstaged restore path; protocols.go:27 says the staged one replaced it",
	"SetMaxRows":           "a dead exported setter: -search.maxRows reaches the server through config (server.go:300), so the LIMIT works and this way of setting it has no caller",
	"SetDirRereadInterval": "the same, one flag over: -readiness-reread-interval arrives via config.DirRereadInterval (server.go:244) and this setter is unused",
	"readiness":            "server.go:2045 is a SUPERSEDED readiness handler; /-/ready goes to s.healthHandler(healthReady) at server.go:716. The earlier reason blamed 'a name shared with unrelated prose', which describes a mechanism this gate does not have -- comments never contribute to uses",
	"FieldRequestID":       "a log field constant no log line uses",
	"FieldStatus":          "same",
	"FieldDurationMS":      "same",
	"FieldRows":            "same",
}

// buildExcluded reports whether f is compiled for a DIFFERENT platform than
// this one, so its declarations do not count as readers here.
//
// It asks go/build/constraint -- the COMPILER'S OWN parser -- rather than
// evaluating the expression itself. Two hand-rolled versions were wrong in
// opposite directions:
//
//   - `strings.Contains(line, plat) && !strings.Contains(line, "linux")`
//     excluded every `//go:build !windows` file, three of them in
//     internal/storage and all compiled here.
//   - a small recursive evaluator treated an unmodelled tag as SATISFIED,
//     which is the safe direction UNTIL it appears under a negation:
//     `!purego` then evaluated false and excluded a file the compiler
//     includes. Its doc claimed the opposite, and the test asserted
//     `{"!purego", false}` and `{"(linux || darwin) && !cgo", false}` -- both
//     contradicted by go/build, the second under CGO_ENABLED=0, which is this
//     repository's stated posture.
//
// The evaluator also hardcoded linux/amd64, so a test named "agrees with the
// compiler" was false on every cross-architecture lane. runtime.GOOS and
// runtime.GOARCH now, and build.Default for everything else.
//
// And the constraint is read from the file's LEADING comment block only.
// Scanning every comment meant a `//go:build windows` line quoted INSIDE a doc
// comment -- which this very file contains, twice -- was evaluated as the
// file's constraint. They survive only because they are not at the start of
// their line.
func buildExcluded(path string) bool {
	ok, err := build.Default.MatchFile(filepath.Dir(path), filepath.Base(path))
	if err != nil {
		// Unreadable or unparseable is not this gate's subject; INCLUDE, which
		// is the direction that never discounts a real reader.
		return false
	}
	return !ok
}

// countReads adds every READ of an identifier in f to uses.
//
// A read is any appearance except the left-hand side of an assignment and a
// struct field's own declaration.
//
// A composite-literal KEY counts as a READ, and that is a deliberate loss. It
// is a write in a struct literal and a read in a map or indexed-array literal,
// and telling the two apart needs the literal's resolved type. Treating every
// key as a write failed on correct code -- a documented constant used only as a
// map key was reported unwired while production read it. Treating every key as
// a read costs a detection instead: a field written ONLY inside a composite
// literal is no longer found, which is how PeerResponse.HighWatermark and
// ReplicasConsulted were written. A field written by ASSIGNMENT and never read
// still is.
//
// A gate that fails on correct code stops being trusted when it goes red, which
// is worse than one that misses a case. What covers the missed case is a
// reader: a test that asks whether the field changes any decision.
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

// The three files the substring version dropped are counted again.
//
// Named explicitly rather than left to the aggregate count: the aggregate went
// 254 -> 256 and either number could be wrong for a different reason.
func TestTheUnixOnlyStorageFilesCountAsReaders(t *testing.T) {
	for _, rel := range []string{
		"internal/storage/atomicfile_unix.go",
		"internal/storage/lock_unix.go",
		"internal/storage/flock_unix.go",
	} {
		path := filepath.Join(repoRoot, rel)
		if _, err := os.Stat(path); err != nil {
			t.Skipf("%s is gone; if it moved, move this list with it", rel)
		}
		if buildExcluded(path) {
			t.Errorf("%s is excluded, and it is compiled on linux: every name read "+
				"only from here looks unwired", rel)
		}
	}
}

// buildExcluded agrees with the compiler on every file in the tree.
//
// Trivially true now that it asks go/build, and kept because the two hand-rolled
// versions it replaced were each wrong on real files here -- one excluding every
// `!windows` file, the other excluding anything under `!<unmodelled tag>`. If a
// future change reintroduces a local evaluator "for speed", this is the check it
// has to pass.
func TestBuildExcludedAgreesWithTheCompiler(t *testing.T) {
	checked, excluded := 0, 0
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		checked++
		want, merr := build.Default.MatchFile(filepath.Dir(path), filepath.Base(path))
		if merr != nil {
			return nil
		}
		got := buildExcluded(path)
		if got == want { // buildExcluded is the NEGATION of MatchFile
			t.Errorf("%s: buildExcluded=%v and the compiler's MatchFile=%v",
				path, got, want)
		}
		if got {
			excluded++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 100 {
		t.Errorf("only %d .go files walked; the tree has hundreds, so this "+
			"comparison ran on almost nothing", checked)
	}
	t.Logf("%d files, %d excluded on %s/%s", checked, excluded, runtime.GOOS, runtime.GOARCH)
}

// A `//go:build` line quoted inside a doc comment is not the file's constraint.
//
// Scanning every comment in the file made one -- and this file contains two --
// the constraint for the whole file, discounting every reader in it. go/build
// reads the leading block only, which is what the compiler does.
func TestAQuotedBuildLineIsNotAConstraint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.go")
	body := "package sample\n\n" +
		"// Files that are windows-only carry\n" +
		"//go:build windows\n" +
		"// at the top; this comment is NOT a constraint.\n" +
		"func F() {}\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if buildExcluded(path) {
		t.Error("a //go:build line inside a doc comment was read as the file's " +
			"constraint, so every declaration read from that file looks unwired")
	}
}
