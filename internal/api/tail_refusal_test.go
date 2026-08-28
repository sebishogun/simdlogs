package api

import (
	"fmt"
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sebishogun/simdlogs/internal/query"
)

// tailCase is one pipe of the language: how LogsQL spells it, the type it
// parses to, and what a live tail answers for it.
//
// `name` and `why` are what `nonStreamingPipe` returns; both empty means the
// pipe RUNS on a tail. The values are not a design document -- they are the
// measured answers, and TestEveryPipeInTheLanguageHasATailRefusal compares them
// exactly.
//
// WHAT THIS TABLE CANNOT SEE, because `why` is spelled as the CONSTANT rather
// than as a literal: the constant being reworded. That changes the server's
// answer and the expectation together. Two other tests carry it --
// TestTheTailRefusalReasonsSayThreeDifferentThings pins each reason's
// distinguishing phrase as a literal, and
// TestTheAPILLDsTailSectionNamesWhatTheCodeRefuses requires each reason's FULL
// TEXT in docs/lld/api.md under the bullet naming that reason's pipes. So a
// wording change has to land in the code and in the document together, and a
// reason rewritten into a different claim is red rather than silent.
type tailCase struct {
	typ    string // type name in internal/query, e.g. "LimitPipe"
	logsql string // a query whose only pipe is that type
	name   string // the name the refusal gives the pipe, "" when it streams
	why    string // the reason, "" when it streams
}

// Every pipe in the language. The set is checked against the query package's
// source below, so a pipe added there and not added here is a failure rather
// than a silent gap.
var tailCases = []tailCase{
	// ROW-LOCAL: a function of one row, so a stream is the same as a batch.
	{typ: "FieldsPipe", logsql: `* | fields _msg`},
	{typ: "RenamePipe", logsql: `* | rename a as b`},
	{typ: "DeletePipe", logsql: `* | delete a`},
	{typ: "CopyPipe", logsql: `* | copy a as b`},
	{typ: "FilterPipe", logsql: `* | filter level:info`},
	{typ: "FormatPipe", logsql: `* | format "<a>" as b`},
	{typ: "ExtractPipe", logsql: `* | extract "<a>"`},
	{typ: "ExtractRegexpPipe", logsql: `* | extract_regexp "(?P<a>.+)"`},
	{typ: "MathPipe", logsql: `* | math "n + 1" as m`},
	{typ: "LenPipe", logsql: `* | len(a) as b`},
	{typ: "HashPipe", logsql: `* | hash(a) as b`},
	{typ: "UnpackJSONPipe", logsql: `* | unpack_json`},
	{typ: "UnpackLogfmtPipe", logsql: `* | unpack_logfmt`},
	{typ: "UnpackSyslogPipe", logsql: `* | unpack_syslog`},
	{typ: "UnpackWordsPipe", logsql: `* | unpack_words`},
	{typ: "ReplacePipe", logsql: `* | replace ("a", "b")`},
	{typ: "DecolorizePipe", logsql: `* | decolorize`},
	{typ: "PackPipe", logsql: `* | pack_json`},
	{typ: "DropEmptyPipe", logsql: `* | drop_empty_fields`},
	{typ: "JSONArrayLenPipe", logsql: `* | json_array_len(a) as b`},
	{typ: "CollapseNumsPipe", logsql: `* | collapse_nums`},
	{typ: "UnrollPipe", logsql: `* | unroll (a)`},

	// COMPUTED OVER THE WHOLE RESULT SET.
	{typ: "StatsPipe", logsql: `* | stats count() c`, name: "stats", why: whyNeverFinal},
	{typ: "SortPipe", logsql: `* | sort by (_time)`, name: "sort", why: whyNeverFinal},
	{typ: "UniqPipe", logsql: `* | uniq by (level)`, name: "uniq", why: whyNeverFinal},
	{typ: "TopPipe", logsql: `* | top 1 by (level)`, name: "top", why: whyNeverFinal},
	{typ: "RankPipe", logsql: `* | rank "error"`, name: "rank", why: whyNeverFinal},
	{typ: "FieldValuesPipe", logsql: `* | field_values level`, name: "field_values", why: whyNeverFinal},
	{typ: "FieldNamesPipe", logsql: `* | field_names`, name: "field_names", why: whyNeverFinal},
	{typ: "FacetsPipe", logsql: `* | facets`, name: "facets", why: whyNeverFinal},
	{typ: "BlocksCountPipe", logsql: `* | blocks_count`, name: "blocks_count", why: whyNeverFinal},
	{typ: "BlockStatsPipe", logsql: `* | block_stats`, name: "block_stats", why: whyNeverFinal},

	// SLICES OF A RESULT SET, and each poll is its own result set.
	{typ: "LimitPipe", logsql: `* | limit 2`, name: "limit", why: whyPerPoll},
	{typ: "OffsetPipe", logsql: `* | offset 2`, name: "offset", why: whyPerPoll},
	{typ: "TailPipe", logsql: `* | tail 2`, name: "tail", why: whyPerPoll},
	{typ: "SamplePipe", logsql: `* | sample 2`, name: "sample", why: whyPerPoll},

	// REACHES OUTSIDE THE ROWS IT WAS GIVEN.
	{typ: "JoinPipe", logsql: `* | join by (a) (*)`, name: "join", why: whySecondSet},
	{typ: "UnionPipe", logsql: `* | union (*)`, name: "union", why: whySecondSet},
	{typ: "StreamContextPipe", logsql: `* | stream_context before 1`, name: "stream_context", why: whySecondSet},
}

// THE REASONS ARE THREE DIFFERENT REASONS, and each says its own thing.
//
// The table above and the endpoint cases in time_reach_test.go both spell their
// expectations as `whyNeverFinal` / `whyPerPoll` / `whySecondSet`, which is what
// keeps them in step with the server -- and it means neither of them can see the
// reasons being MERGED: collapsing `whyPerPoll` into the text of `whyNeverFinal`
// changes both the server's answer and the expectation, and every case stays
// green. Measured, with `whyPerPoll` given `whyNeverFinal`'s text: the whole
// package passed.
//
// So the distinguishing phrase of each reason is written out here, once, as a
// literal. This is the only place the wording is duplicated, and it is the place
// where duplication is the point.
func TestTheTailRefusalReasonsSayThreeDifferentThings(t *testing.T) {
	reasons := []struct{ name, why, saysThis string }{
		// `| stats`, `| sort`, `| uniq`, `| top`, `| rank` and the
		// introspection pipes: no final answer over an input that never ends.
		{"whyNeverFinal", whyNeverFinal, "never ends"},
		// `| limit`, `| offset`, `| tail`, `| sample`: `rows[:N]` and `rows[N:]`
		// are prefixes, and the poll is what they would be a prefix OF.
		{"whyPerPoll", whyPerPoll, "once per poll"},
		// `| join`, `| union`, `| stream_context`.
		{"whySecondSet", whySecondSet, "second result set"},
		// The default, which no pipe of the language reaches.
		{"whyNoStreamingForm", whyNoStreamingForm, "no streaming form"},
	}
	for _, r := range reasons {
		if !strings.Contains(r.why, r.saysThis) {
			t.Errorf("%s no longer says %q:\n%s\nA refusal reason that stopped "+
				"naming its own mechanism is a reason for some other pipe",
				r.name, r.saysThis, r.why)
		}
	}
	for i := range reasons {
		for j := i + 1; j < len(reasons); j++ {
			if reasons[i].why == reasons[j].why {
				t.Errorf("%s and %s are now the same string, so one of the two "+
					"groups of pipes is being told the other's reason:\n%s",
					reasons[i].name, reasons[j].name, reasons[i].why)
			}
		}
	}
}

// EVERY PIPE OF THE LANGUAGE, MEASURED -- not a bucket list written by hand.
//
// docs/lld/api.md described the refusals as two buckets and was wrong about
// each of them four ways: it put `| stats` whole in the "needs an input that
// ends" bucket when only the mergeable-aggregate half was there; it named the
// other bucket as exactly sample/join/union/stream_context when six
// introspection pipes and the non-mergeable half of stats were in it too; it
// said `| limit` and `| offset` "need the whole result set before they can emit
// their first row", which `rows[:N]` and `rows[N:]` are not; and it did not
// mention `| tail` at all, which is refused.
//
// A hand-written enumeration is what produced all four, so this one is not
// hand-written: the table above is compared against the pipe set taken from the
// query package's SOURCE, and every entry's name and reason are the strings
// `nonStreamingPipe` actually returns.
func TestEveryPipeInTheLanguageHasATailRefusal(t *testing.T) {
	inTable := map[string]bool{}
	for _, c := range tailCases {
		if inTable[c.typ] {
			t.Fatalf("%s is listed twice, so one of the two entries is not being "+
				"checked against anything", c.typ)
		}
		inTable[c.typ] = true
	}
	inLanguage := pipeTypesInTheLanguage(t)
	for typ := range inLanguage {
		if !inTable[typ] {
			t.Errorf("%s is a pipe of the language with no case here, so nothing "+
				"says whether a tail runs it, refuses it, or what it tells the "+
				"caller when it does", typ)
		}
	}
	for typ := range inTable {
		if !inLanguage[typ] {
			t.Errorf("this file has a case for %s, which is not a pipe of the "+
				"language -- a renamed or deleted type leaves an assertion that "+
				"measures nothing", typ)
		}
	}

	for _, c := range tailCases {
		t.Run(c.typ, func(t *testing.T) {
			q, err := query.ParseLogsQL(c.logsql)
			if err != nil {
				t.Fatalf("%q does not parse: %v", c.logsql, err)
			}
			if len(q.Pipes) != 1 {
				t.Fatalf("%q parses to %d pipes; a case that measures a chain "+
					"cannot say which pipe answered", c.logsql, len(q.Pipes))
			}
			if got := fmt.Sprintf("%T", q.Pipes[0]); got != "*query."+c.typ {
				t.Fatalf("%q parses to %s, not *query.%s -- so this case measures "+
					"a different pipe than the one it names", c.logsql, got, c.typ)
			}

			name, why := nonStreamingPipe(q.Pipes)
			if name != c.name || why != c.why {
				t.Errorf("a tail answers %q for %q\n got name %q\nwant name %q\n"+
					" got why  %q\nwant why  %q", c.logsql, c.typ, name, c.name, why, c.why)
			}
			if name == "" {
				return
			}
			// THE NAME MUST BE A TOKEN OF THE LANGUAGE. The lowered Go type
			// name is not: it turned `stream_context` into `streamcontext`,
			// `field_values` into `fieldvalues` and `block_stats` into
			// `blockstats` -- five names a caller cannot paste back into a
			// query, from the one message whose job is to say which pipe to
			// remove.
			if !strings.Contains(c.logsql, name) {
				t.Errorf("%s is refused as %q, which does not appear in %q. A "+
					"refusal naming a token the language does not have cannot be "+
					"acted on", c.typ, name, c.logsql)
			}
			if why == whyNoStreamingForm {
				t.Errorf("%s reaches tailRefusal's default and is refused with a "+
					"generic reason. The default exists so a new pipe is refused "+
					"rather than run, not as a place for pipes to live", c.typ)
			}
		})
	}
}

// EVERY `| stats` IS REFUSED FOR THE SAME REASON, whatever its aggregates are.
//
// The message used to come from `ClassifyPipe`, which routes *StatsPipe by
// `mergeableAggs` -- whether a COORDINATOR can combine partial states. Measured
// on the endpoint before this changed, one row per query `* | stats <agg>`:
//
//	count() c            it needs the whole result set and the input never ends
//	avg(d) a             it runs once over a merged result set, which a stream does not have
//	quantile(0.5, d) q   ... the same
//	count_uniq(level) u  ... the same
//	histogram(d) h       ... the same
//	rate() r             ... the same
//
// A live tail has no coordinator and no merge, so the second message names a
// cluster a single-node operator may not have, for a query refused because its
// input never ends. The split is real and belongs to distribution; this test
// asserts the tail's answer does not follow it.
func TestEveryStatsPipeIsRefusedForTheSameReason(t *testing.T) {
	// The control: these two really are on opposite sides of the distribution
	// split. Without it, "both get the same message" would also pass if
	// ClassifyPipe stopped splitting stats at all.
	classes := map[string]query.PipeClass{}
	for _, q := range []string{`* | stats count() c`, `* | stats avg(d) a`} {
		parsed, err := query.ParseLogsQL(q)
		if err != nil {
			t.Fatal(err)
		}
		classes[q] = query.ClassifyPipe(parsed.Pipes[0])
	}
	if classes[`* | stats count() c`] == classes[`* | stats avg(d) a`] {
		t.Fatalf("count() and avg() are both %v, so this test no longer covers "+
			"a stats pipe on each side of the distribution split",
			classes[`* | stats count() c`])
	}

	for _, q := range []string{
		`* | stats count() c`,          // mergeable
		`* | stats sum(d) s`,           // mergeable
		`* | stats avg(d) a`,           // not mergeable: no average of averages
		`* | stats quantile(0.5, d) q`, // not mergeable: no sketch on the wire
		`* | stats count_uniq(level) u`,
		`* | stats histogram(d) h`,
		`* | stats rate() r`,
		`* | stats by (level) count() c`,
	} {
		t.Run(q, func(t *testing.T) {
			parsed, err := query.ParseLogsQL(q)
			if err != nil {
				t.Fatalf("%q does not parse: %v", q, err)
			}
			name, why := nonStreamingPipe(parsed.Pipes)
			if name != "stats" {
				t.Errorf("%q is refused as %q, not `stats`", q, name)
			}
			if why != whyNeverFinal {
				t.Errorf("%q is refused with\n got %q\nwant %q\nA tail has no "+
					"coordinator and no merged result set, so every stats pipe is "+
					"refused for the one reason its input never ends", q, why, whyNeverFinal)
			}
		})
	}
}

// docs/lld/api.md's LIVE TAIL SECTION SAYS WHAT THE CODE DOES, PIPE BY PIPE AND
// BUCKET BY BUCKET.
//
// That section described the refusals as two buckets and was false of the tree
// in four ways at once (see the header of TestEveryPipeInTheLanguageHasATailRefusal).
// Prose drifts from code silently; the pipe names and the counts are cheap to
// check, so they are checked.
//
// PRESENCE IS NOT ENOUGH, and the first version of this gate only asked for
// presence -- that each `| name` and each of three anchor phrases appeared
// SOMEWHERE in the section. Three edits were green under it and all three make
// the document false:
//
//   - moving `| limit` and `| offset` into the never-ends bucket and `| join`
//     out of the second-set bucket: every name still appears, under the wrong
//     reason;
//   - replacing the three buckets with ONE paragraph claiming all 17 are
//     refused because the input never ends -- false for 7 of them -- while
//     mentioning the other two phrases in prose as ones the endpoint does not
//     use;
//   - rewording `whyPerPoll` into a different claim ("... and this build has
//     not implemented a streaming form of it") and leaving the document
//     describing the old one.
//
// So the gate is bucket-aware and reason-complete: the section must carry three
// distinct bullets, each containing one reason's FULL TEXT, and each refused
// pipe must be named under the bullet carrying ITS OWN reason and under no
// other. That is the same pairing the endpoint emits, checked in the document.
func TestTheAPILLDsTailSectionNamesWhatTheCodeRefuses(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "lld", "api.md")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const heading = "\n## Live tail\n"
	i := strings.Index(string(b), heading)
	if i < 0 {
		t.Fatalf("%s has no `## Live tail` section, so this gate measures nothing", path)
	}
	sec := string(b)[i+len(heading):]
	if j := strings.Index(sec, "\n## "); j >= 0 {
		sec = sec[:j]
	}
	if len(sec) < 200 {
		t.Fatalf("%s's Live tail section is %d bytes, which is too short to be "+
			"the section this gate is about", path, len(sec))
	}

	// ONE BULLET PER REASON, carrying that reason's whole text. Matching the
	// full constant rather than an anchor phrase is what makes a reworded reason
	// visible: the document has to be rewritten in the same commit or it stops
	// describing the code.
	bullets := topLevelBullets(sec)
	bucket := map[string]string{} // reason -> the bullet that carries it
	for _, r := range []struct{ name, why string }{
		{"whyNeverFinal", whyNeverFinal},
		{"whyPerPoll", whyPerPoll},
		{"whySecondSet", whySecondSet},
	} {
		var hits []string
		for _, bl := range bullets {
			if strings.Contains(normalizeProse(bl), normalizeProse(r.why)) {
				hits = append(hits, bl)
			}
		}
		if len(hits) != 1 {
			t.Errorf("%s's Live tail section has %d of its %d bullets carrying "+
				"the text of %s, want exactly 1. The document must give each "+
				"refused pipe the reason the endpoint gives it, and a reason "+
				"that is in no bullet -- or in two -- cannot do that:\n%s",
				path, len(hits), len(bullets), r.name, r.why)
			continue
		}
		bucket[r.why] = hits[0]
	}
	for r1 := range bucket {
		for r2 := range bucket {
			if r1 != r2 && bucket[r1] == bucket[r2] {
				t.Fatalf("%s gives two different reasons in one bullet, so the "+
					"pipes under it are being told both:\n%s", path, bucket[r1])
			}
		}
	}

	var rowLocal, refused int
	for _, c := range tailCases {
		if c.name == "" {
			rowLocal++
			continue
		}
		refused++
		own, ok := bucket[c.why]
		if !ok {
			continue // already reported: the reason is in no single bullet
		}
		token := "`| " + c.name + "`"
		if !strings.Contains(own, token) {
			t.Errorf("%s does not name %s in the bullet whose reason is the one "+
				"the endpoint gives it. A caller reading the section is told "+
				"their query is refused for something other than what the 400 "+
				"says:\nreason: %s\nbullet: %s", path, token, c.why, own)
		}
		for _, other := range bullets {
			if other == own || !strings.Contains(other, token) {
				continue
			}
			t.Errorf("%s names %s under a bullet that is not its reason. The "+
				"endpoint refuses it with %q:\n%s", path, token, c.why, other)
		}
	}
	for _, claim := range []string{
		fmt.Sprintf("there are %d", rowLocal),
		fmt.Sprintf("The other %d are refused", refused),
	} {
		if !strings.Contains(sec, claim) {
			t.Errorf("%s's Live tail section does not say %q. The code's answer "+
				"is %d row-local pipes and %d refused, in three reasons",
				path, claim, rowLocal, refused)
		}
	}
}

// topLevelBullets is the `- ` items of a markdown section, each with its
// continuation lines.
//
// A bullet ends at the next `- ` item or at the first line that is neither
// indented nor blank-inside-the-item -- which is what makes "under this bullet"
// a decidable question rather than "somewhere in the section".
func topLevelBullets(sec string) []string {
	var out []string
	cur := -1
	for _, line := range strings.Split(sec, "\n") {
		switch {
		case strings.HasPrefix(line, "- "):
			out = append(out, line)
			cur = len(out) - 1
		case cur >= 0 && strings.HasPrefix(line, "  ") && strings.TrimSpace(line) != "":
			out[cur] += "\n" + line
		default:
			cur = -1
		}
	}
	return out
}

// normalizeProse makes a Go string constant and its markdown rendering
// comparable: em and en dashes become `--`, emphasis markers are dropped, line
// wrapping collapses, and case stops mattering.
//
// Nothing else. A normalizer that also dropped punctuation or backticks would
// let a bullet match a reason it does not actually state.
func normalizeProse(s string) string {
	s = strings.NewReplacer("—", "--", "–", "--", "*", "").Replace(s)
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}

// pipeTypesInTheLanguage is every type of internal/query that is a pipe: the
// ones DECLARING `apply(rows []Row) []Row`, union the ones named as a case in
// `ClassifyPipe`'s type switch.
//
// From the SYNTAX TREE rather than a regexp over the source: a method
// declaration is an ast.FuncDecl and a case is an ast.CaseClause, so a type
// named in a comment is neither, and a value receiver is found as readily as a
// pointer receiver. Those are two of the three holes a reviewer drove through
// the first version of the query package's own gate.
//
// THE UNION IS WHY THIS IS THE PIPE SET AND NOT A METHOD SET. A type that
// PROMOTES apply from an embedded pipe -- `type EmbeddedPipe struct{ FieldsPipe }`
// -- implements Pipe and declares no FuncDecl, so the apply walk alone never
// sees it. Measured: with a ClassifyPipe case added for it, it parsed, reached
// `tailRefusal`'s default, was refused with the generic reason, and every test
// in this file stayed green. The query package's own gate TYPE-CHECKS the
// package and requires a ClassifyPipe case for every type implementing Pipe and
// no case for anything else (TestEveryPipeIsClassifiedExplicitly), so the case
// set IS the Pipe set -- reading it here inherits that without pulling go/types
// and a source importer into this package's test binary, and the apply walk
// stays as the half that does not depend on another package's gate holding.
func pipeTypesInTheLanguage(t *testing.T) map[string]bool {
	t.Helper()
	names, err := filepath.Glob(filepath.Join("..", "query", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	fset := gotoken.NewFileSet()
	out := map[string]bool{}
	declared, classified := 0, 0
	for _, n := range names {
		if strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatalf("%s: %v", n, err)
		}
		for _, d := range f.Decls {
			fn, isFunc := d.(*ast.FuncDecl)
			if !isFunc {
				continue
			}
			if fn.Recv != nil && len(fn.Recv.List) == 1 &&
				fn.Name.Name == "apply" && isRowsToRows(fn.Type) {
				if recv := typeNameOfExpr(fn.Recv.List[0].Type); recv != "" {
					out[recv] = true
					declared++
				}
				continue
			}
			if fn.Recv != nil || fn.Name.Name != "ClassifyPipe" {
				continue
			}
			ast.Inspect(fn.Body, func(nd ast.Node) bool {
				cc, isCase := nd.(*ast.CaseClause)
				if !isCase {
					return true
				}
				for _, e := range cc.List {
					if name := typeNameOfExpr(e); name != "" {
						out[name] = true
						classified++
					}
				}
				return true
			})
		}
	}
	if declared == 0 {
		t.Fatal("no apply method found in ../query, so half this gate measures nothing")
	}
	if classified == 0 {
		t.Fatal("no ClassifyPipe case found in ../query, so the half of this gate " +
			"that sees a promoted apply measures nothing")
	}
	return out
}

// isRowsToRows reports whether a signature is `(rows []Row) []Row`, which is
// what Pipe requires. Without it any method named `apply` would count.
func isRowsToRows(ft *ast.FuncType) bool {
	if ft.Params == nil || len(ft.Params.List) != 1 ||
		ft.Results == nil || len(ft.Results.List) != 1 {
		return false
	}
	return isRowSlice(ft.Params.List[0].Type) && isRowSlice(ft.Results.List[0].Type)
}

func isRowSlice(e ast.Expr) bool {
	arr, isArr := e.(*ast.ArrayType)
	if !isArr || arr.Len != nil {
		return false
	}
	return typeNameOfExpr(arr.Elt) == "Row"
}

// typeNameOfExpr is the identifier of `T` or `*T`.
func typeNameOfExpr(e ast.Expr) string {
	if star, isStar := e.(*ast.StarExpr); isStar {
		e = star.X
	}
	if id, isIdent := e.(*ast.Ident); isIdent {
		return id.Name
	}
	return ""
}
