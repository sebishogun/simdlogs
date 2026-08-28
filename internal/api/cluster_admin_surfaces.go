package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	obs "github.com/sebishogun/simdlogs/internal/observability"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// The two administrative surfaces a router used to answer 501.
//
// Both refusals named a real problem and both problems have the same answer:
// ask the shards.
//
//	listQuarantined      "a router's own store holds no data and quarantines
//	                      nothing, so this would answer an empty list about
//	                      shards it never asked"
//	acknowledgeDegraded  "the router's own store is empty and never degrades;
//	                      acknowledging here clears nothing on the shards that
//	                      are actually degraded"
//
// The 501 was right that the router's LOCAL store cannot answer. It is a
// select-router: it holds no data, so every local answer is a confident
// nothing. But an operator pointed at a cluster's admin endpoint is asking
// about the cluster, and the router is the only node that knows the whole
// shard list.
//
// # They obey the SAME completeness rule as every read
//
// A quarantine listing that silently omits a shard is exactly the failure the
// 501 was protecting against, one level out: "nothing is quarantined" and "I
// could not ask two of five shards" look identical if the missing shards are
// dropped. And a partial acknowledgement is worse than a partial read -- this
// is the one action whose meaning is a person accepting data loss, so a summed
// count with a shard missing is a person accepting something that did not
// happen.
//
// The LISTING goes through fanOutChecked, which is where that rule lives: 503
// naming the missing shards and how to opt in, 206 with X-Simdlogs-Partial
// when the caller does opt in, and the shards-total/answered/missing headers
// either way. Writing the rule again for it is how a second contract appears.
//
// The ACKNOWLEDGEMENT does not, and the difference is not an inconsistency to
// tidy away: it is a write. fanOutChecked's refusal says the answer "would
// have been missing data ... so it is refused", which is false of an operation
// that has already been carried out on every shard it could reach. It keeps
// the same status and the same headers and writes its own body. It does not
// honour allow_partial_response and never answers 206, because a partial
// WRITE is not something a caller opts into -- it is what happened, and the
// body says so. Its own doc comment below has the whole argument.

// noQueryCtx marks a request whose route has no query language.
type noQueryCtx struct{}

// routeTakesNoQuery reports the mark aboutHealth set. (An earlier version of
// this line named `withoutQueryLanguage`, which is not an identifier in this
// package and never was.)
func routeTakesNoQuery(r *http.Request) bool {
	v, _ := r.Context().Value(noQueryCtx{}).(bool)
	return v
}

// healthSubjectCtx marks a request whose SUBJECT is the shards' own health.
type healthSubjectCtx struct{}

// aboutTheShardsHealth reports whether this route is asking about the shards'
// health rather than about their data.
//
// It decides one thing: whether a shard answering "I am degraded" makes the
// answer incomplete. For a read it does -- the shard's rows are missing. For
// these two it does not, and treating it the same way refuses them in exactly
// the case they exist for. A quarantine listing is refused because there is
// something quarantined; an acknowledgement is refused because there is
// something to acknowledge.
//
// Marked on the REQUEST rather than matched by path inside the fan-out,
// because the fan-out already takes the path as a string and a second,
// stringly-typed rule keyed on the same value is how the two drift.
func aboutTheShardsHealth(r *http.Request) bool {
	v, _ := r.Context().Value(healthSubjectCtx{}).(bool)
	return v
}

// aboutHealth marks r with both: these routes take no query language, and
// their subject is the shards' health.
func aboutHealth(r *http.Request) *http.Request {
	ctx := context.WithValue(r.Context(), noQueryCtx{}, true)
	return r.WithContext(context.WithValue(ctx, healthSubjectCtx{}, true))
}

// shardQuarantine is one shard's contribution to the merged listing.
type shardQuarantine struct {
	Shard  int                        `json:"shard"`
	Count  int                        `json:"count"`
	Groups []storage.QuarantineRecord `json:"groups"`
}

// federatedQuarantine merges every shard's quarantine listing.
//
// Records are grouped BY SHARD rather than concatenated into one flat list.
// A group id is only unique within a store, so two shards quarantining their
// own group 41 would appear as one id twice in a flat list, and the operator's
// next step -- go to that node and look at the file -- needs to know which node.
func (s *Server) federatedQuarantine(w http.ResponseWriter, r *http.Request) {
	answers, w, ok := s.fanOutChecked(w, aboutHealth(r),
		"/admin/storage/quarantine", nil)
	if !ok {
		return
	}
	out := make([]shardQuarantine, 0, len(answers))
	total := 0
	for _, a := range answers {
		var v struct {
			Count  int                        `json:"count"`
			Groups []storage.QuarantineRecord `json:"groups"`
		}
		if !s.mergeDecode(w, r, a, &v) {
			return
		}
		sq := shardQuarantine{Shard: a.shard, Groups: []storage.QuarantineRecord{}}
		if v.Groups != nil {
			sq.Groups = v.Groups
		}
		sq.Count = len(sq.Groups)
		total += sq.Count
		out = append(out, sq)
	}
	// Complete, STATED -- but only when nothing has already said otherwise.
	//
	// An admin route carries no cluster envelope of its own, so without this
	// a successful listing arrives with no completeness header at all and
	// "nothing is quarantined" is once again indistinguishable from "some
	// shards were not asked". That distinction is the entire reason this
	// endpoint used to be refused.
	//
	// fanOutChecked returns ok=true on TWO paths, and only one of them is
	// complete: with `allow_partial_response=1` it returns a partialWriter
	// having already set Partial=true and the real total/answered/missing
	// counts, and those headers are still unflushed when this runs. Stamping
	// unconditionally overwrote them -- so a knowingly partial listing went out
	// as 206 saying Complete: true with answered == total, which is the
	// confusion this endpoint exists to prevent, reintroduced by the code that
	// prevents it.
	//
	// The partial path does NOT set Complete itself, and that is not an
	// oversight there: it lowers a header serveEnvelope has already stamped,
	// and a router only stamps one when it is answering as somebody's peer. An
	// admin route has no envelope, so the lowering finds nothing to lower.
	// Which half applies is read from the missing-shards header, the one
	// signal that is set on exactly the partial path.
	h := w.Header()
	if h.Get(HdrShardsMissing) == "" {
		h.Set(HdrShardsTotal, strconv.Itoa(len(answers)))
		h.Set(HdrShardsAnswered, strconv.Itoa(len(answers)))
		h.Set(HdrComplete, "true")
	} else {
		h.Set(HdrComplete, "false")
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Count  int               `json:"count"`
		Shards []shardQuarantine `json:"shards"`
	}{total, out})
}

// federatedAcknowledgeDegraded forwards the operator's accept to every shard.
//
// It does NOT go through fanOutChecked, and that is the point of this comment.
//
// fanOutChecked's refusal is a READ's refusal: "this answer would have been
// missing data with no way for you to tell, so it is refused". Acknowledging
// is a write. By the time a shard is found unreachable, every shard that WAS
// reachable has already acknowledged -- the fan-out is concurrent and there is
// nothing to roll back -- so a refusal saying the request was not carried out
// would be false, and an operator reading it would send it again believing
// nothing had happened. (Sending it again is harmless; believing it is not.)
//
// So it keeps the same headers and the same status a partial read gets, and
// writes its own body: what was accepted, on which shards, and which shards
// were not reached. Acknowledging is idempotent -- a store that is not
// degraded accepts nothing and reports zero -- so re-sending after the
// unreachable shard returns is the whole remedy.
func (s *Server) federatedAcknowledgeDegraded(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.Header().Set("Allow", "POST, PUT")
		http.Error(w, "acknowledging a degraded store is a POST", http.StatusMethodNotAllowed)
		return
	}
	// An empty body with an explicit content type, not nil: askShard sends a
	// GET when the body is nil, and this endpoint answers 405 to a GET -- so
	// every shard would have refused and the router would have reported the
	// whole cluster as failing to acknowledge.
	peers := s.fanOutPeers(r, "/admin/acknowledge-degraded", []byte{}, "text/plain")

	total := 0
	var failed []string
	lines := make([]string, 0, len(peers))
	for i, p := range peers {
		if !p.OK() {
			failed = append(failed, fmt.Sprintf("%d(%s)", i, p.Class))
			lines = append(lines, fmt.Sprintf(
				"shard %d: NOT acknowledged, and still degraded if it was: %s: %v",
				i, p.Class, p.Err))
			continue
		}
		n := parseAcknowledgedCount(string(p.Body))
		total += n
		lines = append(lines, fmt.Sprintf("shard %d: acknowledged %d tenant(s)", i, n))
	}

	outcome := obs.OutcomeOK
	if len(failed) > 0 {
		outcome = obs.OutcomeFailed
	}
	// Audited on the ROUTER as well as on each shard. A shard's own audit says
	// a tenant was acknowledged; only this one says who asked the cluster to,
	// and only this one records that part of the cluster was not reached.
	obs.Audit(r.Context(), obs.EventCorruptionAck, subjectOf(r), outcome,
		"tenants_acknowledged", total, "shards", len(peers),
		"shards_failed", strings.Join(failed, ","))

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set(HdrShardsTotal, strconv.Itoa(len(peers)))
	w.Header().Set(HdrShardsAnswered, strconv.Itoa(len(peers)-len(failed)))
	w.Header().Set(HdrComplete, strconv.FormatBool(len(failed) == 0))
	if len(failed) > 0 {
		w.Header().Set(HdrShardsMissing, strings.Join(failed, ","))
		obs.L().Error("cluster acknowledgement did not reach every shard",
			obs.FieldEvent, "cluster.acknowledge_incomplete",
			obs.FieldRoute, r.URL.Path, "missing", strings.Join(failed, ","),
			obs.FieldErrorClass, string(obs.ClassUpstream))
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "PARTIALLY acknowledged: %d degraded tenant(s) on %d of %d "+
			"shard(s). %s could not be reached and are UNCHANGED -- what follows "+
			"has already been applied to the rest. Send this again once they "+
			"answer; acknowledging is idempotent\n",
			total, len(peers)-len(failed), len(peers), strings.Join(failed, ","))
	} else {
		fmt.Fprintf(w, "acknowledged %d degraded tenant(s) across %d shard(s)\n",
			total, len(peers))
	}
	for _, l := range lines {
		fmt.Fprintln(w, l)
	}
}

// parseAcknowledgedCount reads the leading count out of a storage node's
// acknowledge-degraded body, whose first line is
// "acknowledged N degraded tenant(s)".
//
// A body this cannot read counts 0, and that is a REPORTING gap rather than a
// correctness one -- but it is a gap, and the earlier version of this comment
// waved it away with a mechanism that does not exist ("decided by
// fanOutChecked from the shards' status", which this path does not call).
//
// What actually decides whether the cluster was fully acknowledged is
// PeerResponse.OK(), just below: a shard that did not answer is named and the
// request is 503. A shard that answers 200 with a body this cannot parse is
// counted OK and contributes 0, so the summary can under-report while
// reporting success. The shard did acknowledge -- its own audit says so -- and
// only the number is lost. Worth closing, and not by pretending a check
// happens somewhere else.
func parseAcknowledgedCount(body string) int {
	const prefix = "acknowledged "
	i := strings.Index(body, prefix)
	if i < 0 {
		return 0
	}
	rest := body[i+len(prefix):]
	j := 0
	for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
		j++
	}
	n, err := strconv.Atoi(rest[:j])
	if err != nil {
		return 0
	}
	return n
}
