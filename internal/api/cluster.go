package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sebishogun/simdlogs/internal/storage"
	"io"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/sebishogun/simdlogs/internal/config"
	obs "github.com/sebishogun/simdlogs/internal/observability"
	"github.com/sebishogun/simdlogs/internal/query"
	"sync/atomic"
	"time"
)

// SetBackends puts the node in select-router mode: /select/logsql/query fans
// out to these peer base URLs and merges their rows, the way VictoriaLogs'
// vmselect queries a set of vmstorage nodes. With no backends the node serves
// its own store.
func (s *Server) SetBackends(urls []string) { s.backends = urls }

// SetReplicas sets the replication factor: the backends partition into shards
// of r replicas each. A write goes to every replica of its shard; a read takes
// one live replica per shard, so data survives a replica loss without being
// double-counted. r<=1 is plain sharding (no replication).
func (s *Server) SetReplicas(r int) { s.replicas = r }

// SetMaxRows caps how many rows a bare (no-pipe) select may return; 0 = unlimited.
// Over the cap the query errors rather than truncating silently.
func (s *Server) SetMaxRows(n int) { s.maxRows = n }

// shards groups the backends into replica sets of size max(1, replicas).
func (s *Server) shards() [][]string {
	r := s.replicas
	if r < 1 {
		r = 1
	}
	var out [][]string
	for i := 0; i < len(s.backends); i += r {
		hi := i + r
		if hi > len(s.backends) {
			hi = len(s.backends)
		}
		out = append(out, s.backends[i:hi])
	}
	return out
}

// askShard asks one shard, trying its replicas in order, and returns the
// PeerResponse -- including when every replica failed.
//
// The failure is RETURNED rather than folded into a bool. Every replica error
// used to be a bare `continue` and the whole shard a `return nil, false`, so a
// caller could not tell "this shard has no data" from "every replica is down"
// from "the credential was refused" -- and the merge treated all three as an
// empty contribution.
//
// Not every class is worth another replica: an unauthorized answer is the
// ROUTER's credential being refused, so every replica refuses it identically
// and retrying turns one 401 into N while delaying the report by the timeout.
func (s *Server) askShard(
	r *http.Request, shardID int, shard []string, path string, post []byte, ct string,
) PeerResponse {
	method := http.MethodGet
	if post != nil {
		method = http.MethodPost
	}
	var last PeerResponse
	for i, b := range shard {
		last = s.peers.do(r, shardID, i, b, method, path, post, ct)
		if last.OK() {
			return last
		}
		obs.L().Warn("peer did not answer",
			obs.FieldEvent, "cluster.peer_failed",
			obs.FieldShard, shardID, "replica", i, "peer", b,
			obs.FieldErrorClass, string(last.Class), "error", last.Err)
		if !last.Class.retryAnotherReplica() {
			return last
		}
	}
	return last
}

// isWritePath reports whether a path ingests data (and so, in router mode,
// should forward to a storage node rather than be served locally).
func isWritePath(p string) bool {
	// A liveness probe is not a write. It lives under /insert only because
	// that is where the reference put it, and forwarding it made a router
	// answer 401 to an unauthenticated Kubernetes probe -- forever -- while
	// the same probe on a non-router node answered 200.
	if p == "/insert/ready" {
		return false
	}
	switch {
	case strings.HasPrefix(p, "/insert"), p == "/_bulk", p == "/v1/logs", p == "/v1/input",
		strings.HasPrefix(p, "/api/"), strings.HasPrefix(p, "/loki"):
		return true
	}
	return false
}

// routeWrites forwards ingest to a storage node when the node is in router
// mode; everything else falls through. Reads are handled downstream
// (federatedSelect), so only writes are intercepted here.
func (s *Server) routeWrites(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.backends) > 0 && isWritePath(r.URL.Path) {
			// Forwarding returns before the mux, so none of the per-route
			// wrappers run: the role check, method, media type and body limit
			// all live in there. Unguarded, a router was an unauthenticated,
			// unbounded ingest proxy -- an anonymous multi-megabyte POST was
			// relayed to a backend whose own limits it had already bypassed.
			// The same checks are applied here before anything is forwarded.
			spec := specForPath(r.URL.Path)
			// Authentication FIRST, exactly as Handler() orders it. With the
			// shape checks outside, a wrong Content-Type answered 415 and a
			// wrong method 405 before the 401 -- telling an anonymous caller
			// which media types and methods a route accepts. Single-node
			// answered 401 for both.
			if st := s.auth; st != nil && st.enabled {
				p := principalOf(r)
				if p == nil {
					w.Header().Set("WWW-Authenticate", `Bearer realm="simdlogs"`)
					s.writeErr(w, r, spec, http.StatusUnauthorized, "authentication required")
					return
				}
				if !p.Can(config.RoleIngest) {
					s.writeErr(w, r, spec, http.StatusForbidden,
						"principal "+p.Subject+" does not hold the ingest role")
					return
				}
			}
			if !allowedMethod(spec.methods, r.Method) {
				w.Header().Set("Allow", strings.Join(spec.methods, ", "))
				s.writeErr(w, r, spec, http.StatusMethodNotAllowed,
					"method "+r.Method+" not allowed")
				return
			}
			// Gated the same two ways guard() gates it. Neither guard was
			// here, so a spec with deliberately nil types -- datadogValidateSpec,
			// the very route specForPath exists to select -- rejected every
			// Content-Type with 415, and a GET body-less request was typed at
			// all.
			if len(spec.types) > 0 && r.Method != http.MethodGet && r.Method != http.MethodHead {
				if ct := r.Header.Get("Content-Type"); ct != "" {
					if mt, _, err := mime.ParseMediaType(ct); err != nil || !allowedType(spec.types, mt) {
						s.writeErr(w, r, spec, http.StatusUnsupportedMediaType,
							"unsupported media type "+ct)
						return
					}
				}
			}
			if lim := s.limits.MaxBodyBytes; lim != config.Unlimited && r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, lim)
			}
			// Resolve the tenant here too, and forward the RESOLVED key
			// rather than whatever the client sent. forwardWrite clones the
			// client's headers verbatim, and a storage node normally runs
			// with no -auth.config at all, so the router is the only place
			// AccountID is ever checked: without this a token scoped to one
			// tenant wrote into any other by setting a header.
			key, err := s.tenantFor(r)
			if err != nil {
				s.writeErr(w, r, spec, authStatus(err), err.Error())
				return
			}
			r.Header.Set("AccountID", key.Account)
			r.Header.Set("ProjectID", key.Project)
			// Admission and the request counter both live in guard() and
			// withTenant(), which forwarding returns before. Without the
			// first, N concurrent posts each ReadAll a whole body with no
			// bound; without the second, /metrics reads zero ingest on a
			// router under full load.
			if release, ok := s.admit(s.writeSem, spec, w, r); ok {
				defer release()
			} else {
				return
			}
			s.countRequest(r.URL.Path)
			s.forwardWrite(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// forwardWrite sends the ingest body to one storage node, round-robin, and
// relays its response -- so a burst of inserts spreads across the cluster.
// It round-robins over shards and writes the record to every replica in the
// chosen shard, so a replica loss never loses data.
// ConsistencyLevel is how many replicas must acknowledge a write before the
// client is told it succeeded.
//
// The old behaviour was none of these: forwardWrite replicated to every member
// and relayed the LAST response's status. Replica A refusing on its own quota
// and replica B accepting answered whichever finished last, so the same write
// was reported as stored or refused depending on scheduling -- and a 507 in
// the other order made a retry duplicate into the replica that had already
// taken it.
type ConsistencyLevel string

const (
	// ConsistencyOne succeeds when any replica commits. The write may exist on
	// one machine only, so losing that machine loses the data.
	ConsistencyOne ConsistencyLevel = "one"
	// ConsistencyQuorum succeeds when more than half commit.
	ConsistencyQuorum ConsistencyLevel = "quorum"
	// ConsistencyAll succeeds only when every replica commits.
	//
	// The DEFAULT, and it stays the default until repair is proven. Quorum is
	// the usual production choice because a repair process reconciles the
	// replicas that missed a write -- and this has no repair process (task
	// 8.7). Without one, "quorum" means a replica silently missing data
	// forever, and a read that happens to land on it returns a short answer
	// with nothing to say so. Defaulting to the strictest level is the honest
	// position for a system that cannot yet heal.
	ConsistencyAll ConsistencyLevel = "all"
)

// required is how many acknowledgements this level needs from n replicas.
func (c ConsistencyLevel) required(n int) int {
	switch c {
	case ConsistencyOne:
		return 1
	case ConsistencyQuorum:
		return n/2 + 1
	default:
		return n
	}
}

// ParseConsistency validates a level.
func ParseConsistency(s string) (ConsistencyLevel, error) {
	switch ConsistencyLevel(s) {
	case "":
		return ConsistencyAll, nil
	case ConsistencyOne:
		return ConsistencyOne, nil
	case ConsistencyQuorum:
		return ConsistencyQuorum, nil
	case ConsistencyAll:
		return ConsistencyAll, nil
	}
	return "", fmt.Errorf("simdlogs: consistency must be one, quorum or all, not %q", s)
}

// replicaOutcome is one replica's answer to a write.
type replicaOutcome struct {
	URL       string `json:"replica"`
	Status    int    `json:"status,omitempty"`
	Duplicate bool   `json:"duplicate,omitempty"`
	Class     string `json:"class,omitempty"`
	Error     string `json:"error,omitempty"`
}

func (s *Server) forwardWrite(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		// MaxBytesReader is installed by routeWrites, so an oversized body
		// lands here as a MaxBytesError and must be 413 rather than 400.
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			s.writeErr(w, r, ndjsonSpec(), http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", mbe.Limit))
			return
		}
		s.writeErr(w, r, ndjsonSpec(), http.StatusBadRequest, err.Error())
		return
	}

	level, err := ParseConsistency(r.Header.Get(HdrConsistency))
	if err != nil {
		s.writeErr(w, r, ndjsonSpec(), http.StatusBadRequest, err.Error())
		return
	}

	// The write id: the client's if it sent one -- that is how a retry names
	// the write it is repeating -- otherwise a fresh one. Validated because a
	// client-supplied id ends up in the manifest.
	wid := r.Header.Get(HdrWriteID)
	if wid != "" && !storage.ValidWriteID(wid) {
		s.writeErr(w, r, ndjsonSpec(), http.StatusBadRequest,
			"simdlogs: "+HdrWriteID+" must be 8-64 hex characters")
		return
	}
	if wid == "" {
		id, err := storage.NewWriteID()
		if err != nil {
			s.writeErr(w, r, ndjsonSpec(), http.StatusInternalServerError, err.Error())
			return
		}
		wid = string(id)
	}

	shards := s.shards()
	shard := shards[int(atomic.AddInt64(&s.rr, 1)-1)%len(shards)]

	// Every replica, in parallel, each carrying the SAME write id. A replica
	// that already has it answers "duplicate", which counts as an
	// acknowledgement: the data is there, which is the only thing the level is
	// asking about.
	outcomes := make([]replicaOutcome, len(shard))
	var wg sync.WaitGroup
	for i, b := range shard {
		wg.Add(1)
		go func(i int, b string) {
			defer wg.Done()
			outcomes[i] = s.replicateTo(r, b, body, wid)
		}(i, b)
	}
	wg.Wait()

	acked := 0
	for _, o := range outcomes {
		if o.Error == "" {
			acked++
		}
	}
	need := level.required(len(shard))
	w.Header().Set(HdrWriteID, wid)
	w.Header().Set(HdrConsistency, string(level))
	w.Header().Set("Content-Type", "application/json")

	if acked < need {
		// Refused, and the client is told the write id. That is what makes the
		// retry safe: the replicas that DID commit recognise the id and answer
		// duplicate rather than storing the rows twice.
		obs.L().Error("replicated write did not reach its consistency level",
			obs.FieldEvent, "cluster.write_underreplicated",
			"write_id", wid, "acked", acked, "required", need,
			obs.FieldErrorClass, string(obs.ClassUpstream))
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{
			"error": fmt.Sprintf(
				"%d of %d replicas acknowledged, %s requires %d; retry with %s: %s "+
					"-- replicas that already committed will not duplicate",
				acked, len(shard), level, need, HdrWriteID, wid),
			"write_id": wid,
			"replicas": s.visibleOutcomes(r, outcomes),
			"acked":    acked,
			"required": need,
		})
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"write_id": wid,
		"acked":    acked,
		"replicas": s.visibleOutcomes(r, outcomes),
	})
}

// visibleOutcomes redacts per-replica detail from unauthorized callers.
//
// Replica URLs and their individual failures are the cluster's internal
// topology. An operator needs them to act; an ordinary ingest client needs to
// know only that the write did or did not reach its level, and telling it
// which machines exist and which are down is a map of the deployment.
func (s *Server) visibleOutcomes(r *http.Request, outcomes []replicaOutcome) []replicaOutcome {
	if s.healthDetailAllowed(r) {
		return outcomes
	}
	out := make([]replicaOutcome, len(outcomes))
	for i, o := range outcomes {
		out[i] = replicaOutcome{Status: o.Status, Duplicate: o.Duplicate, Class: o.Class}
	}
	return out
}

// replicateTo sends the write to one replica and classifies the answer.
func (s *Server) replicateTo(r *http.Request, url string, body []byte, wid string) replicaOutcome {
	out := replicaOutcome{URL: url}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, url+r.URL.Path,
		bytes.NewReader(body))
	if err != nil {
		out.Class, out.Error = string(PeerMalformed), err.Error()
		return out
	}
	// Explicit headers, not r.Header.Clone().
	//
	// Cloning forwarded the client's Authorization and cookies to every
	// storage node -- the router authenticates to peers as itself, and a
	// client credential travelling past the node it was presented to is how
	// one node's compromise becomes the cluster's.
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	if ce := r.Header.Get("Content-Encoding"); ce != "" {
		req.Header.Set("Content-Encoding", ce)
	}
	for _, h := range forwardedHeaders {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	req.Header.Set(HdrInternal, "1")
	req.Header.Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
	req.Header.Set(HdrWriteID, wid)

	resp, err := s.peers.http.Do(req)
	if err != nil {
		out.Class, out.Error = string(PeerUnavailable), err.Error()
		return out
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	out.Status = resp.StatusCode
	out.Duplicate = resp.Header.Get(HdrDuplicate) == "true"

	switch {
	case out.Duplicate:
		// Already committed: an acknowledgement, not a failure. The data is
		// there, which is the only thing the consistency level asks about.
		return out
	case resp.StatusCode/100 == 2:
		return out
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		out.Class = string(PeerUnauthorized)
	case resp.StatusCode == http.StatusInsufficientStorage:
		out.Class = string(PeerDegraded)
	case resp.StatusCode == http.StatusTooManyRequests:
		out.Class = string(PeerOverloaded)
	default:
		out.Class = string(PeerUnavailable)
	}
	out.Error = fmt.Sprintf("replica answered HTTP %d", resp.StatusCode)
	return out
}

// federatedSelect queries every backend concurrently, merges the NDJSON rows
// in time order (newest first), and applies the limit across the merged set.
// Row merge is exact -- rows are independent, so concatenate-sort-limit is the
// correct distributed answer. Tenant headers propagate so each backend answers
// for the same tenant.
func (s *Server) federatedSelect(w http.ResponseWriter, r *http.Request) {
	// The PLAN before the fan-out.
	//
	// The whole query used to go to every shard and the final rows were
	// concatenated: `| stats count()` answered once per shard, `| sort |
	// limit 10` returned each shard's top ten, `| uniq` returned each shard's
	// distinct values. Only the row-local prefix is distributable; everything
	// from the first non-row-local pipe runs here, once, over the merged rows.
	shardQuery, coordPipes, ok := s.planQuery(w, r)
	if !ok {
		return
	}
	shardReq := r.Clone(r.Context())
	shardReq.URL.RawQuery = shardQueryURL(r, shardQuery, coordPipes)
	// shardQueryURL rewrites `query` and, when there is a coordinator half,
	// DELETES `limit` -- the shards must return everything and the bound is
	// applied once over the merged rows. A deleted key is not in the shard URL,
	// so withFormInURL's "skip what the URL already has" rule would put the
	// caller's limit back over a POST. Measured before this: three shards of
	// ten rows and `&limit=5`, `* | stats count() c` answered 30 on a single
	// node, 30 over a cluster GET and 15 over a cluster POST form.
	shardReq = withPlanKeys(shardReq, "query", "limit")

	// The completeness gate BEFORE the merge. A shard that did not answer used
	// to contribute nothing and the merge proceeded, so a cluster read with one
	// shard down returned the other shards' rows with HTTP 200 and nothing to
	// say a third of the data was missing.
	bodies, w, ok := s.fanOutChecked(w, shardReq, "/select/logsql/query", nil)
	if !ok {
		return
	}
	s.mergeRows(w, r, bodies, coordPipes)
}

// federatedRows answers an NDJSON-row endpoint for the cluster: fan out
// unchanged, merge in time order, apply `limit` across the merged set.
//
// For endpoints whose whole pipeline is row-local, which is the condition under
// which concatenate-sort-limit is the correct distributed answer. A caller that
// cannot guarantee that must plan first, as federatedSelect does.
func (s *Server) federatedRows(w http.ResponseWriter, r *http.Request, path string) {
	bodies, w, ok := s.fanOutChecked(w, r, path, nil)
	if !ok {
		return
	}
	s.mergeRows(w, r, bodies, nil)
}

// mergeRows merges shard NDJSON and applies the coordinator half of a plan.
func (s *Server) mergeRows(
	w http.ResponseWriter, r *http.Request, answers []shardAnswer, coordPipes []query.Pipe,
) {
	// Rows are kept as slices into each shard's response body, not strings: the
	// merge touches every row of every shard, and a string per row was the
	// router's cost on a big result (a Scanner's Text() copies each line).
	type row struct {
		t    int64
		line []byte
		// shard and seq make the order TOTAL and reproducible.
		//
		// rowLineTime returns 0 for any line with no `"_time":"` -- which is
		// every row after a projecting pipe -- so a whole result could share
		// one key and the sort left it in the order the goroutines happened to
		// finish. `* | fields _msg | limit 5` over three shards returned shard
		// 2's first five, because shard 2's chunk landed first.
		//
		// Ties on t are broken by (shard, position within that shard), which is
		// the order a single node would have produced: shards hold disjoint
		// time ranges, and within a shard the scan order is preserved.
		shard int
		seq   int
	}
	var mu sync.Mutex
	var all []row
	// badLine is the first line no shard should have sent, reported after the
	// fan-in so the refusal names the shard and the bytes.
	var badShard = -1
	var badLine []byte

	var wg sync.WaitGroup
	for _, a := range answers {
		wg.Add(1)
		go func(shardOf int, body []byte) {
			defer wg.Done()
			local := make([]row, 0, bytes.Count(body, []byte{'\n'}))
			var bad []byte
			for start := 0; start < len(body); {
				e := bytes.IndexByte(body[start:], '\n')
				var line []byte
				if e < 0 {
					line, start = body[start:], len(body)
				} else {
					line, start = body[start:start+e], start+e+1
				}
				if len(line) == 0 {
					continue
				}
				// Every line a storage node emits is one JSON object. A line
				// that is not one is not a row, and mergeRows used to carry it
				// through: a proxy's HTML error page in a 200 body came back to
				// the caller AS A LOG LINE, and with a coordinator half it
				// became _msg="<html>...". mergeDecode covers the eight
				// envelope merges; this is the primary read path and had
				// nothing.
				if !looksLikeJSONObject(line) {
					if bad == nil {
						bad = line
					}
					continue
				}
				local = append(local, row{
					t: rowLineTime(bytesToString(line)), line: line, shard: shardOf,
					seq: len(local),
				})
			}
			mu.Lock()
			all = append(all, local...)
			if bad != nil && (badShard < 0 || shardOf < badShard) {
				badShard, badLine = shardOf, bad
			}
			mu.Unlock()
		}(a.shard, a.body)
	}
	wg.Wait()

	if badShard >= 0 {
		obs.L().Error("cluster read refused: a shard sent a line that is not a row",
			obs.FieldEvent, "cluster.read_not_a_row",
			obs.FieldRoute, r.URL.Path, "shard", badShard,
			obs.FieldErrorClass, string(obs.ClassUpstream))
		s.writeErr(w, r, readSpec(), http.StatusBadGateway, fmt.Sprintf(
			"simdlogs: shard %d answered 200 with a line that is not a JSON row "+
				"(%q). Returning it would put that text in front of you as a log "+
				"line, so the answer is refused",
			badShard, truncateLine(badLine, 120)))
		return
	}

	// The coordinator half of the plan, applied ONCE over the merged rows.
	if len(coordPipes) > 0 {
		// The order the pipes see must be the order a single storage node would
		// have given them, or a pipe that reads POSITION -- offset, limit
		// without a sort, tail -- answers a different question on a cluster than
		// on one node. A node scans oldest-first, so the coordinator half sorts
		// ascending; the bare-select path below keeps newest-first, because
		// there `limit` means "the newest N" and that is the endpoint's
		// documented meaning.
		sort.Slice(all, func(i, j int) bool {
			if all[i].t != all[j].t {
				return all[i].t < all[j].t
			}
			if all[i].shard != all[j].shard {
				return all[i].shard < all[j].shard
			}
			return all[i].seq < all[j].seq
		})

		// The rows are parsed into fields ONLY here.
		//
		// The merge keeps raw NDJSON lines because it only has to order and
		// re-emit them, and a string per row was the router's cost on a big
		// result. A pipe operates on fields, so this path has to pay the
		// decode -- and only this path does.
		qrows := make([]query.Row, 0, len(all))
		for _, rw := range all {
			qrows = append(qrows, jsonLineToRow(rw.line))
		}
		merged, err := s.applyCoordinatorPipes(r, qrows, coordPipes)
		if err != nil {
			s.writeErr(w, r, readSpec(), query.HTTPStatus(err), err.Error())
			return
		}

		// The endpoint's `limit`, applied once, here -- and only where a single
		// node would apply it. See limitBoundsOutput.
		//
		// `limit` means the NEWEST n, returned newest-first. Measured against a
		// single node: `*` unlimited answers line 00 first and `*&limit=3`
		// answers line 29 first, on the same data. Taking the first n of the
		// ascending merge gave the OLDEST n -- the opposite rows, not merely
		// the opposite order.
		if n := endpointLimit(r); n > 0 && limitBoundsOutput(coordPipes) {
			if len(merged) > n {
				merged = merged[len(merged)-n:]
			}
			for i, j := 0, len(merged)-1; i < j; i, j = i+1, j-1 {
				merged[i], merged[j] = merged[j], merged[i]
			}
		}
		w.Header().Set("Content-Type", ndjsonContentType)
		w.WriteHeader(http.StatusOK)
		bw := bufio.NewWriter(w)
		defer bw.Flush()
		var buf []byte
		for _, row := range merged {
			// withStream is FALSE here, unlike every direct read: these rows
			// came back from a storage node that already stamped the pair, and
			// they survived the round trip as ordinary fields. Asking for it
			// again appended a second _stream_id to every row -- valid JSON,
			// duplicate key, and the two decoders in this repo disagree about
			// which one wins.
			buf = appendRowJSON(buf[:0], row, false)
			bw.Write(buf)
		}
		return
	}

	// The bare-select order, matching a single node exactly.
	//
	// Newest-first ONLY when `limit` is set. A single node answers `*` with
	// line 00 first and `*&limit=3` with line 29 first on the same data --
	// `limit` is LastN, "the newest N", and an unlimited select is scan order,
	// which is oldest first. This path sorted newest-first unconditionally, so
	// every unlimited cluster select came back reversed relative to the server
	// it is a cluster of.
	//
	// The tiebreak is TOTAL. sort.Slice is not stable, so rows sharing a
	// timestamp came back in an order that varied between identical requests:
	// 40 requests over three shards of eight rows at one timestamp produced two
	// distinct four-row answers. With `limit` that is different ROWS, not
	// merely a different order.
	newestFirst := endpointLimit(r) > 0
	sort.Slice(all, func(i, j int) bool {
		if all[i].t != all[j].t {
			if newestFirst {
				return all[i].t > all[j].t
			}
			return all[i].t < all[j].t
		}
		if all[i].shard != all[j].shard {
			return all[i].shard < all[j].shard
		}
		return all[i].seq < all[j].seq
	})

	limit := 0
	if v := r.FormValue("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	w.Header().Set("Content-Type", ndjsonContentType)
	// Explicit, because an EMPTY result writes no bytes -- and a handler that
	// returns without writing sends 200 by default, which on a partial answer
	// is exactly the confident-and-wrong status this whole mechanism exists to
	// prevent. The partial writer upgrades this to 206.
	w.WriteHeader(http.StatusOK)
	bw := bufio.NewWriter(w)
	defer bw.Flush()
	for i, rw := range all {
		if limit > 0 && i >= limit {
			break
		}
		bw.Write(rw.line)
		bw.WriteByte('\n')
	}
}

// fanOutPeers asks every shard and returns one PeerResponse each, in shard
// order -- including the failures.
//
// The failures are what the old [][]byte could not carry: a nil entry meant
// "no data", "every replica down" and "refused" alike, so a merge could not
// report an incomplete answer because it could not see one.
func (s *Server) fanOutPeers(r *http.Request, path string, body []byte, ct string) []PeerResponse {
	shards := s.shards()
	out := make([]PeerResponse, len(shards))
	var wg sync.WaitGroup
	for i, sh := range shards {
		wg.Add(1)
		go func(i int, sh []string) {
			defer wg.Done()
			out[i] = s.askShard(r, i, sh, path, body, ct)
			s.checkWatermark(&out[i], i)
		}(i, sh)
	}
	wg.Wait()
	return out
}

// withFormInURL folds a POST form body into the request's query string so the
// fan-out carries it.
//
// The peer client sends r.URL.RawQuery with every request and the read fan-out
// sends no body -- askShard's `post` is nil for every read path. So a client
// that POSTs `query=...` as a form, which the reference accepts and which is how
// anything longer than a URL is sent, had its parameters dropped on the way to
// the shards. Every federated endpoint except /select/logsql/query was affected:
// that one survives only because planQuery rebuilds the shard URL from the
// parsed form on its own, which is this fix written once for one endpoint.
//
// Worse than losing them: the shards then answered the EMPTY query, and the
// router reported that as the shards having rejected the request -- pointing an
// operator at the storage nodes for a fault in the router.
//
// Folded into the URL rather than forwarded as a body, because the shard call is
// a GET and every parameter these endpoints take is a query parameter anyway.
// r.Form is the union of the URL query and the parsed body, so re-encoding it
// preserves both. Only for a form content type: ParseForm does not touch a JSON
// body, and the two ES endpoints that DO send a body build their own.
// maxPeerQueryBytes is how much of a peer request may travel in the request
// LINE before the rest is moved into a form body.
//
// A request line and its headers together are bounded by net/http's
// MaxHeaderBytes, 1 MiB by default, and a peer that exceeds it answers 431 from
// the server itself -- before the handler, so with no protocol-version header
// and no error class. Measured: a 1.2 MB `level:in(...)` query, the shape a
// dashboard templating variable expands to, POSTed to a ONE-NODE cluster
// running one build, came back
//
//	503 ... 1 of 1 shards could not answer completely (0(version_mismatch))
//
// on a cluster with no version mismatch in it. The same query on a non-router
// node is answered. POST exists so a query can be larger than a URL, and
// folding the form into the URL took that away in clustered mode only.
//
// 60 KiB of ENCODED parameters, which is not 60 KB of query text.
//
// The comparison is against url.Values.Encode(), and percent-encoding a LogsQL
// query roughly doubles it -- every `{`, `}`, `"`, `:`, `(`, `)` and space
// becomes three bytes. Measured switch point on an `in(...)` list: 30,001 raw
// characters still travelled in the URL, 31,001 (62,001 encoded) took the body.
// So the raw budget is about 30 KB, and an earlier version of this comment said
// "no dashboard emits 60 KB of parameters" while the real threshold was half
// what it named.
//
// The number is still chosen to leave the ordinary GET path untouched -- 30 KB
// of query text is far past anything a dashboard emits -- while the oversized
// case, which cannot work in a URL at all, takes the body. Stated in encoded
// bytes because that is what the request line actually carries.
const maxPeerQueryBytes = 60 << 10

// withFormInURL folds a POST form into the peer request. It returns the request
// and, when the parameters are too large for a request line, the form body the
// peer request must carry instead.
func withFormInURL(r *http.Request) (*http.Request, []byte) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		return r, nil
	}
	ct := r.Header.Get("Content-Type")
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	if strings.TrimSpace(ct) != "application/x-www-form-urlencoded" {
		return r, nil
	}
	if err := r.ParseForm(); err != nil {
		// A form the router cannot parse is not a request it can fan out. This
		// used to `return r`, which sent the shards the EMPTY query at HTTP 200
		// -- the exact behaviour this function's own doc comment names as the
		// reason it exists, left in place for the error path.
		return nil, nil
	}
	if len(r.Form) == 0 {
		return r, nil
	}
	// MERGED UNDER the query string, never over it.
	//
	// http.Request.Clone copies Form and PostForm. Every federated handler that
	// rewrites the shard URL -- federatedSelect's plan, withoutLimits' facets
	// bounds, federatedValueCounts -- clones the request first, so ParseForm
	// here returns immediately with the CALLER's parsed form still attached, and
	// replacing RawQuery with it discarded the rewrite.
	//
	// That made a POST form answer a different question than the same GET:
	// `stats count()` over two shards of five rows answered 10 over GET and 2
	// over POST, because each shard ran the whole pipeline and the coordinator
	// re-ran it over the two results. It also handed the shards the caller's
	// `limit=3` in place of the `limit=0` the facets path sets, so the cluster
	// merged shard-local top-3s -- the defect 883508c exists to prevent.
	//
	// Worst of it: /select/logsql/query over a POST form was CORRECT before the
	// commit that added this, and the commit message said so. A key already in
	// the query string wins; the form only supplies what the URL does not have.
	q := r.URL.Query()
	extra := make(url.Values, len(r.Form))
	for k, vs := range r.Form {
		// Already in the shard URL, or OWNED by the plan -- which covers the
		// key the plan deliberately deleted, the case "already in the URL"
		// cannot see. See planKeysCtx.
		if _, ok := q[k]; ok || planOwns(r, k) {
			continue
		}
		extra[k] = vs
	}
	out := r.Clone(r.Context())

	// The MERGED set, resolved here, so exactly one of the two carries it.
	//
	// The first version of this moved only the form's contribution to the body
	// and left the query string alone. That covered eleven of the twelve
	// fan-out endpoints and missed the one the change was written for:
	// federatedSelect writes the PLANNED query into the shard URL
	// (shardQueryURL -> vals.Set("query", ...)) before this runs, so `query` is
	// already a URL key, is skipped from `extra`, and cannot move. With nothing
	// else in the form there was no body at all and the request line was
	// unchanged. Measured on one router and one shard: a 520 KB query answered
	// 200, a 560 KB one answered 503 `0(unavailable)` with the shard never
	// reached, and the same query on a NON-router node answered 200.
	//
	// Resolving first and sending the whole set one way removes the precedence
	// question rather than relying on disjointness: r.Form copies PostForm
	// BEFORE the URL query, so on the peer a body value beats a query one, and
	// a set split across both depends on which half each key landed in. With
	// RawQuery cleared there is one source and the plan wins because the plan
	// is what was merged in.
	//
	// Safe because every fan-out endpoint's shard handler reads r.FormValue,
	// which is the union of both. The one deliberate r.URL.Query() reader,
	// ingestOptions, is on the ingest paths and never reaches here.
	merged := make(url.Values, len(q)+len(extra))
	for k, vs := range q {
		merged[k] = vs
	}
	for k, vs := range extra {
		merged[k] = vs
	}
	enc := merged.Encode()
	if len(enc) > maxPeerQueryBytes {
		out.URL.RawQuery = ""
		return out, []byte(enc)
	}
	if len(extra) == 0 {
		return r, nil
	}
	out.URL.RawQuery = enc
	return out, nil
}

// checkWatermark demotes an answer served by a replica that has fallen behind.
//
// askShard returns the FIRST replica that answers, and Complete is that peer's
// report on its OWN store -- true, and useless here, because a lagging replica's
// store is complete as far as it knows. Two replicas of one shard holding 12 and
// 8 rows answered the same query with 12 or 8 depending on which was up, both at
// HTTP 200, with nothing in the response saying which had happened.
//
// PeerResponse.HighWatermark is the field that tells them apart, and its own
// documentation says so -- "what lets a caller tell no results from no results
// yet". It was populated on the wire, parsed by the client, and read by no read
// path: built, documented, and wired to nothing, which is the shape round six
// found four times over.
//
// This is the reader. The router remembers the highest watermark it has seen
// from each shard and LOGS when an answer comes in below it, naming the shard,
// the peer and both numbers.
//
// # What it does not do
//
// It does not refuse. The first version marked such an answer incomplete, which
// made it a 503 -- and a watermark going backwards turned out not to be reliable
// evidence of a lagging replica: tenant eviction, a topology change and
// retention each lower it on a perfectly healthy cluster. See the comment at the
// comparison below.
//
// It is also blind where it has no history: a router freshly started, or one
// whose only answer for a shard ever came from the lagging replica, has nothing
// to compare against. Catching that needs a quorum read, a request per replica
// on every query.
//
// One atomic per read and no extra replica asked.
func (s *Server) checkWatermark(p *PeerResponse, shard int) {
	// A zero watermark is "the peer did not say", not "the epoch". A peer that
	// omits the header is on an older protocol version, and treating silence as
	// maximally-lagging would fail every read against it.
	if !p.OK() || p.HighWatermark == 0 {
		return
	}
	seen := s.shardHW(shard)
	for {
		hi := seen.Load()
		if p.HighWatermark < hi {
			// REPORTED, not refused.
			//
			// The first version marked the answer incomplete, which routed it
			// into the completeness rule and produced a 503. That is wrong,
			// because a watermark going backwards is not reliably evidence of a
			// lagging replica -- three ways to produce it on a HEALTHY cluster
			// were found, and each one is a hard outage:
			//
			//   - highWatermark() is the max over OPEN tenants, so evicting a
			//     tenant lowers a node's reported watermark. A one-node,
			//     one-replica cluster reading tenant 2 then tenant 1 refuses
			//     itself, with nothing lagging and no replica to blame.
			//   - the history is keyed by shard INDEX, and SetBackends may
			//     repoint an index at a different machine, which inherits the
			//     previous machine's floor.
			//   - retention deleting the newest data lowers it legitimately.
			//
			// A false 503 is not the failure this was built to catch; it is a
			// worse one, and it is unrecoverable within the process. The signal
			// still reaches an operator -- the log line names the shard, the
			// peer and both watermarks, and PartialReads is not touched -- but
			// it no longer decides the answer. Turning it back into a refusal
			// needs a watermark that does not move for reasons unrelated to
			// replication, which is task #434.
			obs.L().Warn("shard answered from a replica behind the highest watermark seen",
				obs.FieldEvent, "cluster.replica_lagging",
				obs.FieldShard, shard, "replica", p.Replica, "peer", p.URL,
				"watermark", p.HighWatermark, "highest_seen", hi,
				obs.FieldErrorClass, string(obs.ClassUpstream))
			return
		}
		if p.HighWatermark == hi || seen.CompareAndSwap(hi, p.HighWatermark) {
			return
		}
		// Lost the race to another shard's goroutine; re-read and decide again.
	}
}

// shardHW is the per-shard highest watermark this router has observed. Created
// on first use: the shard count is a runtime property of the backend list, and
// sizing a slice from it would need re-sizing every time that list changes.
func (s *Server) shardHW(shard int) *atomic.Int64 {
	s.hwMu.Lock()
	defer s.hwMu.Unlock()
	if s.hw == nil {
		s.hw = map[int]*atomic.Int64{}
	}
	v, ok := s.hw[shard]
	if !ok {
		v = &atomic.Int64{}
		s.hw[shard] = v
	}
	return v
}

// mergeDecode unmarshals one shard's answer into v, refusing rather than
// skipping when it will not parse.
//
// A shard that answered 200 with a body this coordinator cannot read has NOT
// contributed its rows. Skipping it produces a short answer that looks
// complete -- which is exactly what the completeness rule in fanOutChecked
// exists to prevent, one layer down and invisible to it, because as far as the
// fan-out is concerned that shard answered fine.
//
// EIGHT handlers decode a shard body: _count, _search, valueCounts, matrix,
// statsQuery, hits, facets and vector. Four of them reached this point with a
// bare `continue` (matrix, hits, facets, vector) and four with an
// `if json.Unmarshal(...) == nil` guard around the whole body (the rest); both
// shapes drop the shard. An earlier version of this comment said "six", and
// the commit that wrote it said "eight merges continued", and neither was the
// count of either thing.
func (s *Server) mergeDecode(w http.ResponseWriter, r *http.Request, a shardAnswer, v any) bool {
	shard := a.shard
	if err := json.Unmarshal(a.body, v); err != nil {
		obs.L().Error("cluster read refused: unreadable shard answer",
			obs.FieldEvent, "cluster.read_unreadable",
			obs.FieldRoute, r.URL.Path,
			"shard", shard,
			"error", err.Error(),
			obs.FieldErrorClass, string(obs.ClassUpstream))
		s.writeErr(w, r, readSpec(), http.StatusBadGateway, fmt.Sprintf(
			"simdlogs: shard %d answered 200 with a body this coordinator could not "+
				"parse (%v). Its rows are missing from this answer with no way for you "+
				"to tell, so the answer is refused rather than returned short.",
			shard, err))
		return false
	}
	return true
}

// shardAnswer is one shard's response body with the shard it came from.
//
// This used to be a [][]byte indexed by shard with a nil for any shard that did
// not answer, and every merge told the two apart by testing the body for nil.
// That is the same value json.Unmarshal fails on, so "this shard is absent and
// the caller opted into a partial answer" and "this shard sent something
// unreadable" arrived at the merge indistinguishable. Absent shards are now
// absent from the slice, and anything in it is an answer that has to parse.
type shardAnswer struct {
	shard int
	body  []byte
}

func answersOf(rs []PeerResponse) []shardAnswer {
	out := make([]shardAnswer, 0, len(rs))
	for i, p := range rs {
		if p.OK() {
			out = append(out, shardAnswer{shard: i, body: p.Body})
		}
	}
	return out
}

// federatedESCount sums the ES _count across storage nodes.
func (s *Server) federatedESCount(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	total := 0
	bodies, w, ok := s.fanOutChecked(w, r, "/_count", body)
	if !ok {
		return
	}
	for _, a := range bodies {
		var v struct {
			Count int `json:"count"`
		}
		if !s.mergeDecode(w, r, a, &v) {
			return
		}
		total += v.Count
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"count": total})
}

// federatedESSearch merges ES _search hits across storage nodes.
func (s *Server) federatedESSearch(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	// The SHARDS are asked for from=0 and size=from+size.
	//
	// The client's body went to every shard verbatim, so each one skipped
	// `from` of its OWN hits before answering -- and then the coordinator
	// skipped `from` again over the concatenation. Measured, 2 shards x 6 docs,
	// {"from":4,"size":4}:
	//
	//   single node   4 hits (a-04 a-05 b-00 b-01)
	//   cluster       0 hits, "total":12, HTTP 200
	//
	// Each shard returned its last 2, the coordinator dropped 4 more, and rows
	// 0-3 of every shard were unreachable from any page at all. A shard must
	// return everything the coordinator might need to page over, which is the
	// first from+size of that shard, and the paging happens once, here.
	// Decoded with the SAME rules a single node applies, and the errors kept.
	//
	// This discarded both: `_ = dec0.Decode(&want)`. So the federated path
	// accepted a body the single node rejects with 400 -- `{"from":-1,"size":3}`
	// came back 200 with the WRONG DOCUMENTS, because need = from+size = 2 made
	// each shard return two hits and rows 2-5 were never fetched, while
	// "total":12 said they existed. An unknown field was swallowed the same
	// way, and a reframe failure fell back to shipping the caller's body
	// verbatim, which is the double-paging bug the comment above documents.
	var want esQuery
	dec0 := json.NewDecoder(bytes.NewReader(body))
	dec0.DisallowUnknownFields()
	if err := dec0.Decode(&want); err != nil {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest, "simdlogs: "+err.Error())
		return
	}
	if want.Size < 0 || want.From < 0 {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest,
			"simdlogs: size and from must not be negative")
		return
	}
	shardBody, err := reframeESPaging(body, want)
	if err != nil {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest, fmt.Sprintf(
			"simdlogs: this search body could not be reframed for the shards (%v). "+
				"Sending it unchanged would apply from/size twice -- once on each "+
				"shard and again here -- so it is refused", err))
		return
	}

	var hits []json.RawMessage
	total := 0
	bodies, w, ok := s.fanOutChecked(w, r, "/_search", shardBody)
	if !ok {
		return
	}
	for _, a := range bodies {
		var v struct {
			Hits struct {
				Total struct {
					Value int `json:"value"`
				} `json:"total"`
				Hits []json.RawMessage `json:"hits"`
			} `json:"hits"`
		}
		if !s.mergeDecode(w, r, a, &v) {
			return
		}
		total += v.Hits.Total.Value
		hits = append(hits, v.Hits.Hits...)
	}
	// from/size applied to the MERGED hits.
	//
	// The shards' hits were concatenated and returned whole, so `size: 10`
	// across three shards returned thirty documents -- and an ES client that
	// renders `size` results shows three pages' worth on one page, while one
	// that paginates by `from` skips two thirds of the corpus on every step.
	// The total was already cluster-wide; only the page was not.
	// Ordered before paging. The concatenation is in shard-response order, so
	// without this a page is "whichever shard answered first", and two
	// identical requests can return different documents.
	sortESHits(hits)

	body2 := want
	if body2.From > 0 {
		if body2.From >= len(hits) {
			hits = nil
		} else {
			hits = hits[body2.From:]
		}
	}
	size := body2.Size
	if size <= 0 {
		size = esDefaultSize
	}
	if len(hits) > size {
		hits = hits[:size]
	}
	if hits == nil {
		hits = []json.RawMessage{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"hits": map[string]any{
			// The cluster-wide count of MATCHING documents, which is what
			// hits.total means -- not the number returned. Every ES client
			// renders it as "N results".
			"total": map[string]any{"value": total, "relation": "eq"},
			"hits":  hits,
		},
	})
}

// federatedValueCounts fans a GET out to the storage nodes and merges the
// value/hits list found under key, summing hits per value -- the cluster form
// of streams/stream_ids/stream_field_values.
// The key is ALWAYS "values", whatever the path.
//
// Every one of these endpoints answers through writeValues on a storage node,
// which emits {"values": [...]}. The router asked for "streams" on
// /select/logsql/streams and "stream_ids" on /select/logsql/stream_ids --
// keys no backend has ever sent -- so both merged an absent field and answered
// an empty list, under a key a storage node does not use either. The same path
// returned a different SHAPE depending on deployment mode, and the router's
// half was empty.
//
// This is the "decodes envelopes the backends no longer send" the LLD banner
// has been warning about. The key parameter is gone rather than corrected:
// a parameter that must always take one value is a way to get it wrong again.
func (s *Server) federatedValueCounts(w http.ResponseWriter, r *http.Request, path string) {
	counts := map[string]int{}
	// The SHARDS are asked without a limit.
	//
	// `limit` reached them too, so each returned only its own top N and the
	// merge combined N truncated lists. Re-applying the limit afterwards then
	// looked like it made the answer cluster-wide, and the comment below said
	// so, but a value can only survive if it was already in some shard's top N:
	//
	//   svc counts per shard {big1:4, spread:3, second:2} x3, limit=2
	//     single node   [spread:9, second:6]
	//     cluster       [spread:9, big1:4]
	//
	// second has 6 hits cluster-wide and is displaced by one with 4, because
	// second was third on every shard and never returned by any of them.
	// `limit` here defaults to 0 (unlimited) on a storage node, so deleting it
	// is enough -- unlike facets, whose default caps at 10.
	vcReq, wlOK := withoutLimits(r, nil)
	if !wlOK {
		s.refuseUnparseableQuery(w, r)
		return
	}
	vcBodies, w, ok := s.fanOutChecked(w, vcReq, path, nil)
	if !ok {
		return
	}
	for _, a := range vcBodies {
		var v struct {
			Values []query.ValueCount `json:"values"`
		}
		if !s.mergeDecode(w, r, a, &v) {
			return
		}
		for _, vc := range v.Values {
			counts[vc.Value] += vc.Count
		}
	}
	out := make([]query.ValueCount, 0, len(counts))
	for val, c := range counts {
		out = append(out, query.ValueCount{Value: val, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Value < out[j].Value
	})
	// The limit applies to the MERGED list.
	//
	// Each backend applies its own limit and the router merged what came back,
	// so `limit=2` across three shards returned up to six values -- and the
	// two kept from each shard are that shard's top two, which is not the
	// cluster's top two. A cluster-wide question needs a cluster-wide answer.
	if n := intParam(r, "limit", 0); n > 0 && len(out) > n {
		out = out[:n]
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"values": out})
}

// federatedMatrix merges stats_query_range across storage nodes by concatenating
// each shard's series (shards hold disjoint groups, so a series is one shard's).
// matrixSeries is one Prometheus matrix series: a label set and [ts, "value"]
// pairs.
type matrixSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"`
}

// federatedMatrix merges stats-range series, combining identical label sets.
//
// It CONCATENATED the shards' results, so a series present on three shards
// appeared three times with the same labels. That is not a valid matrix: a
// Prometheus client renders them as three lines, every point is drawn
// repeatedly, and any aggregation over the result counts each shard's
// contribution as a separate series. The numbers are individually correct and
// the answer is wrong.
//
// Points at the same timestamp are SUMMED, because these are counts over
// disjoint shards and the cluster's value for a bucket is the total. A
// non-additive statistic (an average, a quantile) cannot be merged this way at
// all, which is why stats_query_range over a cluster is restricted to additive
// aggregates -- see the LLD.
func (s *Server) federatedMatrix(w http.ResponseWriter, r *http.Request) {
	// The same refusal the other two stats surfaces apply.
	//
	// This function's own doc comment said "stats_query_range over a cluster is
	// meaningful only for additive aggregates" and nothing checked it, so it
	// summed whatever came back: two shards averaging 10 answered 20, and a
	// quantile was answered where /select/logsql/query and
	// /select/logsql/stats_query both refuse it with 400. One binary, the same
	// aggregate, three different answers depending on which endpoint was asked.
	if !s.rejectNonMergeableStats(w, r) {
		return
	}
	srBodies, w, ok := s.fanOutChecked(w, r, "/select/logsql/stats_query_range", nil)
	if !ok {
		return
	}
	// The operator per output name. This merge added unconditionally, so a
	// range query over min() summed the shards' minima -- a number in the right
	// units, on the right axis, that no row produced.
	// nil for a query with no stats pipe: nothing to get wrong, and summing is
	// the only defined behaviour. See federatedVector.
	ops, opsOK := query.MergeOps(r.FormValue("query"))
	if query.HasStatsPipe(r.FormValue("query")) && !opsOK {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest,
			"simdlogs: this query's aggregates cannot be combined across shards. "+
				"The router reads from the stats pipe whether each output is summed "+
				"or taken extremally, and this build does not know for at least one "+
				"of them, so the query is refused rather than summed by default")
		return
	}
	if !opsOK {
		ops = nil
	}
	type acc struct {
		metric map[string]string
		points map[string]float64
		seen   map[string]bool
		op     query.MergeOp
		order  []string // timestamps in first-seen order, sorted before output
	}
	byLabels := map[string]*acc{}
	var labelOrder []string
	for _, a := range srBodies {
		var v struct {
			Data struct {
				Result []matrixSeries `json:"result"`
			} `json:"data"`
		}
		if !s.mergeDecode(w, r, a, &v) {
			return
		}
		for _, se := range v.Data.Result {
			key := labelKey(se.Metric)
			a := byLabels[key]
			if a == nil {
				m := se.Metric
				if m == nil {
					m = map[string]string{}
				}
				op, known := query.MergeSum, true
				if ops != nil {
					op, known = ops[m["__name__"]]
				}
				if !known {
					s.writeErr(w, r, readSpec(), http.StatusBadGateway, fmt.Sprintf(
						"simdlogs: a shard returned a series named %q, which is not an "+
							"output of this query's stats pipe, so the router does not "+
							"know whether to sum it or take its extreme", m["__name__"]))
					return
				}
				a = &acc{metric: m, points: map[string]float64{},
					seen: map[string]bool{}, op: op}
				byLabels[key] = a
				labelOrder = append(labelOrder, key)
			}
			for _, pt := range se.Values {
				ts := fmt.Sprint(pt[0])
				if !a.seen[ts] {
					a.order = append(a.order, ts)
				}
				// A value this router cannot read is refused, not counted as
				// zero. matrixValue returned 0 for an unreadable value, so a
				// term went missing from a sum and a min gained a zero that
				// beats every positive value -- both plausible, both wrong.
				// federatedVector refuses the identical condition; this was the
				// odd one out.
				f, ferr := parseMatrixValue(pt[1])
				if ferr != nil {
					s.writeErr(w, r, readSpec(), http.StatusBadGateway, fmt.Sprintf(
						"simdlogs: a shard returned the value %q for series %q at %s, "+
							"which this router cannot read as a number. Counting it as "+
							"zero would change the answer without saying so",
						fmt.Sprint(pt[1]), a.metric["__name__"], ts))
					return
				}
				a.points[ts] = a.op.Combine(a.points[ts], f, !a.seen[ts])
				a.seen[ts] = true
			}
		}
	}

	sort.Strings(labelOrder)
	out := make([]matrixSeries, 0, len(labelOrder))
	for _, key := range labelOrder {
		a := byLabels[key]
		stamps := append([]string(nil), a.order...)
		for _, ts := range stamps {
			if _, err := strconv.ParseFloat(ts, 64); err != nil {
				s.writeErr(w, r, readSpec(), http.StatusBadGateway, fmt.Sprintf(
					"simdlogs: a shard returned the bucket start %q for series %q, "+
						"which this router cannot read as a number. It was silently "+
						"sorted to the epoch and emitted as 0", ts, a.metric["__name__"]))
				return
			}
		}
		sort.Slice(stamps, func(i, j int) bool {
			return matrixStamp(stamps[i]) < matrixStamp(stamps[j])
		})
		pts := make([][2]any, 0, len(stamps))
		for _, ts := range stamps {
			// The value is a STRING in the Prometheus wire format, and it goes
			// back as one: a client that parses it expects to.
			pts = append(pts, [2]any{matrixStamp(ts),
				strconv.FormatFloat(a.points[ts], 'f', -1, 64)})
		}
		out = append(out, matrixSeries{Metric: a.metric, Values: pts})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data":   map[string]any{"resultType": "matrix", "result": out},
	})
}

// matrixValue reads a point's value, which the wire format carries as a
// string.
// parseMatrixValue is matrixValue with the error kept.
//
// matrixValue substitutes 0 for anything it cannot read, which is a term
// silently missing from a sum and a zero that wins every min. The merge uses
// this; matrixValue stays for callers that genuinely have no way to report.
func parseMatrixValue(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case string:
		return strconv.ParseFloat(x, 64)
	}
	return strconv.ParseFloat(fmt.Sprint(v), 64)
}

func matrixValue(v any) float64 {
	switch x := v.(type) {
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	case float64:
		return x
	}
	return 0
}

// matrixStamp reads a point's timestamp, which is a number.
func matrixStamp(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// federatedStatsQuery merges stats across storage nodes.
//
// Two response shapes, because the endpoint has two. With `by=` the backend
// answers this repository's `{"stats":[{value,hits}]}` extension and hits sum
// per value. Without it the backend answers the Prometheus instant vector
// envelope, which is federatedVector's job.
//
// That second case used to decode `{"count":N}` from each backend. No backend
// emits that field, so the sum was always zero and the router answered
// `{"count":0}` for every query against every cluster, however much data the
// shards held -- a confident zero, and the shape a client is least likely to
// question.
//
// Non-mergeable aggregates are refused here on the same rule the LogsQL
// planner applies, so one binary does not answer the same aggregate two ways.
func (s *Server) federatedStatsQuery(w http.ResponseWriter, r *http.Request) {
	// Which SHAPE the shards send is decided by whether the query has a stats
	// pipe, not by `by=`.
	//
	// A storage node emits `{"stats":[...]}` only when StatsQueryInstant FAILS
	// -- which it does exactly when the query has no stats pipe -- and `by=` is
	// set. A query that DOES have one gets the Prometheus vector whatever `by=`
	// says. Switching on `by=` therefore decoded `{"stats":...}` out of a
	// vector envelope, which unmarshals cleanly into a nil slice, so
	// `* | stats count() c` with `by=level` answered `{"stats":[]}` -- HTTP
	// 200, empty, and mergeDecode cannot see it because nothing failed to
	// parse. A dashboard panel grouped by a label drew nothing.
	if query.HasStatsPipe(r.FormValue("query")) || r.FormValue("by") == "" {
		s.federatedVector(w, r)
		return
	}
	if !s.rejectNonMergeableStats(w, r) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	bodies, w, ok := s.fanOutChecked(w, r, "/select/logsql/stats_query", nil)
	if !ok {
		return
	}
	type vc struct {
		Value string `json:"value"`
		Hits  int    `json:"hits"`
	}
	merged := map[string]int{}
	for _, a := range bodies {
		var v struct {
			Stats []vc `json:"stats"`
		}
		if !s.mergeDecode(w, r, a, &v) {
			return
		}
		for _, s := range v.Stats {
			merged[s.Value] += s.Hits
		}
	}
	stats := make([]vc, 0, len(merged))
	for val, h := range merged {
		stats = append(stats, vc{val, h})
	}
	// Value breaks the tie: without it, equal hit counts came out of a map in
	// per-process order, so two routers over the same shards answered the same
	// query with the rows in different orders.
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].Hits != stats[j].Hits {
			return stats[i].Hits > stats[j].Hits
		}
		return stats[i].Value < stats[j].Value
	})
	json.NewEncoder(w).Encode(map[string]any{"stats": stats})
}

// federatedHits sums per-bucket histogram counts across storage nodes.
// clusterHitSeries is the shape a storage node returns: one entry per label
// set, with parallel timestamp and value arrays.
type clusterHitSeries struct {
	Fields     map[string]string `json:"fields"`
	Timestamps []string          `json:"timestamps"`
	Values     []int             `json:"values"`
	Total      int               `json:"total"`
}

// federatedHits merges dense hit series by label set and timestamp.
//
// It decoded `{"_time": ..., "hits": ...}` -- a bag of {time, count} objects
// that no backend has ever sent. /select/logsql/hits returns the reference's
// DENSE shape, so every field the merge read was absent: the router answered
// one bogus series, `[{"_time":"","hits":0}]`, on every cluster histogram.
//
// The same stale shape was in the embedded UI (task 7.4). Two independent
// readers of one endpoint had both been written against a remembered
// envelope, which is what a fixture test is for and why there is one now.
func (s *Server) federatedHits(w http.ResponseWriter, r *http.Request) {
	hitBodies, w, ok := s.fanOutChecked(w, r, "/select/logsql/hits", nil)
	if !ok {
		return
	}
	// Merged by LABEL SET first: a shard contributes its own series per label
	// set, and two shards' `{level=error}` are the same series, not two.
	// Buckets are keyed by NANOSECONDS, not by the shard's timestamp text.
	//
	// RFC3339Nano omits trailing zeros in the fractional second, so the format
	// is not fixed-width and lexicographic order is not time order: '.' (0x2E)
	// sorts before 'Z' (0x5A), which puts `00:00:00.5Z` BEFORE `00:00:00Z`.
	// `step` is a time.Duration off the query string, so `step=500ms` produces
	// exactly that mix -- whole seconds with no fraction interleaved with half
	// seconds that have one. Sorting the text drew the graph out of order.
	type acc struct {
		fields  map[string]string
		buckets map[int64]int
		total   int
	}
	byLabels := map[string]*acc{}
	var order []string
	for _, ans := range hitBodies {
		var v struct {
			Hits []clusterHitSeries `json:"hits"`
		}
		if !s.mergeDecode(w, r, ans, &v) {
			return
		}
		for _, se := range v.Hits {
			key := labelKey(se.Fields)
			a := byLabels[key]
			if a == nil {
				a = &acc{fields: se.Fields, buckets: map[int64]int{}}
				if a.fields == nil {
					a.fields = map[string]string{}
				}
				byLabels[key] = a
				order = append(order, key)
			}
			// The dense shape means the two arrays are indexed together, so
			// unequal lengths are a protocol violation, not a short read to
			// absorb: truncating to the shorter one drops counts silently.
			if len(se.Timestamps) != len(se.Values) {
				s.writeErr(w, r, readSpec(), http.StatusBadGateway, fmt.Sprintf(
					"simdlogs: shard %d returned %d timestamps and %d values for one "+
						"series; the dense shape indexes them together, so this answer "+
						"is refused rather than truncated to the shorter array",
					ans.shard, len(se.Timestamps), len(se.Values)))
				return
			}
			// Buckets summed per timestamp: shards cover the same window, so
			// the same bucket appears on each and the cluster's count for it
			// is the sum.
			for j, ts := range se.Timestamps {
				t, err := time.Parse(time.RFC3339Nano, ts)
				// UnixNano is only defined for 1677-09-21 .. 2262-04-11 and
				// wraps silently outside it, while time.Parse accepts any year:
				// 2600-01-01 became 2015-06-13, and its count was filed on --
				// and summed into -- a real 2015 bucket. The round trip is the
				// check, because the wrap has no error to test.
				if err == nil && !time.Unix(0, t.UnixNano()).UTC().Equal(t.UTC()) {
					s.writeErr(w, r, readSpec(), http.StatusBadGateway, fmt.Sprintf(
						"simdlogs: shard %d returned the bucket timestamp %q, which is "+
							"outside the range nanoseconds since the epoch can represent "+
							"(1677-09-21 to 2262-04-11). Converting it wraps to an "+
							"unrelated date, so the answer is refused", ans.shard, ts))
					return
				}
				if err != nil {
					s.writeErr(w, r, readSpec(), http.StatusBadGateway, fmt.Sprintf(
						"simdlogs: shard %d returned bucket timestamp %q, which is not "+
							"RFC3339: it cannot be ordered against the other shards' "+
							"buckets, so the answer is refused", ans.shard, ts))
					return
				}
				a.buckets[t.UnixNano()] += se.Values[j]
			}
			a.total += se.Total
		}
	}

	sort.Strings(order)
	out := make([]clusterHitSeries, 0, len(order))
	for _, key := range order {
		a := byLabels[key]
		ns := make([]int64, 0, len(a.buckets))
		for t := range a.buckets {
			ns = append(ns, t)
		}
		// Ascending and gap-free is what the dense shape means: a client
		// indexes the two arrays together.
		sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
		stamps := make([]string, 0, len(ns))
		vals := make([]int, 0, len(ns))
		for _, t := range ns {
			stamps = append(stamps, time.Unix(0, t).UTC().Format(time.RFC3339Nano))
			vals = append(vals, a.buckets[t])
		}
		out = append(out, clusterHitSeries{
			Fields: a.fields, Timestamps: stamps, Values: vals, Total: a.total,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"hits": out})
}

// labelKey is a canonical key for a label set, so two shards' identical label
// sets merge whatever order their fields decoded in.
func labelKey(fields map[string]string) string {
	if len(fields) == 0 {
		return ""
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(fields[k])
		b.WriteByte(0)
	}
	return b.String()
}

// rowLineTime extracts and parses the "_time":"..." value from a result line
// for merge ordering; unparseable lines sort oldest.
func rowLineTime(line string) int64 {
	const k = `"_time":"`
	i := strings.Index(line, k)
	if i < 0 {
		return 0
	}
	i += len(k)
	j := strings.IndexByte(line[i:], '"')
	if j < 0 {
		return 0
	}
	if ns, ok := fastRFC3339Nano(line[i : i+j]); ok {
		return ns
	}
	if t, err := time.Parse(time.RFC3339Nano, line[i:i+j]); err == nil {
		return t.UnixNano()
	}
	return 0
}

// bytesToString views b as a string without copying. Safe here: the bytes are a
// slice of an already-read response body that is never mutated.
func bytesToString(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }

// fastRFC3339Nano parses the exact shape simdlogs emits --
// 2006-01-02T15:04:05[.fraction]Z -- without time.Parse, which showed up as the
// federated merge's dominant cost (the router parses a timestamp for EVERY row
// returned by every shard, just to order the merge). Anything else returns
// ok=false and the caller falls back to the general parser.
func fastRFC3339Nano(s string) (int64, bool) {
	if len(s) < 20 || s[len(s)-1] != 'Z' || s[4] != '-' || s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':' {
		return 0, false
	}
	d2 := func(at int) (int, bool) {
		a, b := s[at], s[at+1]
		if a < '0' || a > '9' || b < '0' || b > '9' {
			return 0, false
		}
		return int(a-'0')*10 + int(b-'0'), true
	}
	y1, ok1 := d2(0)
	y2, ok2 := d2(2)
	mo, ok3 := d2(5)
	dy, ok4 := d2(8)
	hh, ok5 := d2(11)
	mi, ok6 := d2(14)
	ss, ok7 := d2(17)
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7) || mo < 1 || mo > 12 {
		return 0, false
	}
	year := y1*100 + y2
	// Days from the civil epoch (Howard Hinnant's algorithm): exact, no tables.
	yy := year
	if mo <= 2 {
		yy--
	}
	era := yy / 400
	if yy < 0 {
		era = (yy - 399) / 400
	}
	yoe := yy - era*400
	mp := (mo + 9) % 12
	doy := (153*mp+2)/5 + dy - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	days := int64(era)*146097 + int64(doe) - 719468

	ns := (days*86400 + int64(hh)*3600 + int64(mi)*60 + int64(ss)) * 1e9
	if len(s) > 20 { // fractional seconds: ".123456789Z"
		if s[19] != '.' {
			return 0, false
		}
		frac, scale := int64(0), int64(100_000_000)
		for k := 20; k < len(s)-1; k++ {
			c := s[k]
			if c < '0' || c > '9' {
				return 0, false
			}
			if scale > 0 {
				frac += int64(c-'0') * scale
				scale /= 10
			}
		}
		ns += frac
	} else if len(s) != 20 {
		return 0, false
	}
	return ns, true
}

// SetIdentity records this node's place in the cluster: which shard it holds
// and which replica of it this is.
//
// Reported in every internal response. Without it a router that got a partial
// answer could say only that something was incomplete -- not which shard, so
// not which machine to look at.
func (s *Server) SetIdentity(shard, replica int) {
	s.shardID, s.replicaID = shard, replica
}

// Read completeness: what a router does when a shard cannot answer.
//
// # The failure
//
// Every merge consumed `[][]byte` with a nil entry for a shard that did not
// answer, and merged the rest. So a cluster read with one shard down returned
// the other shards' rows, with HTTP 200 and nothing anywhere in the response
// to say a third of the data was missing. A caller cannot tell that from a
// query that genuinely matched fewer rows, which makes it the worst kind of
// wrong answer: confident, plausible, and silent.
//
// # The rule
//
// A read fails unless every shard contributed a COMPLETE answer. Not "every
// shard answered" -- a shard whose store is degraded answers, and says so in
// the envelope, and that answer is missing data too.
//
// # Why partial is opt-in rather than the default
//
// A dashboard that would rather draw something than nothing is a real use, and
// so is an operator triaging with one node down. But it has to be ASKED for:
// `allow_partial_response=1`, answered 206 with headers naming the shards that
// are missing. The default is failure because the caller who did not ask has
// no way to know, and a monitoring system built on silently-partial answers
// alerts on the wrong thing at the worst time.

// partialParam is the opt-in. Named for the reference's own parameter so a
// client that already sets it keeps working.
const partialParam = "allow_partial_response"

// Response headers describing a partial answer.
const (
	HdrPartial        = "X-Simdlogs-Partial"
	HdrShardsTotal    = "X-Simdlogs-Shards-Total"
	HdrShardsAnswered = "X-Simdlogs-Shards-Answered"
	HdrShardsMissing  = "X-Simdlogs-Shards-Missing"
)

// fanOutChecked fans out, then enforces the completeness rule.
//
// It returns the bodies and true when the caller may proceed to merge. When it
// returns false it has already written the response, and the handler must
// return immediately.
// It returns the writer the handler must use: on a partial answer that writer
// forces 206 on the first write. The status cannot be set here directly --
// every handler sets its own Content-Type and then writes, so a WriteHeader in
// this function would be overtaken by the handler's first Write, which sends
// 200. Returning the writer makes the substitution visible at the call site
// rather than hidden in a wrapper the handler does not know about.
func (s *Server) fanOutChecked(
	w http.ResponseWriter, r *http.Request, path string, body []byte,
) ([]shardAnswer, http.ResponseWriter, bool) {
	fr, formBody := withFormInURL(r)
	if fr == nil {
		s.writeErr(w, r, readSpec(), http.StatusBadRequest,
			"simdlogs: the request body is not a readable form, so the query it "+
				"carries cannot be sent to the shards. Asking them the empty query "+
				"instead would answer a question you did not ask.")
		return nil, w, false
	}
	// The form overflow only applies where the caller sent no body of its own.
	//
	// The two cannot both occur today: withFormInURL returns before it looks at
	// anything unless the content type is a form, and the two endpoints that
	// build their own body (/_count, /_search) send JSON. That is a statement
	// about what CLIENTS send, not a property of this code -- so the impossible
	// case is refused rather than reasoned away. Under the previous shape the
	// query string survived a dropped formBody; now RawQuery has been cleared,
	// so dropping it would hand the shard neither, and the answer would be a
	// smaller one at HTTP 200.
	ct := ""
	switch {
	case body != nil && formBody != nil:
		s.writeErr(w, r, readSpec(), http.StatusBadRequest,
			"simdlogs: this request carries both a body of its own and a form "+
				"large enough to need one, and the router has one body to send. "+
				"Refused rather than silently dropping either: sending the form "+
				"would discard the request body, and sending the body would ask "+
				"the shards a shorter question than you asked")
		return nil, w, false
	case body == nil && formBody != nil:
		body, ct = formBody, "application/x-www-form-urlencoded"
	}
	peers := s.fanOutPeers(fr, path, body, ct)

	var missing []string
	var incomplete []string
	for i, p := range peers {
		switch {
		case !p.OK():
			missing = append(missing, fmt.Sprintf("%d(%s)", i, p.Class))
		case !p.Complete:
			// Answered, but from an incomplete store. Counted as missing for
			// the completeness rule: a shard serving from a degraded store is
			// missing data just as surely as one that did not answer, and the
			// only difference is that this one looks fine.
			incomplete = append(incomplete, strconv.Itoa(i))
		}
	}
	bad := append(append([]string(nil), missing...), incomplete...)
	if len(bad) == 0 {
		return answersOf(peers), w, true
	}

	answered := len(peers) - len(bad)
	w.Header().Set(HdrShardsTotal, strconv.Itoa(len(peers)))
	w.Header().Set(HdrShardsAnswered, strconv.Itoa(answered))
	w.Header().Set(HdrShardsMissing, strings.Join(bad, ","))

	if r.FormValue(partialParam) != "1" {
		obs.L().Error("cluster read refused: shards missing",
			obs.FieldEvent, "cluster.read_incomplete",
			obs.FieldRoute, r.URL.Path,
			"shards_total", len(peers), "shards_answered", answered,
			"missing", strings.Join(bad, ","),
			obs.FieldErrorClass, string(obs.ClassUpstream))
		s.writeErr(w, r, readSpec(), http.StatusServiceUnavailable, fmt.Sprintf(
			"simdlogs: %d of %d shards could not answer completely (%s). "+
				"This answer would have been missing data with no way for you to tell, "+
				"so it is refused. Set %s=1 to accept a partial answer, which is "+
				"returned as 206 with the missing shards named in %s",
			len(bad), len(peers), strings.Join(bad, ","), partialParam, HdrShardsMissing))
		return nil, w, false
	}

	// Asked for. 206, so a client that switches on the status sees it, and the
	// headers say exactly what is missing.
	obs.L().Warn("cluster read answered partially",
		obs.FieldEvent, "cluster.read_partial",
		obs.FieldRoute, r.URL.Path,
		"shards_total", len(peers), "shards_answered", answered,
		"missing", strings.Join(bad, ","))
	w.Header().Set(HdrPartial, "true")
	// The counter is NOT incremented here.
	//
	// PartialReads() is documented as "answers this router knowingly returned
	// incomplete", which is what an operator alerts on. Counting at this point
	// counts every read that was ALLOWED to be partial, including the ones a
	// merge then refuses with 502 -- so a refused read arrived at the operator
	// as a returned partial answer, with X-Simdlogs-Partial: true on a 502
	// telling the client the same wrong thing. partialWriter increments when it
	// actually writes the 206.
	return answersOf(peers), &partialWriter{ResponseWriter: w, srv: s}, true
}

// partialWriter answers 206 on the first write.
//
// A partial answer must be distinguishable by STATUS, not only by a header: a
// client that checks `resp.ok` sees 200 for both, and the whole point is that
// a caller cannot accidentally treat an incomplete answer as a complete one.
type partialWriter struct {
	http.ResponseWriter
	srv   *Server
	wrote bool
}

func (p *partialWriter) WriteHeader(code int) {
	if p.wrote {
		return
	}
	p.wrote = true
	if code == http.StatusOK {
		code = http.StatusPartialContent
		// Counted HERE, where a partial answer is actually being returned. A
		// merge that refuses writes a 4xx/5xx through this same writer and does
		// not count.
		if p.srv != nil {
			p.srv.notePartialRead()
		}
	} else {
		// Not a partial answer after all: the merge refused. The header set
		// before the merge would otherwise tell the client it is holding one.
		p.Header().Del(HdrPartial)
	}
	p.ResponseWriter.WriteHeader(code)
}

func (p *partialWriter) Write(b []byte) (int, error) {
	if !p.wrote {
		// StatusOK, not StatusPartialContent: WriteHeader maps 200 to 206 and
		// treats anything else as a refusal that must not be marked partial.
		// Passing 206 here took the refusal branch and deleted the header on
		// every successful partial answer.
		p.WriteHeader(http.StatusOK)
	}
	return p.ResponseWriter.Write(b)
}

// partialReads counts answers this router knowingly returned incomplete.
var partialReads atomic.Int64

func (s *Server) notePartialRead() { partialReads.Add(1) }

// PartialReads is the count, for /metrics. An operator alerts on it: a
// dashboard quietly running on partial answers is the state this whole
// mechanism exists to make visible.
func PartialReads() int64 { return partialReads.Load() }

// reframeESPaging rewrites a search body so the shards return enough for the
// coordinator to page over: from=0, size=from+size.
//
// The original body is edited as JSON rather than re-marshalled from the parsed
// struct, because esQuery does not carry every field a client may send and
// re-marshalling would silently drop the ones it does not know.
func reframeESPaging(body []byte, q esQuery) ([]byte, error) {
	size := q.Size
	if size <= 0 {
		size = esDefaultSize
	}
	need := q.From + size
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	raw["from"] = json.RawMessage("0")
	raw["size"] = json.RawMessage(strconv.Itoa(need))
	return json.Marshal(raw)
}

// sortESHits orders merged hits by @timestamp, OLDEST FIRST.
//
// Oldest first because that is what a single node returns -- measured, not
// assumed: `{"from":0,"size":2}` against one node answers doc-00 then doc-01.
// A page has to be the same page whichever it is asked of, and the first
// version of this sorted newest-first, which made every page a different set
// of documents rather than a wrongly-ordered one.
//
// A hit whose timestamp cannot be read sorts last: an unreadable timestamp
// should not displace a real document from page one.
func sortESHits(hits []json.RawMessage) {
	key := func(h json.RawMessage) string {
		var v struct {
			Source struct {
				Time string `json:"@timestamp"`
			} `json:"_source"`
		}
		if json.Unmarshal(h, &v) != nil {
			return ""
		}
		return v.Source.Time
	}
	sort.SliceStable(hits, func(i, j int) bool {
		a, b := key(hits[i]), key(hits[j])
		if a == b {
			return false
		}
		if a == "" {
			return false
		}
		if b == "" {
			return true
		}
		return a < b // oldest first, as a single node returns them
	})
}
