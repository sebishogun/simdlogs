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
	r *http.Request, shardID int, shard []string, path string, post []byte,
) PeerResponse {
	method := http.MethodGet
	if post != nil {
		method = http.MethodPost
	}
	var last PeerResponse
	for i, b := range shard {
		last = s.peers.do(r, shardID, i, b, method, path, post)
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
	w http.ResponseWriter, r *http.Request, bodies [][]byte, coordPipes []query.Pipe,
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
	var wg sync.WaitGroup
	for shardIdx, body := range bodies {
		wg.Add(1)
		go func(shardOf int, body []byte) {
			defer wg.Done()
			if body == nil {
				return
			}
			local := make([]row, 0, bytes.Count(body, []byte{'\n'}))
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
				local = append(local, row{
					t: rowLineTime(bytesToString(line)), line: line, shard: shardOf,
					seq: len(local),
				})
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}(shardIdx, body)
	}
	wg.Wait()

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
func (s *Server) fanOutPeers(r *http.Request, path string, body []byte) []PeerResponse {
	shards := s.shards()
	out := make([]PeerResponse, len(shards))
	var wg sync.WaitGroup
	for i, sh := range shards {
		wg.Add(1)
		go func(i int, sh []string) {
			defer wg.Done()
			out[i] = s.askShard(r, i, sh, path, body)
		}(i, sh)
	}
	wg.Wait()
	return out
}

// bodiesOf keeps the [][]byte shape the existing merges consume. Each merge
// moves to the PeerResponse form as it learns to report completeness (8.3).
func bodiesOf(rs []PeerResponse) [][]byte {
	out := make([][]byte, len(rs))
	for i, p := range rs {
		if p.OK() {
			out[i] = p.Body
		}
	}
	return out
}

// fanOutPost sends the same POST (body + tenant headers) to every backend and
// returns the response bodies -- the ES surface fans out this way, since its
// query is a JSON body rather than URL params.
func (s *Server) fanOutPost(r *http.Request, path string, body []byte) [][]byte {
	return bodiesOf(s.fanOutPeers(r, path, body))
}

// federatedESCount sums the ES _count across storage nodes.
func (s *Server) federatedESCount(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	total := 0
	bodies, w, ok := s.fanOutChecked(w, r, "/_count", body)
	if !ok {
		return
	}
	for _, b := range bodies {
		var v struct {
			Count int `json:"count"`
		}
		if json.Unmarshal(b, &v) == nil {
			total += v.Count
		}
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
	var want esQuery
	dec0 := json.NewDecoder(bytes.NewReader(body))
	dec0.DisallowUnknownFields()
	_ = dec0.Decode(&want)
	shardBody := body
	if b, err := reframeESPaging(body, want); err == nil {
		shardBody = b
	}

	var hits []json.RawMessage
	total := 0
	bodies, w, ok := s.fanOutChecked(w, r, "/_search", shardBody)
	if !ok {
		return
	}
	for _, b := range bodies {
		var v struct {
			Hits struct {
				Total struct {
					Value int `json:"value"`
				} `json:"total"`
				Hits []json.RawMessage `json:"hits"`
			} `json:"hits"`
		}
		if json.Unmarshal(b, &v) == nil {
			total += v.Hits.Total.Value
			hits = append(hits, v.Hits.Hits...)
		}
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
	vcReq := withoutLimits(r)
	vcBodies, w, ok := s.fanOutChecked(w, vcReq, path, nil)
	if !ok {
		return
	}
	for _, b := range vcBodies {
		var v struct {
			Values []query.ValueCount `json:"values"`
		}
		if json.Unmarshal(b, &v) == nil {
			for _, vc := range v.Values {
				counts[vc.Value] += vc.Count
			}
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
	type acc struct {
		metric map[string]string
		points map[string]float64
		order  []string // timestamps in first-seen order, sorted before output
	}
	byLabels := map[string]*acc{}
	var labelOrder []string
	for _, b := range srBodies {
		var v struct {
			Data struct {
				Result []matrixSeries `json:"result"`
			} `json:"data"`
		}
		if json.Unmarshal(b, &v) != nil {
			continue
		}
		for _, se := range v.Data.Result {
			key := labelKey(se.Metric)
			a := byLabels[key]
			if a == nil {
				a = &acc{metric: se.Metric, points: map[string]float64{}}
				if a.metric == nil {
					a.metric = map[string]string{}
				}
				byLabels[key] = a
				labelOrder = append(labelOrder, key)
			}
			for _, pt := range se.Values {
				ts := fmt.Sprint(pt[0])
				if _, seen := a.points[ts]; !seen {
					a.order = append(a.order, ts)
				}
				a.points[ts] += matrixValue(pt[1])
			}
		}
	}

	sort.Strings(labelOrder)
	out := make([]matrixSeries, 0, len(labelOrder))
	for _, key := range labelOrder {
		a := byLabels[key]
		stamps := append([]string(nil), a.order...)
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
	if r.FormValue("by") == "" {
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
	for _, b := range bodies {
		var v struct {
			Stats []vc `json:"stats"`
		}
		if json.Unmarshal(b, &v) == nil {
			for _, s := range v.Stats {
				merged[s.Value] += s.Hits
			}
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
	type acc struct {
		fields  map[string]string
		buckets map[string]int
		total   int
	}
	byLabels := map[string]*acc{}
	var order []string
	for _, b := range hitBodies {
		var v struct {
			Hits []clusterHitSeries `json:"hits"`
		}
		if json.Unmarshal(b, &v) != nil {
			continue
		}
		for _, se := range v.Hits {
			key := labelKey(se.Fields)
			a := byLabels[key]
			if a == nil {
				a = &acc{fields: se.Fields, buckets: map[string]int{}}
				if a.fields == nil {
					a.fields = map[string]string{}
				}
				byLabels[key] = a
				order = append(order, key)
			}
			// Buckets summed per timestamp: shards cover the same window, so
			// the same bucket appears on each and the cluster's count for it
			// is the sum.
			for i := range se.Timestamps {
				if i < len(se.Values) {
					a.buckets[se.Timestamps[i]] += se.Values[i]
				}
			}
			a.total += se.Total
		}
	}

	sort.Strings(order)
	out := make([]clusterHitSeries, 0, len(order))
	for _, key := range order {
		a := byLabels[key]
		stamps := make([]string, 0, len(a.buckets))
		for t := range a.buckets {
			stamps = append(stamps, t)
		}
		// Ascending and gap-free is what the dense shape means: a client
		// indexes the two arrays together.
		sort.Strings(stamps)
		vals := make([]int, 0, len(stamps))
		for _, t := range stamps {
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
) ([][]byte, http.ResponseWriter, bool) {
	peers := s.fanOutPeers(r, path, body)

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
		return bodiesOf(peers), w, true
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
	s.notePartialRead()
	return bodiesOf(peers), &partialWriter{ResponseWriter: w}, true
}

// partialWriter answers 206 on the first write.
//
// A partial answer must be distinguishable by STATUS, not only by a header: a
// client that checks `resp.ok` sees 200 for both, and the whole point is that
// a caller cannot accidentally treat an incomplete answer as a complete one.
type partialWriter struct {
	http.ResponseWriter
	wrote bool
}

func (p *partialWriter) WriteHeader(code int) {
	if p.wrote {
		return
	}
	p.wrote = true
	if code == http.StatusOK {
		code = http.StatusPartialContent
	}
	p.ResponseWriter.WriteHeader(code)
}

func (p *partialWriter) Write(b []byte) (int, error) {
	if !p.wrote {
		p.WriteHeader(http.StatusPartialContent)
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
