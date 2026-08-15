package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	obs "github.com/sebishogun/simdlogs/internal/observability"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// Anti-entropy between the replicas of a shard.
//
// # What the cluster could not do before this
//
// A write goes to every replica of its shard. Any of them can miss one -- it
// was restarting, its disk was full, the connection dropped after the router
// gave up -- and nothing brought it back into line. The replica stayed short
// forever, and a read that happened to land on it returned fewer rows with
// nothing in the response to say so.
//
// That is why ConsistencyAll is the default write level: without repair,
// "quorum" means a replica permanently missing data, and the shortfall surfaces
// as a smaller number rather than as an error. Repair is what makes a weaker
// level defensible.
//
// # The shape
//
// Groups are immutable once sealed, which makes this far simpler than general
// anti-entropy: there are no conflicting versions to reconcile, only sets that
// differ. Each replica reports the digests of the groups it holds; the union is
// what the shard should hold; each replica is sent the ones it is missing.
//
// Three properties do the safety work:
//
//   - Reconciliation is by CONTENT, never by manifest id. A group's id is
//     assigned by the store that wrote it, so two replicas agree on ids only by
//     coincidence of ordering -- and they stop agreeing precisely when one
//     misses a write. See storage.GroupDigest.
//   - Repair only ADDS. Nothing here deletes, so no repair can remove the last
//     good copy of anything, and a replica with data the others lack keeps it
//     and hands it over rather than losing it.
//   - Every transfer is validated by the RECEIVER: the bytes are hashed against
//     the digest that was requested and parsed as a group before anything is
//     committed. A peer that is compromised or on another format version cannot
//     put arbitrary bytes in this store's directory.
//
// # Bounded
//
// A pass copies at most maxRepairGroups groups and maxRepairBytes bytes. A
// shard that has been down for a day can differ by more data than the cluster
// can move in one pass without starving live traffic, and an unbounded repair
// is a self-inflicted outage. A bounded pass converges over several runs, and
// reports what it left, so "still diverging" is visible rather than inferred.

// Repair bounds.
//
// Deliberately small: repair competes with live reads and writes for the same
// disks and the same network. These are per pass and per shard -- an operator
// who wants faster convergence runs more passes, which is a decision made with
// the previous pass's report in hand rather than in advance.
const (
	maxRepairGroups = 64
	maxRepairBytes  = 1 << 30 // 1 GiB
	// repairFetchTimeout bounds one group transfer. A peer that accepts the
	// connection and stalls would otherwise hold a repair pass open for as long
	// as it cared to.
	repairFetchTimeout = 60 * time.Second
)

// Internal repair endpoints. Under /internal/ so they are recognisably not
// public API, and they carry admin authorization: the adopt endpoint writes
// into the store.
const (
	pathReplicaState = "/internal/replica/state"
	pathReplicaGroup = "/internal/replica/group"
)

// ReplicaState is what one replica reports about itself.
type ReplicaState struct {
	Shard   int    `json:"shard"`
	Replica int    `json:"replica"`
	URL     string `json:"url,omitempty"`

	// HighWatermark is the newest timestamp this replica's data covers. Two
	// replicas of a shard with different watermarks are visibly out of step
	// before any digest is compared.
	HighWatermark int64 `json:"highWatermark"`
	// Receipts is how many write ids this replica remembers. It bounds how far
	// back a retry can be recognised, and a replica whose count is far below
	// its peers has been restarted or has fallen behind.
	Receipts int `json:"receipts"`
	// Groups is every group this replica holds, by content.
	Groups []storage.GroupDigest `json:"groups"`
	// Err is set when this replica could not be asked. A state that could not
	// be read is NOT an empty state: treating it as empty would make repair try
	// to copy the whole shard into a node that already has it.
	Err string `json:"err,omitempty"`
}

// rows is how many rows this replica holds, summed from the inventory.
func (rs ReplicaState) rows() int {
	n := 0
	for _, g := range rs.Groups {
		n += g.Rows
	}
	return n
}

// serveReplicaState answers this node's own inventory.
func (s *Server) serveReplicaState(w http.ResponseWriter, r *http.Request) {
	tn := s.tn(r)
	digests, err := tn.store.GroupDigests()
	if err != nil {
		s.writeErr(w, r, adminSpec(), http.StatusInternalServerError, err.Error())
		return
	}
	st := ReplicaState{
		Shard:         s.shardID,
		Replica:       s.replicaID,
		HighWatermark: tn.store.NewestTimestamp(),
		Receipts:      tn.store.ReceiptCount(),
		Groups:        digests,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(st)
}

// serveReplicaGroup serves one group's bytes (GET) or adopts one (POST).
func (s *Server) serveReplicaGroup(w http.ResponseWriter, r *http.Request) {
	digest := r.FormValue("digest")
	if digest == "" {
		s.writeErr(w, r, adminSpec(), http.StatusBadRequest,
			"simdlogs: a group is addressed by digest, not by id: an id means only "+
				"what the store that assigned it says it means")
		return
	}
	tn := s.tn(r)
	switch r.Method {
	case http.MethodGet:
		b, err := tn.store.GroupBytes(digest)
		if err != nil {
			s.writeErr(w, r, adminSpec(), http.StatusNotFound, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(b)
	case http.MethodPost, http.MethodPut:
		// Bounded: the body is another machine's, and an unbounded read here is
		// an allocation driven by a peer.
		b, err := io.ReadAll(io.LimitReader(r.Body, maxRepairBytes+1))
		if err != nil {
			s.writeErr(w, r, adminSpec(), http.StatusBadRequest, err.Error())
			return
		}
		if int64(len(b)) > maxRepairBytes {
			s.writeErr(w, r, adminSpec(), http.StatusRequestEntityTooLarge,
				fmt.Sprintf("simdlogs: a repaired group may not exceed %d bytes", maxRepairBytes))
			return
		}
		// Refused if this node's retention would delete it on the next sweep.
		//
		// Absent is absent: anti-entropy cannot tell a group that was never
		// received from one that was deliberately dropped, so a peer holding
		// data past this node's horizon offers it back on every pass. The
		// horizon belongs to this node, so this node is the only one that can
		// say no.
		if maxAge := s.retentionMaxAge.Load(); maxAge > 0 {
			if g, gerr := storage.ReadGroup(b); gerr == nil {
				cutoff := time.Now().Add(-time.Duration(maxAge)).UnixNano()
				if g.TimeMax < cutoff {
					s.writeErr(w, r, adminSpec(), http.StatusConflict, fmt.Sprintf(
						"simdlogs: refusing a group whose newest row (%d) is older "+
							"than this node's retention horizon (%d). It was deleted "+
							"here deliberately, and adopting it would resurrect rows "+
							"retention removed",
						g.TimeMax, cutoff))
					return
				}
			}
		}
		adopted, err := tn.store.AdoptGroup(digest, b)
		if err != nil {
			// A refusal here is the validation doing its job: bytes that do not
			// hash to what was asked for, or do not parse as a group.
			obs.L().Warn("refused a repaired group",
				obs.FieldEvent, "cluster.repair_refused", "digest", digest, "err", err)
			s.writeErr(w, r, adminSpec(), http.StatusBadRequest, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"adopted": adopted, "digest": digest})
	default:
		w.Header().Set("Allow", "GET, POST, PUT")
		s.writeErr(w, r, adminSpec(), http.StatusMethodNotAllowed,
			"simdlogs: a replica group is fetched with GET and adopted with POST")
	}
}

// RepairReport is what one repair pass did.
type RepairReport struct {
	Shards    []ShardRepair `json:"shards"`
	Copied    int           `json:"copied"`
	Bytes     int64         `json:"bytes"`
	Remaining int           `json:"remaining"`
	// Blocked is groups repair REFUSED to copy because the destination may
	// already hold their rows under different bytes -- a compacted or retained
	// range. Separate from Remaining, which is work the bounds cut short and a
	// later pass finishes: a blocked group is work no pass will ever do, and it
	// needs an operator.
	Blocked int `json:"blocked"`
	// Complete is false when any replica could not be reached, or when the
	// bounds cut the pass short. A caller that treats an incomplete pass as
	// convergence has re-created the problem repair exists to solve.
	Complete bool     `json:"complete"`
	Errors   []string `json:"errors,omitempty"`
}

// ShardRepair is one shard's part of a pass.
type ShardRepair struct {
	Shard     int            `json:"shard"`
	Replicas  []ReplicaState `json:"replicas"`
	Copied    int            `json:"copied"`
	Remaining int            `json:"remaining"`
	Blocked   int            `json:"blocked"`
	// Divergent is how many groups were not held by every reachable replica
	// when the pass started. Zero means the shard was already in step.
	Divergent int `json:"divergent"`
}

// repairCluster runs one bounded anti-entropy pass over every shard.
func (s *Server) repairCluster(w http.ResponseWriter, r *http.Request) {
	if !s.clusterTenant(w, r) {
		return
	}
	if len(s.backends) == 0 {
		s.writeErr(w, r, adminSpec(), http.StatusNotImplemented,
			"simdlogs: repair reconciles the replicas of a shard, and this node has "+
				"no backends configured, so it is not a router")
		return
	}
	obs.Audit(r.Context(), obs.EventClusterRepair, subjectOf(r), obs.OutcomeOK,
		obs.FieldTenant, tenantKeyOf(r), obs.FieldRoute, r.URL.Path)
	rep := RepairReport{Complete: true}
	budget := int64(maxRepairBytes)
	copied := 0

	for shardIdx, replicas := range s.shards() {
		sr := ShardRepair{Shard: shardIdx}
		states := make([]ReplicaState, 0, len(replicas))
		for i, u := range replicas {
			st := s.askReplicaState(r, shardIdx, i, u)
			states = append(states, st)
			if st.Err != "" {
				rep.Complete = false
				rep.Errors = append(rep.Errors,
					fmt.Sprintf("shard %d replica %d (%s): %s", shardIdx, i, u, st.Err))
			}
		}
		sr.Replicas = states

		// The union of what the reachable replicas hold is what the shard
		// should hold. Unreachable replicas contribute nothing and are NOT
		// treated as empty: a node that could not be asked may hold groups
		// nobody else has, and repairing "into" it would copy the shard twice.
		union := map[string]storage.GroupDigest{}
		for _, st := range states {
			if st.Err != "" {
				continue
			}
			for _, g := range st.Groups {
				union[g.Digest] = g
			}
		}
		for _, st := range states {
			if st.Err != "" {
				continue
			}
			if len(st.Groups) != len(union) {
				sr.Divergent++
			}
		}

		for _, st := range states {
			if st.Err != "" {
				continue
			}
			have := map[string]bool{}
			for _, g := range st.Groups {
				have[g.Digest] = true
			}
			for digest, g := range union {
				if have[digest] {
					continue
				}
				// The destination must not already hold these ROWS under
				// different bytes.
				//
				// Reconciling by content assumes two replicas differ only by
				// writes one of them missed. Two local operations break that
				// without either replica being wrong:
				//
				//   compaction  {g1,g2} -> G. Same rows, new bytes, new digest.
				//               The union then holds g1, g2 AND G, each replica
				//               is "missing" the other's spelling, and repair
				//               copies both ways. Measured: 4 rows written
				//               became 8 rows on both replicas.
				//   retention   a group dropped on one replica is still in the
				//               union while another holds it, and the next pass
				//               copies the deleted rows back.
				//
				// Both are per-node timers, so replicas never do them in
				// lockstep. A group's time span is contiguous and groups within
				// one store do not overlap, so an OVERLAP is exactly the
				// signal: a genuinely missed write occupies a span the
				// destination has nothing in; a compacted or retained range
				// does not.
				//
				// Overlapping means REFUSE. Repair's promise is that it never
				// makes a replica worse, and duplicating or resurrecting rows
				// is worse than leaving a divergence an operator can see.
				if blocked := overlapping(st.Groups, g); blocked != "" {
					sr.Blocked++
					rep.Blocked++
					rep.Complete = false
					rep.Errors = append(rep.Errors, fmt.Sprintf(
						"shard %d replica %d: not copying %s [%d,%d]: its rows may "+
							"already be here as %s -- compaction or retention has "+
							"diverged these replicas, and copying would duplicate "+
							"or resurrect rows",
						shardIdx, st.Replica, shortDigest(digest), g.TimeMin, g.TimeMax,
						shortDigest(blocked)))
					continue
				}
				// The peer's SELF-REPORTED size decides only whether to try.
				// It cannot be trusted to decide what was spent: a peer
				// reporting 0 left the budget untouched and one reporting a
				// negative made it GROW, so a pass moved megabytes while
				// reporting a negative total and complete: true. Clamped, and
				// the budget is charged what actually crossed the wire.
				claimed := g.Bytes
				if claimed < 0 {
					claimed = 0
				}
				if copied >= maxRepairGroups || budget-claimed < 0 {
					sr.Remaining++
					rep.Remaining++
					rep.Complete = false
					continue
				}
				src := pickSource(states, digest, st.URL)
				if src == "" {
					continue
				}
				moved, err := s.copyGroup(r, src, st.URL, digest)
				if err != nil {
					rep.Complete = false
					rep.Errors = append(rep.Errors, fmt.Sprintf(
						"copying %s to %s: %v", shortDigest(digest), st.URL, err))
					continue
				}
				copied++
				sr.Copied++
				budget -= moved
				rep.Bytes += moved
			}
		}
		rep.Shards = append(rep.Shards, sr)
	}
	rep.Copied = copied
	obs.L().Info("cluster repair pass",
		obs.FieldEvent, "cluster.repair", "copied", rep.Copied,
		"bytes", rep.Bytes, "remaining", rep.Remaining, "complete", rep.Complete)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rep)
}

// pickSource is a reachable replica, other than the destination, that holds
// this group.
func pickSource(states []ReplicaState, digest, notThis string) string {
	for _, st := range states {
		if st.Err != "" || st.URL == notThis {
			continue
		}
		for _, g := range st.Groups {
			if g.Digest == digest {
				return st.URL
			}
		}
	}
	return ""
}

// askReplicaState reads one replica's inventory.
func (s *Server) askReplicaState(r *http.Request, shard, replica int, u string) ReplicaState {
	st := ReplicaState{Shard: shard, Replica: replica, URL: u}
	resp := s.peers.do(r, shard, replica, u, http.MethodGet, pathReplicaState, nil)
	if !resp.OK() {
		st.Err = fmt.Sprintf("%s: %v", resp.Class, resp.Err)
		return st
	}
	var got ReplicaState
	if err := json.Unmarshal(resp.Body, &got); err != nil {
		st.Err = "unparseable state: " + err.Error()
		return st
	}
	got.Shard, got.Replica, got.URL = shard, replica, u
	return got
}

// copyGroup fetches one group from src and hands it to dst.
//
// The bytes pass through this router rather than src streaming to dst
// directly. That is slower and it is what makes the transfer verifiable: the
// receiver hashes what it is given against the digest the router asked for, so
// a source that returns the wrong group is caught by the destination rather
// than trusted by it.
func (s *Server) copyGroup(r *http.Request, src, dst, digest string) (int64, error) {
	// Bounded: a peer that accepts the connection and then stalls would hold
	// the whole repair pass open for as long as it cared to.
	ctx, cancel := context.WithTimeout(r.Context(), repairFetchTimeout)
	defer cancel()
	fetch := r.Clone(ctx)
	fetch.URL.RawQuery = url.Values{"digest": {digest}}.Encode()

	got := s.peers.do(fetch, 0, 0, src, http.MethodGet, pathReplicaGroup, nil)
	if !got.OK() {
		return 0, fmt.Errorf("fetching from %s: %s: %v", src, got.Class, got.Err)
	}
	put := s.peers.do(fetch, 0, 0, dst, http.MethodPost, pathReplicaGroup, got.Body)
	if !put.OK() {
		return 0, fmt.Errorf("adopting at %s: %s: %v", dst, put.Class, put.Err)
	}
	// What CROSSED THE WIRE, which is the only number this router observed
	// itself. Everything else here is the peer's word.
	return int64(len(got.Body)), nil
}

// clusterTenant authorizes the tenant a cluster-scope admin request names, and
// stamps the RESOLVED tenant back onto the request.
//
// # Why this exists separately from withTenant
//
// /admin/cluster/backup and /admin/cluster/repair are deliberately absent from
// tenantPaths: they touch no local store, and putting them there would make a
// router open a tenant directory it will never write to. The consequence was
// that `withTenant` never ran for them, so `tenantFor` never ran, so
// `CanTenant` never ran -- and the client's RAW AccountID was then forwarded to
// every shard by forwardedHeaders. Shards normally carry no auth config of
// their own, so they honour it.
//
// A principal holding only tenant 7:0 could therefore send `AccountID: 0` to
// /admin/cluster/backup and receive tenant 0's entire archive, on the same
// server where the same claim on /select/logsql/query answers 403. The repair
// endpoint leaked the same tenant's whole group inventory.
//
// The second half matters as much: STAMPING the resolved key. A principal that
// sends no header forwards no header, and each shard then falls back to ITS
// default -- so a repair pass reported success having reconciled a tenant the
// caller never named and may not hold.
func (s *Server) clusterTenant(w http.ResponseWriter, r *http.Request) bool {
	key, err := s.tenantFor(r)
	if err != nil {
		code := authStatus(err)
		if code == http.StatusUnauthorized {
			w.Header().Set("WWW-Authenticate", `Bearer realm="simdlogs"`)
		}
		obs.Audit(r.Context(), obs.EventAuthFailed, subjectOf(r), obs.OutcomeDenied,
			obs.FieldRoute, r.URL.Path, "err", err.Error())
		s.writeErr(w, r, adminSpec(), code, err.Error())
		return false
	}
	// The resolved key, not the caller's text. Everything downstream --
	// forwardedHeaders, the audit record, the shards -- sees the tenant this
	// principal is actually authorized for.
	r.Header.Set("AccountID", key.Account)
	r.Header.Set("ProjectID", key.Project)
	return true
}

// shortDigest is a digest truncated for a message, safely.
//
// `digest[:12]` panicked on any peer whose state JSON carried a short or absent
// digest -- a missing field is version skew, not necessarily malice, and it
// took the whole repair pass down mid-loop with no report of what it had
// already copied.
func shortDigest(d string) string {
	if len(d) > 12 {
		return d[:12]
	}
	if d == "" {
		return "(empty)"
	}
	return d
}

// overlapping reports the digest of a group the destination already holds whose
// time span intersects g's, or "" when there is none.
//
// Groups within a store cover contiguous, non-overlapping spans -- each is one
// flush's rows in time order -- so an intersection means the destination
// already has rows from that range. It may hold them as the groups g was
// compacted from, or it may have dropped them to retention. Either way the
// digests differ for a reason other than a missed write, and copying is unsafe.
func overlapping(have []storage.GroupDigest, g storage.GroupDigest) string {
	for _, h := range have {
		// Closed intervals: a group whose max equals another's min shares a
		// timestamp, and a duplicate row at the boundary is still a duplicate.
		if g.TimeMin <= h.TimeMax && h.TimeMin <= g.TimeMax {
			return h.Digest
		}
	}
	return ""
}
