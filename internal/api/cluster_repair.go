package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
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
// A pass copies at most maxRepairGroups groups and has a maxRepairBytes
// accounting budget. Actual transferred bytes are charged after each group;
// a peer that understates its inventory can admit one final group past the
// budget, but each group is independently capped at maxRepairBytes, so the hard
// byte ceiling is at most twice the budget. A shard that has been down for a
// day can differ by more data than the cluster should move in one pass without
// starving live traffic. The pass reports what it left, so "still diverging"
// is visible rather than inferred.

// Repair bounds.
//
// Deliberately small: repair competes with live reads and writes for the same
// disks and the same network. The group count and accounting budget are
// cluster-wide per pass; maxRepairBytes is also the hard ceiling for one group
// transfer. An operator who wants faster convergence runs more passes, which
// is a decision made with the previous pass's report in hand rather than in
// advance.
const (
	maxRepairGroups = 64
	maxRepairBytes  = 1 << 30 // 1 GiB
	// repairTransferTimeout bounds ONE HOP of a group transfer: the fetch and
	// the adopt each get their own deadline. A peer that accepts the
	// connection and stalls would otherwise hold a repair pass open for as
	// long as it cared to -- and one context covering both hops let a slow
	// fetch eat the adopt's share of the time.
	repairTransferTimeout = 60 * time.Second
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
	// FROM THE URL, never r.FormValue.
	//
	// FormValue parses the BODY for a form-encoded content type, and this
	// handler's POST branch consumes that same body as the group -- so the
	// digest lookup ate the group it was about to adopt. protocols.go states
	// the rule for the ingest routes ("Read from the URL, never r.FormValue")
	// after a line-protocol write stored nothing while answering 204; this is
	// the same defect on the anti-entropy path, where the consequence is a
	// shard that can never converge.
	//
	// Found by the all-routes temp-file gate: a multipart POST here answered
	// 400 and left a temp file behind, because FormValue parsed a multipart
	// form on the request copy and nothing removed it. cluster_client sends
	// the digest as `?digest=`, so the URL is where it already is.
	digest := r.URL.Query().Get("digest")
	if digest == "" {
		s.writeErr(w, r, adminSpec(), http.StatusBadRequest,
			"simdlogs: a group is addressed by digest, not by id: an id means only "+
				"what the store that assigned it says it means")
		return
	}
	tn := s.tn(r)
	switch r.Method {
	case http.MethodGet:
		// Streamed, not buffered: a repaired group may be a gigabyte, and the
		// router spools what it fetches, so reading the whole file into memory
		// here would hold it twice.
		f, err := tn.store.OpenGroupBytes(digest)
		if err != nil {
			s.writeErr(w, r, adminSpec(), http.StatusNotFound, err.Error())
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		io.Copy(w, f)
	case http.MethodPost, http.MethodPut:
		// Bounded by the MaxBytesReader replicaGroupSpec installed, and
		// streamed rather than buffered: the body is another machine's, a
		// repaired group may be a gigabyte, and an unbounded in-memory read
		// here is an allocation driven by a peer.
		adopted, _, err := tn.store.AdoptGroupStream(digest, r.Body, func(g *storage.Reader) error {
			// Refused if this node's retention would delete it on the next
			// sweep.
			//
			// Absent is absent: anti-entropy cannot tell a group that was never
			// received from one that was deliberately dropped, so a peer holding
			// data past this node's horizon offers it back on every pass. The
			// horizon belongs to this node, so this node is the only one that can
			// say no.
			if maxAge := s.retentionMaxAge.Load(); maxAge > 0 {
				cutoff := time.Now().Add(-time.Duration(maxAge)).UnixNano()
				if g.TimeMax < cutoff {
					return fmt.Errorf("%w: its newest row (%d) is older than this "+
						"node's retention horizon (%d). It was deleted here "+
						"deliberately, and adopting it would resurrect rows "+
						"retention removed",
						errRetentionRefused, g.TimeMax, cutoff)
				}
			}
			return nil
		})
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			s.writeErr(w, r, adminSpec(), http.StatusRequestEntityTooLarge,
				fmt.Sprintf("simdlogs: a repaired group may not exceed %d bytes", s.replicaGroupLimit))
			return
		}
		if errors.Is(err, errRetentionRefused) {
			s.writeErr(w, r, adminSpec(), http.StatusConflict, err.Error())
			return
		}
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
	// Declined is groups a destination refused because it already had them --
	// its reported state was stale by the time the transfer landed. Not an
	// error and not a copy; recorded because a pass where every group is
	// declined and nothing else is counted would otherwise be indistinguishable
	// from a shard that was already in step.
	Declined int `json:"declined"`
	// SelfOverlapping is replicas whose own inventory holds two groups over
	// the same span. Such a store already holds duplicate rows; it is still a
	// source and a destination and it cannot certify that two groups differ.
	SelfOverlapping int `json:"selfOverlapping"`
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
	Declined  int            `json:"declined"`
	// SelfOverlapping is replicas whose own inventory contains two groups
	// covering the same time span. Such a store already holds duplicate rows,
	// and it is not trusted to certify that two groups differ.
	SelfOverlapping int `json:"selfOverlapping"`
	// Divergent is how many groups were not held by every reachable replica
	// when the pass started. Zero means the shard was already in step.
	Divergent int `json:"divergent"`
}

// repairCluster runs one bounded anti-entropy pass over every shard.
func (s *Server) repairCluster(w http.ResponseWriter, r *http.Request) {
	if !s.clusterTenant(w, r) {
		return
	}
	if len(s.backendList()) == 0 {
		s.writeErr(w, r, adminSpec(), http.StatusNotImplemented,
			"simdlogs: repair reconciles the replicas of a shard, and this node has "+
				"no backends configured, so it is not a router")
		return
	}
	// One repair at a time on this router.
	//
	// Repair reads every replica's group digests, decides what is missing, and
	// copies it. Two overlapping passes both read the same missing set before
	// either has written any of it, and both then copy it: 5 of 5 runs
	// duplicated rows, and BOTH reports said complete:true, blocked:0 -- which
	// is the exact output the runbook tells an operator to repair until they
	// see. The digest read and the copy that acts on it are separated by every
	// peer round trip in the shard, so this is check-then-act with the widest
	// window this code has.
	//
	// The backup path has admitted one at a time since it was written
	// (tenant.backupBusy). Repair, which MUTATES, had nothing.
	//
	// This latch is per-PROCESS and cannot close the two-router case: two
	// routers pointed at the same cluster both read the same missing set and
	// neither sees the other. That is closed at the DESTINATION, which is the
	// only participant that can see it already holds the group --
	// storage.AdoptGroup now holds one lock across its "do I have this?" and
	// its append, so a second POST of the same digest is a no-op however many
	// routers send it (task #428). Before, four concurrent adopts of one
	// four-row group left 2 groups and 8 rows, and eight left 3 and 12, with
	// every loser returning adopted=false.
	//
	// This latch still earns its place: it stops one router doing the whole
	// pass twice, which wastes a full round of fetches even when nothing
	// duplicates. It is the cheap half, and it is no longer the only half.
	if !s.repairBusy.CompareAndSwap(false, true) {
		s.writeErr(w, r, adminSpec(), http.StatusConflict,
			"simdlogs: a repair is already running on this router. Two overlapping "+
				"passes read the same missing set before either writes it, then both "+
				"copy it -- duplicating rows while both reports say complete. Wait for "+
				"the running pass to finish.")
		return
	}
	defer s.repairBusy.Store(false)

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
		// Which replicas hold each group.
		//
		// Two groups held by ONE replica cannot contain the same rows: they
		// come from one store, and groups within one store do not overlap.
		// That is what tells a legitimately adjacent pair from a compacted
		// alternative, and without it the overlap guard blocks the pair --
		// permanently, because the block is recomputed identically on every
		// pass. See holdersShare below.
		holders := map[string]map[int]bool{}
		for _, st := range states {
			if st.Err != "" {
				continue
			}
			for _, g := range st.Groups {
				// The WIDEST span any holder reports for this digest, not the
				// last one seen.
				//
				// This was last-writer-wins, and the guard then compared a
				// span the router took on one peer's word. A peer reporting a
				// real digest with a fabricated far-future span made the
				// overlap check miss, and a CLEAN replica was copied onto and
				// had every row duplicated -- reported as complete, with
				// selfOverlapping 0, because the fabricated spans were
				// disjoint. Widening is fail-safe: a lie can now only cause
				// MORE blocking, which is a reported stall rather than silent
				// duplication.
				if have, ok := union[g.Digest]; ok {
					if have.TimeMin < g.TimeMin {
						g.TimeMin = have.TimeMin
					}
					if have.TimeMax > g.TimeMax {
						g.TimeMax = have.TimeMax
					}
				}
				union[g.Digest] = g
			}
		}
		// A replica may CERTIFY a pair as non-duplicate only if its own
		// inventory is internally non-overlapping.
		//
		// The exemption rests on "one store cannot hold two groups with the
		// same rows", and that is false: ingesting one time range twice
		// without a write id leaves a store holding [T0,T0], [T0,T9] and
		// [T1,T9] at once. A reviewer built that with an ordinary retried
		// ingest, and the consequence was worse than the stall the exemption
		// replaced -- every pair got exempted, a CLEAN replica was copied onto,
		// every one of its rows was duplicated, and the pass reported
		// complete: true. Loud-and-intact became silent-and-destroyed.
		//
		// So the premise is checked rather than assumed, per replica, once.
		// A replica that fails is still a source and still a destination; it
		// just cannot vouch for anything.
		for _, st := range states {
			if st.Err != "" {
				continue
			}
			if bad := selfOverlap(st.Groups); bad != "" {
				sr.SelfOverlapping++
				rep.SelfOverlapping++
				rep.Errors = append(rep.Errors, fmt.Sprintf(
					"shard %d replica %d holds two groups whose spans touch or overlap "+
						"(%s). From spans alone this router cannot tell an ordinary pair "+
						"of adjacent flushes from a re-ingest that duplicated rows, so it "+
						"will not use this replica to certify that two groups differ -- "+
						"which means a replica behind it stays short until you check "+
						"whether those two groups hold the same rows",
					shardIdx, st.Replica, bad))
				rep.Complete = false
				continue
			}
			for _, g := range st.Groups {
				if holders[g.Digest] == nil {
					holders[g.Digest] = map[int]bool{}
				}
				holders[g.Digest][st.Replica] = true
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
			// The spans this destination holds, GROWN as the pass copies.
			//
			// The overlap guard checked st.Groups, the destination's inventory
			// as it was before the pass, and never the union's members against
			// each other. With two replicas that is enough, because the guard
			// is symmetric. With three it is not:
			//
			//   A = {g1[0,10], g2[10,20]}   uncompacted
			//   B = {G[0,20]}               compacted
			//   C = {}                      missed the range, or restored empty
			//   union = {g1, g2, G}
			//
			// Every one of the three overlaps NOTHING in C's empty inventory,
			// so all three were copied and C ended up holding the range twice
			// -- reported as copied: 3, blocked: 0, complete: true. That is the
			// outcome the comment below says this guard prevents.
			spans := append([]storage.GroupDigest(nil), st.Groups...)
			// TIME order, then digest for ties.
			//
			// Deterministic, so two runs against one divergence block the same
			// group. And time order rather than digest order because the
			// ordering decides which spelling of a diverged range wins: a hash
			// is uncorrelated with anything, so a compacted group could be
			// copied first and then block both of the pieces it replaced,
			// leaving the destination with fewer, coarser groups than either
			// source. Narrowest first is the conservative direction.
			digests := make([]string, 0, len(union))
			for d := range union {
				digests = append(digests, d)
			}
			sort.Slice(digests, func(i, j int) bool {
				a, b := union[digests[i]], union[digests[j]]
				if a.TimeMin != b.TimeMin {
					return a.TimeMin < b.TimeMin
				}
				if a.TimeMax != b.TimeMax {
					return a.TimeMax < b.TimeMax
				}
				return digests[i] < digests[j]
			})
			for _, digest := range digests {
				g := union[digest]
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
				if blocked := overlappingFrom(spans, g, holders); blocked != "" {
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
					// The one exit from this loop that used to report nothing:
					// not Remaining, not Blocked, not Errors, and Complete
					// stayed true. Reachable when the only holder is excluded
					// as "not this replica" because two entries resolved to the
					// same URL and the node flushed between the two state
					// reads.
					sr.Remaining++
					rep.Remaining++
					rep.Complete = false
					continue
				}
				moved, err := s.copyGroup(r, src, st.URL, digest)
				if errors.Is(err, errAlreadyHeld) {
					// The destination declined because it already had the
					// group -- its state was stale by the time the PUT landed.
					// Nothing moved, so nothing is counted as COPIED, and the
					// pass is still complete, because the group IS where it
					// needs to be.
					//
					// Counted separately rather than passed over in silence: a
					// peer that declines EVERYTHING otherwise reports copied 0,
					// remaining 0, blocked 0, complete true -- a clean bill of
					// health for a replica that may hold nothing, on nothing
					// but that replica's word.
					sr.Declined++
					rep.Declined++
					continue
				}
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
				// The destination now holds this span, so a later member of
				// the same union that covers it is blocked rather than copied
				// on top.
				spans = append(spans, g)
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
	resp := s.peers.do(r, shard, replica, u, http.MethodGet, pathReplicaState, nil, "")
	if !resp.OK() {
		st.Err = fmt.Sprintf("%s: %v", resp.Class, resp.Err)
		return st
	}
	var got ReplicaState
	if err := json.Unmarshal(resp.Body, &got); err != nil {
		st.Err = "unparseable state: " + err.Error()
		return st
	}
	// A body that PARSES but is not a state reads as an empty replica.
	//
	// `null`, `{}`, and any object with no `groups` key all unmarshal into a
	// ReplicaState with Groups == nil and Err == "". ReplicaState.Err's own
	// doc says "A state that could not be read is NOT an empty state: treating
	// it as empty would make repair try to copy the whole shard into a node
	// that already has it" -- and that is exactly what this produced, because
	// the guard covered the unmarshal error and not this. `groups` is present
	// in every real answer, including an empty replica's, so its absence is
	// the discriminator -- checked as a KEY.
	//
	// This was `bytes.Contains(resp.Body, []byte("\"groups\""))`, which matches
	// the quoted token anywhere, including inside a value: `{"note":"groups"}`
	// passed it and read as an empty replica, so repair proceeded to copy the
	// whole shard into a peer that never reported a state. Two reviewers
	// bypassed it, one of them on the first attempt, and a third confirmed the
	// key check had been described in a commit message and never written.
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(resp.Body, &keys); err != nil {
		st.Err = "the answer is not a JSON object: " + err.Error()
		return st
	}
	// The key must be present AND non-null. `{"groups":null}` has the key and
	// unmarshals to a nil Groups, which is indistinguishable from an empty
	// replica -- and a real empty node emits `"groups":[]`, so rejecting null
	// costs no false negatives.
	//
	// Recorded because the round-four commit message claimed this check was
	// here and it was not, on the same function whose round-three commit
	// message claimed the key check was here and it was not. Twice, on one
	// function, in consecutive commits.
	raw, ok := keys["groups"]
	if !ok || string(bytes.TrimSpace(raw)) == "null" {
		st.Err = "the answer parsed but is not a replica state (no usable groups key)"
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
//
// The bytes are spooled to a temp file at the fetch and streamed from it at
// the adopt, so neither hop buffers the group in this router's memory.
func (s *Server) copyGroup(r *http.Request, src, dst, digest string) (int64, error) {
	query := url.Values{"digest": {digest}}.Encode()

	// Each hop gets its OWN deadline, so a fetch that stalls cannot eat the
	// adopt's share of one shared context -- the adopt is the hop that
	// validates. A peer that accepts the connection and then stalls would
	// otherwise hold the whole repair pass open for as long as it cared to.
	fetchCtx, cancelFetch := context.WithTimeout(r.Context(), repairTransferTimeout)
	fetch := r.Clone(fetchCtx)
	fetch.URL.RawQuery = query

	// SPOOLED, not buffered, and bounded by maxRepairBytes rather than by the
	// peer client's in-memory response ceiling. A group may be a gigabyte, and
	// do()'s 256 MiB ceiling is why groups in (256 MiB, maxRepairBytes] could
	// never be repaired: the answer was discarded as malformed before the
	// destination ever saw it. A group past maxRepairBytes is refused here
	// with the same bound the destination enforces when adopting.
	f, size, got, cleanup := s.peers.spool(fetch, 0, 0, src, pathReplicaGroup, query, maxRepairBytes)
	cancelFetch()
	defer cleanup()
	if !got.OK() {
		return 0, fmt.Errorf("fetching from %s: %s: %v", src, got.Class, got.Err)
	}
	// The adopt streams the spooled file back out; the bytes never sit in
	// this router's memory. The destination still hashes everything it is
	// given against the digest, which is the validation the comment above
	// promises. A clone of the FETCH request carries the digest query.
	putCtx, cancelPut := context.WithTimeout(r.Context(), repairTransferTimeout)
	put := s.peers.doReader(fetch.Clone(putCtx), 0, 0, dst, http.MethodPost, pathReplicaGroup, f, "application/octet-stream")
	cancelPut()
	if !put.OK() {
		return 0, fmt.Errorf("adopting at %s: %s: %v", dst, put.Class, put.Err)
	}
	// The destination says whether it ADOPTED the group. AdoptGroup answers
	// 200 with {"adopted":false} when it already had it, and this used to
	// check only put.OK() -- so a pass could report copied: N, bytes: M,
	// complete: true having moved nothing new, which is the number an operator
	// reads to decide the cluster is repaired.
	var ack struct {
		Adopted *bool `json:"adopted"`
	}
	if err := json.Unmarshal(put.Body, &ack); err != nil || ack.Adopted == nil {
		return 0, fmt.Errorf("adopting at %s: the destination answered 200 with a body "+
			"this router could not read as an adoption result (%q)",
			dst, truncateLine(put.Body, 120))
	}
	if !*ack.Adopted {
		return 0, errAlreadyHeld
	}
	// What CROSSED THE WIRE, which is the only number this router observed
	// itself. Everything else here is the peer's word.
	return size, nil
}

// errAlreadyHeld is a copy the destination declined because it already had the
// group. Not an error to report and not a copy to count.
var errAlreadyHeld = errors.New("the destination already held this group")

// errRetentionRefused is an adoption this node refused because the group's
// newest row is older than its retention horizon. The horizon belongs to the
// receiver, so the receiver is the only participant that can say no -- and
// unlike errAlreadyHeld it IS an error: a group nobody may adopt is work every
// pass will attempt and none can finish.
var errRetentionRefused = errors.New(
	"simdlogs: refusing a group this node's retention horizon has already deleted")

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

// overlappingFrom is overlapping, skipping any group that shares a holder
// with g.
//
// Placed BELOW overlapping rather than above it: inserting a helper directly
// above an existing function silently re-heads that function's doc comment,
// and this commit series has now done exactly that three times -- bodiesOf's
// text ended up heading mergeDecode, and this comment and rowsBytes' each took
// over the function below them.
//
// The overlap guard is a heuristic for "these two spellings may be the same
// rows", and it is right about a compacted group against its pieces. It is
// WRONG about two ordinary adjacent groups from one store: overlapping uses a
// CLOSED interval, deliberately, so a group whose TimeMax equals the next
// one's TimeMin trips it -- and rows sharing one nanosecond split across a
// size-triggered flush produce exactly that, on any client timestamping at
// second or millisecond granularity.
//
// Before this pass grew `spans` mid-copy that was harmless: the guard only saw
// the destination's own groups, which cannot overlap each other. Growing spans
// made it reachable AND permanent -- copy g1, block g2, and every later pass
// rebuilds the same state and blocks it again, so the destination is short
// forever while reporting a healthy store. Reviewer C traced that; it is a
// defect introduced by the three-replica fix, not by the original guard.
//
// Sharing a holder is the discriminator: one replica holding both means they
// came from one store, and this file's own invariant is that groups within one
// store do not overlap.
func overlappingFrom(have []storage.GroupDigest, g storage.GroupDigest,
	holders map[string]map[int]bool) string {
	var candidates []storage.GroupDigest
	for _, h := range have {
		if holdersShare(holders[h.Digest], holders[g.Digest]) {
			continue
		}
		candidates = append(candidates, h)
	}
	return overlapping(candidates, g)
}

// selfOverlap returns a description of the first overlapping-or-touching pair
// within one replica's own inventory, or "" when there is none.
//
// # This is deliberately conservative, and the conservatism costs convergence
//
// The test is CLOSED, so it fires on two groups that merely touch at one
// instant -- which is what an ordinary pair of flushes produces on any
// second-resolution source. That replica is then excluded from `holders`, the
// exemption cannot fire, and a replica behind it stays short until an operator
// intervenes. It is reported loudly (SelfOverlapping, Complete false, a named
// error) rather than silently.
//
// That is a choice between two bad outcomes, and it is the right one. The
// alternatives were measured, each by a reviewer who broke the previous one:
//
//	no check at all      a peer with duplicate groups makes every exemption
//	                     fire, a CLEAN replica is copied onto, all its rows
//	                     are duplicated, complete: true. Silent, destroys data.
//	strict (half-open)   the same, because a re-ingest of ONE instant produces
//	                     two groups with identical [T,T] spans -- structurally
//	                     identical to a legitimate adjacency.
//	closed (this)        no duplication; a permanent, reported stall on
//	                     ordinary adjacency.
//
// Repair's promise is that it never makes a replica worse, so a loud stall
// beats silent duplication.
//
// # Spans cannot decide this, and the fix is not here
//
// The two shapes are indistinguishable in [TimeMin,TimeMax] alone. Deciding
// correctly needs evidence this router does not have and cannot be given by a
// peer's own report: whether the ROWS in the overlapping instant are the same
// rows. The destination has them -- AdoptGroup parses the group and holds the
// store's index -- so that is where the decision belongs. Until it moves
// there, this stalls rather than guesses.
//
// O(n^2) over one shard's groups, run once per replica per pass. A shard holds
// tens to hundreds of groups, and this decides whether that replica may exempt
// pairs from the duplication guard, so the cost is the point.
func selfOverlap(groups []storage.GroupDigest) string {
	for i := range groups {
		for j := i + 1; j < len(groups); j++ {
			a, b := groups[i], groups[j]
			// CLOSED, and it costs a stall. Read the note above the function.
			if a.TimeMin <= b.TimeMax && b.TimeMin <= a.TimeMax {
				return fmt.Sprintf("%s [%d,%d] and %s [%d,%d]",
					shortDigest(a.Digest), a.TimeMin, a.TimeMax,
					shortDigest(b.Digest), b.TimeMin, b.TimeMax)
			}
		}
	}
	return ""
}

// holdersShare reports whether one replica holds both groups.
func holdersShare(a, b map[int]bool) bool {
	for r := range a {
		if b[r] {
			return true
		}
	}
	return false
}
