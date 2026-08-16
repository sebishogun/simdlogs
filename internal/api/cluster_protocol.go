package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

// The internal cluster wire protocol.
//
// # Why the internal protocol is not the public one
//
// A select-router asks storage nodes for partial answers and merges them. It
// did that by calling the same public endpoints a client calls and merging the
// public response bodies -- so the merge state WAS the public envelope. Three
// things follow from that, all of them bad:
//
//   - The router cannot tell a complete answer from a partial one. A public
//     response has no field for "I skipped two groups because my disk is
//     degraded", because a client has no use for one; so a router merging
//     public bodies reports a short answer as a whole one, and nothing in the
//     response says otherwise.
//   - Changing a public response shape changes the merge. The two have
//     different compatibility requirements -- a public shape is a promise to
//     clients, an internal one is a promise between versions of this binary --
//     and tying them together means neither can move.
//   - There is no version. A router talking to a node from another release
//     silently merged whatever came back. A field that moved produced a wrong
//     answer rather than an error.
//
// So internal responses carry an envelope: version, who answered, whether the
// answer is complete, how far their data goes, and either a result or a TYPED
// error. It travels in headers rather than in the body, because the bodies are
// already several shapes (NDJSON, JSON documents, Prometheus matrices) and a
// header set is the one thing they all have.
//
// # Why the version is refused rather than negotiated
//
// A router that could speak two protocol versions would have to merge results
// from both, which means every merge is written twice and tested once. During
// a rolling upgrade the honest behaviour is: a node speaking an unknown
// version is a node this router cannot use, reported as an incomplete answer
// -- which the caller can see -- rather than silently merged.

// ProtocolVersion is the internal protocol this binary speaks.
//
// Bumped when the meaning of any header below changes, NOT when a public
// response shape changes. Those are different promises: one is between
// versions of this binary, the other is to clients.
const ProtocolVersion = 1

// Internal protocol headers. Prefixed so they cannot collide with anything a
// client sends -- and so a public request that tried to forge one is
// distinguishable from a peer speaking the protocol.
const (
	HdrProtocolVersion = "X-Simdlogs-Protocol"
	HdrShardID         = "X-Simdlogs-Shard"
	HdrReplicaID       = "X-Simdlogs-Replica"
	HdrComplete        = "X-Simdlogs-Complete"
	HdrHighWatermark   = "X-Simdlogs-High-Watermark"
	// HdrNodeGeneration identifies this PROCESS. It changes on every restart
	// and never within one, which is what lets a router tell a peer that
	// restarted from a peer that fell behind -- see shardHigh.observe.
	HdrNodeGeneration = "X-Simdlogs-Generation"
	HdrErrorClass     = "X-Simdlogs-Error-Class"
	HdrTraceID        = "X-Simdlogs-Trace"
	// HdrInternal marks a request as coming from a peer rather than a client.
	// A storage node answers internal requests with the envelope; a client
	// request gets the public response unchanged.
	HdrInternal = "X-Simdlogs-Internal"
	// HdrWriteID is a replicated write's idempotency token. A retry carries
	// the same one, which is what lets a replica that already committed answer
	// duplicate instead of storing the rows twice.
	HdrWriteID = "X-Simdlogs-Write-Id"
	// HdrDuplicate marks a response to a write this node had already taken.
	HdrDuplicate = "X-Simdlogs-Duplicate"
	// HdrConsistency is the level a write must reach.
	HdrConsistency = "X-Simdlogs-Consistency"
)

// PeerErrorClass is why a peer could not answer, in terms the router acts on.
//
// A class rather than a message, for the same reason the log's error_class is:
// the router's decision is "retry another replica", "report incomplete" or
// "fail the whole request", and it cannot make that decision from a sentence.
type PeerErrorClass string

const (
	PeerOK PeerErrorClass = ""
	// PeerUnavailable is a peer that did not answer at all -- connection
	// refused, timeout, TLS failure. Another replica may serve.
	PeerUnavailable PeerErrorClass = "unavailable"
	// PeerVersionMismatch is a peer speaking a protocol this binary does not
	// know. Another replica of the same shard may be on the right version.
	PeerVersionMismatch PeerErrorClass = "version_mismatch"
	// PeerUnauthorized is a credential problem between nodes. Retrying another
	// replica is pointless -- the credential is the router's, not the peer's.
	PeerUnauthorized PeerErrorClass = "unauthorized"
	// PeerDegraded is a peer that answered from an incomplete store.
	PeerDegraded PeerErrorClass = "degraded"
	// PeerMalformed is a response this binary could not parse. Not retryable
	// on the same peer and not silently mergeable.
	PeerMalformed PeerErrorClass = "malformed"
	// PeerOverloaded is a peer that refused for a budget reason.
	PeerOverloaded PeerErrorClass = "overloaded"
	// PeerRejected is a peer that refused the REQUEST -- a 4xx that is not an
	// auth or load problem. The router sent something this peer would not
	// accept, so every replica will refuse it identically.
	//
	// Before this class existed, do() classified 401/403, 429/503 and 5xx and
	// let every other 4xx fall through as success: the peer's error body became
	// the merged answer. A 400 from one shard was merged as though it were that
	// shard's data, and an anti-entropy copy that the destination REFUSED was
	// reported as copied.
	PeerRejected PeerErrorClass = "rejected"
)

// retryAnotherReplica reports whether a different replica of the same shard is
// worth trying.
//
// Unauthorized is not: the credential is the router's and every replica will
// refuse it identically, so retrying turns one 401 into N and delays the
// report. Malformed is not either -- a peer that returned something
// unparseable will return it again.
func (c PeerErrorClass) retryAnotherReplica() bool {
	switch c {
	case PeerUnavailable, PeerVersionMismatch, PeerOverloaded:
		return true
	}
	return false
}

// PeerResponse is one peer's answer, as the router sees it.
//
// Body is the raw payload; the router's merge code owns its shape. Everything
// else is protocol, and it is the part the merge must consult before trusting
// Body.
type PeerResponse struct {
	Shard   int
	Replica int
	URL     string

	Version int
	// Complete reports whether this peer answered from its whole dataset. A
	// peer with quarantined groups, a blown budget or a partial scan says
	// false, and a router that merges it without saying so has turned a
	// partial answer into a confident one.
	Complete bool
	// Generation identifies the peer's PROCESS. A watermark that fell because
	// the peer restarted is not evidence that it is behind its siblings, and
	// without this the two are the same observation.
	Generation string

	// HighWatermark is the newest timestamp this peer's data covers. It is
	// what lets a caller tell "no results" from "no results yet": a shard that
	// has ingested nothing since yesterday reports yesterday, and a merge that
	// hid that would look identical to a shard that is up to date and empty.
	HighWatermark int64
	// BehindSibling is set by the reader when this peer's watermark is below
	// one a live sibling reported. It never refuses -- see checkWatermark --
	// but it is named in the response so the shortfall is not silent.
	BehindSibling bool
	// HasWatermark says the peer SENT one, which is not the same as sending 0.
	//
	// A peer that omits the header predates it. A peer that sends 0 has
	// accepted nothing, ever -- and collapsing the two into int64(0) let a
	// replica that had lost its whole dataset answer for a shard whose sibling
	// held data, at 200, with the shard's rows simply absent.
	HasWatermark bool
	// TraceID ties this response to the request across nodes.
	TraceID string

	Class  PeerErrorClass
	Err    error
	Body   []byte
	Status int
}

// OK reports whether this peer contributed an answer.
func (p PeerResponse) OK() bool { return p.Class == PeerOK && p.Err == nil }

// String is for logs and errors: who, and what went wrong.
func (p PeerResponse) String() string {
	if p.OK() {
		return fmt.Sprintf("shard %d replica %d (%s): ok, complete=%v",
			p.Shard, p.Replica, p.URL, p.Complete)
	}
	return fmt.Sprintf("shard %d replica %d (%s): %s: %v",
		p.Shard, p.Replica, p.URL, p.Class, p.Err)
}

// writeEnvelope stamps a storage node's response with the protocol headers.
//
// Called on the SERVING side, before the body. A header set after the first
// write is silently dropped, which would make completeness look true (the zero
// value a reader sees for a missing header is "absent", and the first version
// of this treated absent as complete).
// writeEnvelope stamps the protocol headers. hasWatermark false OMITS the
// watermark rather than sending zero: a node that did not answer from its own
// stores has no watermark to report, and a zero from it is not a statement
// about any shard. A router sending 0 is what made the whole check inert one
// level up -- the parent compared a router's zero against a leaf's real value.
func writeEnvelope(h headerSetter, shard, replica int, complete bool, highWatermark int64, hasWatermark bool, generation, traceID string) {
	h.Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
	h.Set(HdrShardID, strconv.Itoa(shard))
	h.Set(HdrReplicaID, strconv.Itoa(replica))
	h.Set(HdrComplete, strconv.FormatBool(complete))
	if hasWatermark {
		h.Set(HdrHighWatermark, strconv.FormatInt(highWatermark, 10))
	}
	h.Set(HdrNodeGeneration, generation)
	if traceID != "" {
		h.Set(HdrTraceID, traceID)
	}
}

// newNodeGeneration identifies one process, or in a test one simulated machine.
//
// Random, not the start time: two nodes started in the same second, or a clock
// that steps backwards, must not produce the same value -- and the only thing
// asked of it is that it differ across restarts and never within one. It is a
// per-SERVER value rather than a package one so a test can build two machines,
// and restart one, in a single process.
func newNodeGeneration() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A process that cannot read randomness still has to run. The fallback
		// is unique per process on any real system and the consequence of a
		// collision is a missed lag report, not a wrong answer.
		return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.Itoa(os.Getpid())
	}
	return hex.EncodeToString(b[:])
}

// headerSetter is http.Header's Set, so writeEnvelope can be tested without a
// ResponseWriter.
type headerSetter interface{ Set(key, value string) }

// serveEnvelope stamps the protocol headers on responses to INTERNAL requests.
//
// Only internal ones: a client gets the public response unchanged, because the
// envelope is a promise between versions of this binary and not part of the
// public API. A request is internal when it carries X-Simdlogs-Internal, which
// only the peer client sets.
//
// Outermost in the chain, so the headers exist before any handler writes. A
// header set after the first write is dropped silently, and a dropped Complete
// header reads as "the peer did not say" -- which a router treats as
// incomplete, so a late stamp would make every answer look partial.
func (s *Server) serveEnvelope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(HdrInternal) == "" {
			next.ServeHTTP(w, r)
			return
		}
		// A peer speaking a version this node does not know is refused HERE,
		// before the request reaches a handler that might answer it under
		// different assumptions.
		if v := r.Header.Get(HdrProtocolVersion); v != "" {
			if n, err := strconv.Atoi(v); err != nil || n != ProtocolVersion {
				w.Header().Set(HdrProtocolVersion, strconv.Itoa(ProtocolVersion))
				w.Header().Set(HdrErrorClass, string(PeerVersionMismatch))
				http.Error(w, fmt.Sprintf(
					"simdlogs: this node speaks protocol %d, the caller speaks %q",
					ProtocolVersion, v), http.StatusBadRequest)
				return
			}
		}
		// Completeness is this node's own health: a store with quarantined or
		// corrupt groups cannot claim a complete answer, whatever the query
		// did. The per-query half (a scan that stopped on a budget) rides the
		// same header and is task 8.3's.
		complete := true
		for _, t := range s.degradedSnapshot() {
			if !t.health.Ready() {
				complete = false
				break
			}
		}
		// A ROUTER CLAIMS NO WATERMARK. This node's watermark describes this
		// node's own stores, and a node with backends did not answer this read
		// from them -- its own store is empty and its watermark is zero, which
		// says nothing about the shards that actually answered.
		hw, hasHW := int64(0), len(s.backends) == 0
		if hasHW {
			hw = s.highWatermark()
		}
		writeEnvelope(w.Header(), s.shardID, s.replicaID, complete,
			hw, hasHW, s.generation, r.Header.Get(HdrTraceID))
		next.ServeHTTP(w, r)
	})
}

// highWatermark is the newest timestamp this node's data covers.
//
// It is what lets a caller tell "no results" from "no results yet": a shard
// that stopped ingesting yesterday reports yesterday, and a merge that hid
// that would look identical to a shard that is up to date and empty.
func (s *Server) highWatermark() int64 {
	var hw int64
	s.forEachTenantDetached(func(tn *tenant) {
		if t := tn.store.NewestTimestamp(); t > hw {
			hw = t
		}
	})
	// MONOTONIC, AND NODE-LEVEL. The scan above is the max over the tenants
	// that are OPEN right now and over the data they currently HOLD, and both
	// qualifiers moved it for reasons that have nothing to do with replication:
	//
	//   - evicting a tenant dropped it, so a one-node cluster reading tenant 2
	//     and then tenant 1 reported itself going backwards;
	//   - retention deleting the newest data dropped it legitimately.
	//
	// Neither is a replica falling behind, and both made the reader's refusal
	// unsafe (see checkWatermark). Keeping the running maximum removes both:
	// what this reports is the newest timestamp this NODE has accepted, which
	// only ever moves when a write arrives.
	//
	// Within the process. A restart re-derives it from the stores that load,
	// so a node whose newest data retention already deleted comes back lower --
	// the remaining reason the reader still reports rather than refuses.
	for {
		prev := s.hwOwn.Load()
		if hw <= prev {
			return prev
		}
		if s.hwOwn.CompareAndSwap(prev, hw) {
			return hw
		}
	}
}
