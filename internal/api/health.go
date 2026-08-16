package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/sebishogun/simdlogs/internal/config"
	"time"
)

// Liveness and readiness, and the difference between them.
//
// # Why they are not the same probe
//
// An orchestrator uses them for opposite actions. Liveness failing means KILL
// THE PROCESS; readiness failing means STOP SENDING IT TRAFFIC. Conflating
// them is how a full disk turns into a crash loop: the store is degraded, the
// probe goes red, the process is killed, it restarts onto the same full disk,
// and the restart destroys the buffered rows a graceful drain would have
// flushed. So liveness here is about the PROCESS and nothing else -- if the
// handler runs, the process is alive -- and every condition about the store,
// the disk and the cluster lives in readiness.
//
// The one thing liveness does report is shutdown. A process that has begun
// draining is going to exit, and an orchestrator that keeps probing it green
// keeps routing to it until the connection fails.
//
// # Why the states are a matrix and not a boolean
//
// "Not ready" was one bit and a line of text. An operator reading it could not
// tell "still opening the stores" from "the disk is full" from "two of five
// peers are gone" -- three conditions with three different responses, one of
// which (starting) resolves by waiting and two of which do not. Each state
// here names itself, says whether it is expected to clear on its own, and
// carries the detail that identifies which tenant or peer.
//
// # Why the detail is authorized
//
// The JSON form names tenants, peer URLs and byte counts. That is the shape of
// an internal inventory, and a probe endpoint is the one place a server hands
// out information to an unauthenticated caller by design. Unauthorized callers
// get the state and nothing else; the text routes stay as they were so an
// existing probe keeps working.

// HealthState is one condition the server can be in. The zero value is
// StateReady, so a server with nothing wrong reports ready without any
// condition having to set it.
type HealthState string

const (
	// StateReady is serving normally.
	StateReady HealthState = "ready"
	// There is deliberately no "starting" state.
	//
	// It was in the first draft of this matrix and it is not reachable:
	// NewServerConfig opens the default tenant's store and runs
	// scanDegradedTenants before it returns, and the mux does not exist until
	// it has returned -- so by the time any probe can be answered, starting is
	// over. A state that cannot occur is the dead arm this campaign has now
	// removed twice, and adding one on purpose is worse than finding one.
	// StateStorageDegraded is a tenant whose store is corrupt or quarantined.
	StateStorageDegraded HealthState = "storage_degraded"
	// StateDiskLow is free space at or below the warn reserve. Writes are
	// still accepted here -- that is the point of having a warn threshold as
	// well as a reject one.
	StateDiskLow HealthState = "disk_low"
	// StateDiskFull is at or past the reject reserve: writes are refused.
	StateDiskFull HealthState = "disk_full"
	// StateClusterIncomplete is a select-router missing peers.
	StateClusterIncomplete HealthState = "cluster_incomplete"
	// StateShuttingDown is draining. It never clears.
	StateShuttingDown HealthState = "shutting_down"
)

// severity orders the states so the reported one is the most actionable.
//
// Shutting down outranks everything because it is the only state where the
// right response is "stop routing here and do not wait". Disk full outranks
// disk low because one refuses writes and the other does not.
func (h HealthState) severity() int {
	switch h {
	case StateShuttingDown:
		return 6
	case StateDiskFull:
		return 5
	case StateStorageDegraded:
		return 4
	case StateClusterIncomplete:
		return 3
	case StateDiskLow:
		return 2
	}
	return 0
}

// A HealthCondition is one reason the server is in a state.
type HealthCondition struct {
	State HealthState `json:"state"`
	// Detail identifies the tenant, peer or number involved. It is what makes
	// the report actionable rather than merely red.
	Detail string `json:"detail"`
	// Transient reports whether the condition is expected to clear without
	// an operator doing anything.
	Transient bool `json:"transient"`
}

// HealthReport is the machine-readable answer.
type HealthReport struct {
	State      HealthState       `json:"state"`
	Ready      bool              `json:"ready"`
	Live       bool              `json:"live"`
	UptimeSecs float64           `json:"uptime_seconds"`
	Conditions []HealthCondition `json:"conditions,omitempty"`
}

// health evaluates every condition and returns the report.
//
// Ordered: the reported State is the most severe condition found, and the
// conditions list holds all of them, because an operator fixing a full disk
// still needs to know two peers are missing.
func (s *Server) health() HealthReport {
	rep := HealthReport{State: StateReady, Ready: true, Live: true}
	rep.UptimeSecs = time.Since(s.started).Seconds()

	if s.stopping.Load() {
		// Reported as NOT live as well as not ready. A draining process is
		// going to exit, and an orchestrator that keeps probing it green keeps
		// routing to it until the connection fails.
		rep.Live = false
		rep.add(HealthCondition{State: StateShuttingDown,
			Detail: "the server is draining in-flight requests", Transient: false})
		return rep.finish()
	}

	for _, t := range s.degradedSnapshot() {
		if t.health.Ready() {
			continue
		}
		rep.add(HealthCondition{State: StateStorageDegraded,
			Detail: fmt.Sprintf("tenant %s: %s", t.key, t.health), Transient: false})
	}

	for _, p := range s.storagePressureConditions() {
		rep.add(p)
	}

	if n := len(s.backends); n > 0 {
		if missing := s.unreachablePeers(); len(missing) > 0 {
			sort.Strings(missing)
			rep.add(HealthCondition{State: StateClusterIncomplete,
				Detail: fmt.Sprintf("%d of %d peers unreachable: %v", len(missing), n, missing),
				// Transient: a peer restarting comes back, and a router that
				// reported this permanently would be paged on every rolling
				// upgrade.
				Transient: true})
		}
	}
	return rep.finish()
}

func (r *HealthReport) add(c HealthCondition) { r.Conditions = append(r.Conditions, c) }

// finish sets the summary state from the conditions.
func (r *HealthReport) finish() HealthReport {
	for _, c := range r.Conditions {
		if c.State.severity() > r.State.severity() {
			r.State = c.State
		}
	}
	// Every condition makes the server not ready, `disk_low` included.
	//
	// disk_low is a state where writes still succeed, so it is tempting to
	// call it ready. It is not: the warn reserve exists precisely so readiness
	// goes red BEFORE the first write fails, giving an operator the interval
	// between the two thresholds to act. A probe that only went red once
	// writes started failing would report the outage rather than prevent it,
	// which is the shipped contract and what TestReadinessDegradesBeforeWritesFail
	// pins.
	r.Ready = r.State == StateReady
	return *r
}

// healthDetailAllowed reports whether this caller may see the full report.
//
// The metrics role, not admin: the report is the same class of information as
// /metrics -- tenant keys, byte counts, peer URLs -- and a deployment that
// already lets its monitoring scrape one should not need admin credentials for
// the other. With no -auth.config at all the server is unauthenticated by
// configuration and this follows it, exactly as /metrics does.
func (s *Server) healthDetailAllowed(r *http.Request) bool {
	st := s.auth
	if st == nil || !st.enabled {
		return true
	}
	p := principalOf(r)
	return p != nil && (p.Can(config.RoleMetrics) || p.Can(config.RoleAdmin))
}

// storagePressureConditions is storagePressure as typed conditions.
//
// The text form stays for the compatibility routes; this one separates
// "degraded but accepting" from "refusing writes", which the single string
// could not -- and which is the difference between a warning and an outage.
func (s *Server) storagePressureConditions() []HealthCondition {
	var out []HealthCondition
	s.forEachTenantDetached(func(tn *tenant) {
		st := tn.store.QuotaState()
		switch {
		case st.Reject || st.OverQuota:
			// Which reserve, named, and both causes when both trip. An
			// operator's response to "the machine is full" and "this tenant is
			// over its share" are different actions, and the state alone
			// cannot say which.
			// The cause that REFUSED is first. With warn and quota both
			// tripped the line read "writes REJECTED: 500 bytes free, below
			// the warn reserve; ..." -- leading with the one condition that
			// does not reject, so the first thing an operator reads is the
			// wrong reason for the outage.
			var why []string
			if st.Reject {
				why = append(why, fmt.Sprintf("%d bytes free, below the reject reserve",
					st.Usage.Free))
			}
			if st.OverQuota {
				why = append(why, fmt.Sprintf("%d bytes used, at its quota", st.StoreBytes))
			}
			if st.Warn && !st.Reject {
				why = append(why, fmt.Sprintf("%d bytes free, below the warn reserve",
					st.Usage.Free))
			}
			// The state word is part of the detail, because the detail is what
			// both renderings print. Without it a tenant whose writes are
			// REFUSED read identically to one that is merely degraded, and an
			// operator cannot tell an outage from a warning.
			out = append(out, HealthCondition{State: StateDiskFull,
				Detail: fmt.Sprintf("tenant %s: writes REJECTED: %s",
					tn.key, strings.Join(why, "; ")),
				Transient: true})
		case st.Warn:
			out = append(out, HealthCondition{State: StateDiskLow,
				Detail: fmt.Sprintf("tenant %s: degraded: %d bytes free, below the warn reserve",
					tn.key, st.Usage.Free),
				Transient: true})
		}
	})
	return out
}

// healthHandler answers /-/ready, /-/healthy and /health.
//
// `format=json` gives the machine-readable report to an authorized caller;
// everything else keeps the plain-text shape an existing probe parses. The
// status code is the same either way -- a probe that switched on the body
// would break, and one that switches on the code is the common case.
func (s *Server) healthHandler(kind healthKind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rep := s.health()
		s.readyOnce.Store(true)

		ok := rep.Ready
		if kind == healthLive {
			// Liveness is the process. Every store, disk and peer condition is
			// readiness's business: a full disk that killed the process would
			// restart onto the same full disk, and the restart would lose the
			// rows a graceful drain flushes.
			ok = rep.Live
		}
		code := http.StatusOK
		if !ok {
			code = http.StatusServiceUnavailable
		}

		// FROM THE URL, never r.FormValue.
		//
		// The health routes are registered bare -- no guard, so no
		// MaxBytesReader, no multipart pre-parse and no RemoveAll. FormValue
		// parses a multipart body itself, and once authentication is enabled
		// withPrincipal replaces the request with a copy, so net/http's
		// finishRequest looks at a MultipartForm that is still nil and removes
		// nothing. Measured on an authenticated server, a 33 MiB multipart
		// POST to each:
		//
		//	/health     200  multipart-105144472
		//	/-/healthy  200  multipart-842413133
		//	/-/ready    200  multipart-920247530   all three survive the close
		//
		// Unbounded, on routes that are unauthenticated by design and have no
		// body limit. Reading the URL means no body is parsed at all, which is
		// right for a probe endpoint: it has no use for one.
		if r.URL.Query().Get("format") == "json" {
			if !s.healthDetailAllowed(r) {
				// The state and nothing else. The full report names tenants,
				// peer URLs and byte counts -- an internal inventory, and a
				// probe endpoint is the one place a server answers an
				// unauthenticated caller by design.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				json.NewEncoder(w).Encode(map[string]any{
					"state": rep.State, "ready": rep.Ready, "live": rep.Live,
				})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			json.NewEncoder(w).Encode(rep)
			return
		}

		// The text form keeps the shape it had. It is what an existing probe
		// parses, and a health endpoint whose body changes shape breaks the
		// monitoring that was watching for exactly this -- the one time you
		// least want the monitoring to break.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(code)
		if ok && len(rep.Conditions) == 0 {
			w.Write([]byte("OK"))
			return
		}
		s.writeHealthText(w, rep, ok)
	}
}

// unreachablePeers is the backends whose own health probe does not answer.
//
// A select-router with peers missing is not broken -- it answers from the ones
// that are up -- but its answers are INCOMPLETE, and a client cannot tell an
// incomplete answer from a small one. So it is a readiness condition rather
// than an error: stop routing new reads here while another router with all its
// peers can serve them, and keep serving the ones already in flight.
//
// Probed with a short timeout and in parallel, because this runs on a
// readiness probe an orchestrator calls every few seconds: five sequential
// peers at a one-second timeout is a five-second probe, and an orchestrator
// whose probe times out kills the process.
func (s *Server) unreachablePeers() []string {
	peers := s.backends
	if len(peers) == 0 {
		return nil
	}
	type res struct {
		url string
		ok  bool
	}
	ch := make(chan res, len(peers))
	client := &http.Client{Timeout: peerProbeTimeout}
	for _, u := range peers {
		go func(u string) {
			resp, err := client.Get(u + "/-/healthy")
			if err != nil {
				ch <- res{u, false}
				return
			}
			resp.Body.Close()
			ch <- res{u, resp.StatusCode == http.StatusOK}
		}(u)
	}
	var down []string
	for range peers {
		r := <-ch
		if !r.ok {
			down = append(down, r.url)
		}
	}
	return down
}

// peerProbeTimeout bounds one peer's health probe. Short: this is on the
// readiness path, and a probe that takes longer than the orchestrator's own
// timeout gets the process killed for a peer being slow.
const peerProbeTimeout = 500 * time.Millisecond

// writeHealthText renders the report in the shape the previous readiness
// endpoint used: a count line per category, then one line per instance.
func (s *Server) writeHealthText(w http.ResponseWriter, rep HealthReport, ok bool) {
	byState := map[HealthState][]HealthCondition{}
	for _, c := range rep.Conditions {
		byState[c.State] = append(byState[c.State], c)
	}
	if ok {
		fmt.Fprintf(w, "OK (%s)\n", rep.State)
	}

	if n := len(byState[StateShuttingDown]); n > 0 {
		fmt.Fprintf(w, "NOT READY: shutting down\n")
	}
	if n := len(byState[StateStorageDegraded]); n > 0 {
		fmt.Fprintf(w, "NOT READY: %d degraded tenant(s)\n", n)
		for _, c := range byState[StateStorageDegraded] {
			fmt.Fprintf(w, "%s\n", c.Detail)
		}
	}
	pressure := append(byState[StateDiskFull], byState[StateDiskLow]...)
	if n := len(pressure); n > 0 {
		fmt.Fprintf(w, "NOT READY: %d tenant(s) under storage pressure\n", n)
		for _, c := range pressure {
			fmt.Fprintf(w, "%s\n", c.Detail)
		}
	}
	for _, c := range byState[StateClusterIncomplete] {
		fmt.Fprintf(w, "NOT READY: %s\n", c.Detail)
	}
}

// logErrText renders an error for a log field without a nil check at every
// call site. "" rather than "<nil>", so a query for `error != ""` means what
// it looks like. (errText is already taken in this package by the response
// error format.)
func logErrText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type healthKind uint8

const (
	healthLive healthKind = iota
	healthReady
)
