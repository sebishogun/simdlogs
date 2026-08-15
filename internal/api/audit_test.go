package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	obs "github.com/sebishogun/simdlogs/internal/observability"
)

// The audit trail.
//
// Its whole value is completeness: an operator greps the operational log while
// an incident is open, and a security review reads the audit log months later
// and needs every record. "We have no record" is the answer that must not
// happen for an authentication failure or an administrative action.

// auditRecords runs f with both log streams captured and returns the audit
// records as decoded objects.
func auditRecords(t *testing.T, f func()) []map[string]any {
	t.Helper()
	var opBuf, auditBuf bytes.Buffer
	restore := obs.SetForTest(&opBuf, &auditBuf)
	defer restore()
	f()

	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(auditBuf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("audit line is not JSON: %q", line)
		}
		if rec["msg"] != "audit" {
			continue
		}
		out = append(out, rec)
	}
	return out
}

func findEvent(recs []map[string]any, event string) map[string]any {
	for _, r := range recs {
		if r[obs.FieldEvent] == event {
			return r
		}
	}
	return nil
}

// An unauthenticated request to a privileged route is recorded, with the route
// and the role it needed.
func TestAuthFailuresAreAudited(t *testing.T) {
	_, ts := authedServer(t)

	recs := auditRecords(t, func() {
		resp := do(t, ts, http.MethodGet, "/select/logsql/query?query=*", "", nil, "")
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}
	})

	rec := findEvent(recs, obs.EventAuthFailed)
	if rec == nil {
		t.Fatalf("no auth.failed record: %v", recs)
	}
	if rec[obs.FieldOutcome] != obs.OutcomeDenied {
		t.Errorf("outcome = %v", rec[obs.FieldOutcome])
	}
	// The subject is a FACT, not a gap: "unauthenticated" is countable and an
	// omitted field is not.
	if rec[obs.FieldSubject] != "unauthenticated" {
		t.Errorf("subject = %v, want unauthenticated", rec[obs.FieldSubject])
	}
	if !strings.Contains(fmt.Sprint(rec[obs.FieldRoute]), "/select/logsql/query") {
		t.Errorf("route = %v", rec[obs.FieldRoute])
	}
	// Either the role the route needed or the resolver's reason. Which one
	// depends on WHERE the refusal happened: the tenant resolver runs before
	// the mux and refuses without knowing the route's role, and requireAuth
	// knows the role but is only reached for requests the resolver let past.
	// Both are diagnostic; neither is optional.
	if rec["required_role"] == nil && rec["reason"] == nil {
		t.Errorf("the record says nothing about why: %v", rec)
	}
}

// A VALID credential reaching for a role it does not hold is a different event
// from no credential at all: one is a client that forgot to authenticate, the
// other is a principal doing something it was not given.
func TestForbiddenIsADistinctAuditEvent(t *testing.T) {
	_, ts := authedServer(t)

	recs := auditRecords(t, func() {
		// The ingest credential reaching for a query route.
		resp := do(t, ts, http.MethodGet, "/select/logsql/query?query=*", tokIngest, nil, "")
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", resp.StatusCode)
		}
	})

	if findEvent(recs, obs.EventAuthFailed) != nil {
		t.Error("a forbidden request was recorded as an authentication failure; " +
			"they are different incidents with different responses")
	}
	rec := findEvent(recs, obs.EventAuthForbidden)
	if rec == nil {
		t.Fatalf("no auth.forbidden record: %v", recs)
	}
	if rec[obs.FieldSubject] != "shipper" {
		t.Errorf("subject = %v, want the principal's name", rec[obs.FieldSubject])
	}
}

// Administrative actions are recorded with who took them.
func TestAdminActionsAreAudited(t *testing.T) {
	_, ts := authedServer(t)

	recs := auditRecords(t, func() {
		resp := do(t, ts, http.MethodPost, "/admin/acknowledge-degraded", tokAdmin, nil, "")
		resp.Body.Close()
	})
	rec := findEvent(recs, obs.EventCorruptionAck)
	if rec == nil {
		t.Fatalf("acknowledging corruption was not audited: %v", recs)
	}
	if rec[obs.FieldSubject] != "ops" {
		t.Errorf("subject = %v, want the principal who acknowledged", rec[obs.FieldSubject])
	}

	recs = auditRecords(t, func() {
		resp := do(t, ts, http.MethodGet, "/admin/backup", tokAdmin, nil, "")
		resp.Body.Close()
	})
	if rec := findEvent(recs, obs.EventBackupTaken); rec == nil {
		t.Fatalf("a backup -- a full copy of a tenant's data leaving the server -- "+
			"was not audited: %v", recs)
	} else if rec[obs.FieldSubject] != "ops" {
		t.Errorf("subject = %v", rec[obs.FieldSubject])
	}
}

// Audit records are never filtered by the operational log level. "We stopped
// recording authentication failures because someone raised the log level" is
// not a thing an audit trail may do.
func TestAuditSurvivesTheOperationalLevel(t *testing.T) {
	var auditBuf bytes.Buffer
	restore := obs.Init(obs.Config{Format: "json", Level: "error", AuditOut: &auditBuf})
	if restore != nil {
		t.Fatalf("Init: %v", restore)
	}
	t.Cleanup(func() { obs.Init(obs.Config{}) })

	obs.Audit(nil, obs.EventAuthFailed, "", obs.OutcomeDenied)
	if !strings.Contains(auditBuf.String(), obs.EventAuthFailed) {
		t.Fatalf("an audit record was filtered by -log.level=error:\n%s", auditBuf.String())
	}
}
