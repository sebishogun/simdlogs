// Package observability is the server's structured logging and audit trail.
//
// # Why structured rather than log.Printf
//
// Every operational line this server wrote was a formatted sentence:
// `tenant 7:3: flush on eviction failed: no space left on device`. A human
// reads that fine and a machine cannot: to alert on "eviction flush failures
// for tenant 7:3" you need a regex against a sentence whose wording is not a
// contract, and the wording changed three times in this campaign alone.
//
// The fields are the contract instead. `tenant`, `route`, `request_id`,
// `shard` and `error_class` mean the same thing in every line that carries
// them, so a query is `tenant=7:3 AND event=tenant.evict.flush_failed` rather
// than a pattern match, and rewording a message breaks nobody.
//
// # Why an audit stream separate from the operational one
//
// They answer to different people and have different retention. An operator
// greps the operational log while an incident is open and lets it roll; a
// security review reads the audit log months later and needs it complete.
// Mixing them means either the operational volume evicts the audit records or
// the audit retention pays for the operational volume.
//
// Audit records are also a smaller, fixed vocabulary -- authentication
// refused, backup taken, restore run, rule changed, corruption acknowledged,
// topology reloaded -- and every one of them is an action a person took or a
// person's credential took. That is the property that makes them auditable:
// each carries a subject, and "subject unknown" is itself the finding.
package observability

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
)

// Field names. Constants rather than literals at the call sites, because a
// field name is the queryable part and `tenant` in one file with `tenant_id`
// in another is two fields as far as any log backend is concerned.
// Every one of these is used by a log line. Four more were declared and never
// logged -- request_id, status, duration_ms, rows -- and a field name nobody
// writes cannot cause the drift this block exists to prevent, while its
// presence tells a reader those fields are in the logs. They are gone; the day
// a request line needs `duration_ms`, the constant costs one line to add and
// will then be true.
const (
	FieldTenant     = "tenant"
	FieldRoute      = "route"
	FieldMethod     = "method"
	FieldShard      = "shard"
	FieldErrorClass = "error_class"
	FieldEvent      = "event"
	FieldSubject    = "subject"
	FieldOutcome    = "outcome"
	FieldBytes      = "bytes"
)

// ErrorClass is the coarse kind of a failure, for alerting.
//
// A class, not a message. An operator alerts on "storage errors rose" and
// cannot alert on a sentence; and the class survives the message being
// reworded, which the messages in this repo demonstrably do not.
type ErrorClass string

const (
	ClassNone       ErrorClass = ""
	ClassClient     ErrorClass = "client"     // malformed request, bad credential
	ClassStorage    ErrorClass = "storage"    // the filesystem or the store
	ClassBudget     ErrorClass = "budget"     // a limit refused the work
	ClassUpstream   ErrorClass = "upstream"   // a cluster peer
	ClassInternal   ErrorClass = "internal"   // a bug here
	ClassCancelled  ErrorClass = "cancelled"  // the caller went away
	ClassCorruption ErrorClass = "corruption" // data that did not verify
)

// Logger is the operational logger. Package-level and atomic: every call site
// is a leaf that should not have to be handed one, and the process installs it
// once at startup.
var operational atomic.Pointer[slog.Logger]

// audit is the audit stream. Separate destination, separate retention.
var auditLog atomic.Pointer[slog.Logger]

func init() {
	// Text to stderr, matching what log.Printf did, so a deployment that
	// captures stderr keeps working before anything is configured.
	setDefault(newHandler(os.Stderr, "text", slog.LevelInfo))
}

func setDefault(h slog.Handler) {
	l := slog.New(h)
	operational.Store(l)
	auditLog.Store(l)
}

// Config selects the format and destinations.
type Config struct {
	// Format is "text" or "json". JSON for a log pipeline, text for a
	// terminal; the default is text because that is what the process wrote
	// before this existed and a silent format change breaks whatever was
	// parsing it.
	Format string
	// Level is "debug", "info", "warn" or "error".
	Level string
	// Out is where operational lines go. Nil means stderr.
	Out io.Writer
	// AuditOut is where audit records go. Nil means the same place as Out,
	// which is the honest default: claiming a separate audit trail while
	// writing both to one stream would be worse than not claiming one.
	AuditOut io.Writer
}

// Init installs the loggers. Called once at startup.
func Init(c Config) error {
	lvl, err := parseLevel(c.Level)
	if err != nil {
		return err
	}
	format := strings.ToLower(strings.TrimSpace(c.Format))
	if format == "" {
		format = "text"
	}
	if format != "text" && format != "json" {
		return errors.New("observability: -log.format must be `text` or `json`")
	}
	out := c.Out
	if out == nil {
		out = os.Stderr
	}
	operational.Store(slog.New(newHandler(out, format, lvl)))

	auditOut := c.AuditOut
	if auditOut == nil {
		auditOut = out
	}
	// Audit records are always at Info and are never filtered by the
	// operational level: "we stopped recording authentication failures because
	// someone raised the log level" is not a thing an audit trail may do.
	auditLog.Store(slog.New(newHandler(auditOut, format, slog.LevelInfo)))
	return nil
}

func newHandler(w io.Writer, format string, lvl slog.Level) slog.Handler {
	opts := &slog.HandlerOptions{Level: lvl}
	if format == "json" {
		return slog.NewJSONHandler(w, opts)
	}
	return slog.NewTextHandler(w, opts)
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, errors.New("observability: -log.level must be debug, info, warn or error")
}

// L is the operational logger.
func L() *slog.Logger { return operational.Load() }

// Audit records one auditable action.
//
// Every record carries an event name, a subject and an outcome. The subject is
// who did it -- a principal's name, or "unauthenticated", which is a fact
// rather than an absence: an audit record that omits the subject when there
// isn't one is an audit record that cannot be counted.
func Audit(ctx context.Context, event, subject, outcome string, args ...any) {
	if subject == "" {
		subject = "unauthenticated"
	}
	all := make([]any, 0, len(args)+6)
	all = append(all,
		FieldEvent, event,
		FieldSubject, subject,
		FieldOutcome, outcome,
	)
	all = append(all, args...)
	auditLog.Load().LogAttrs(ctx, slog.LevelInfo, "audit", attrs(all)...)
}

// The audit vocabulary. A closed set, because an audit trail whose event names
// are free text cannot be queried for "every authentication failure" -- and
// every one of these is an action a person or a person's credential took.
const (
	EventAuthFailed    = "auth.failed"
	EventAuthForbidden = "auth.forbidden"
	EventBackupTaken   = "admin.backup"
	// EventClusterBackup and EventClusterRepair are the CLUSTER-scope versions.
	// Distinct events because the scope differs in a way a reviewer cares
	// about: a cluster backup copies a tenant's data out of every shard at
	// once, and a repair moves a tenant's data between machines. Neither was
	// audited at all -- the single-node backup was, on the argument that "who
	// took one, when" is the first question a security review asks, and the
	// whole-cluster version of the same operation left no record anywhere.
	EventClusterBackup  = "admin.cluster_backup"
	EventClusterRepair  = "admin.cluster_repair"
	EventRestoreRun     = "admin.restore"
	EventRuleChanged    = "admin.rule_changed"
	EventCorruptionAck  = "admin.corruption_acknowledged"
	EventTopologyReload = "admin.topology_reload"
	EventRetentionRun   = "admin.retention"

	OutcomeAllowed = "allowed"
	OutcomeDenied  = "denied"
	OutcomeOK      = "ok"
	OutcomeFailed  = "failed"
)

// attrs converts an alternating key/value list to slog attributes. LogAttrs
// with pre-built attrs skips the reflective any-slice path, which matters
// because the audit call sites are on request paths.
func attrs(kv []any) []slog.Attr {
	out := make([]slog.Attr, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		k, _ := kv[i].(string)
		out = append(out, slog.Any(k, kv[i+1]))
	}
	return out
}

// SetForTest redirects both streams and returns a restore function.
func SetForTest(out, auditOut io.Writer) func() {
	prevOp, prevAudit := operational.Load(), auditLog.Load()
	operational.Store(slog.New(newHandler(out, "json", slog.LevelDebug)))
	if auditOut == nil {
		auditOut = out
	}
	auditLog.Store(slog.New(newHandler(auditOut, "json", slog.LevelInfo)))
	return func() {
		operational.Store(prevOp)
		auditLog.Store(prevAudit)
	}
}
