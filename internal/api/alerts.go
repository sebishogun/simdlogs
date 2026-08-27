package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	obs "github.com/sebishogun/simdlogs/internal/observability"
	"github.com/sebishogun/simdlogs/internal/query"
)

// Alerting over the logs: a LogsQL count crossing a threshold. VictoriaLogs
// leans on vmalert; here it is native.
//
// Every defect logrules.go had, this had too, and one more. It evaluated over
// all history, so an alert on "errors > 100" fired the first time the store
// had ever seen 101 errors and then never stopped -- the count cannot fall.
// Its operator was an unvalidated string, and `crossed`'s default returns
// false, so a typo produced a rule that is configured, listed at /alerts, and
// permanently silent. And it fired on the FIRST crossing, so a single spike
// pages someone.

// alertRule is one alert and its state.
type alertRule struct {
	spec config.AlertRule

	mu     sync.Mutex
	firing bool
	value  float64
	// since is when the condition started holding, for `for`. Zero when it is
	// not holding.
	since       time.Time
	lastRun     time.Time
	lastErr     string
	evaluations int64
}

// AddAlertRule registers an alert, evaluates it once, and re-evaluates on its
// interval under the server's background lifecycle.
func (s *Server) AddAlertRule(spec config.AlertRule) error {
	rs := config.RuleSet{Alerts: []config.AlertRule{spec}}
	if err := rs.Validate(); err != nil {
		return err
	}
	spec = rs.Alerts[0]
	if _, err := query.ParseLogsQL(spec.Query); err != nil {
		return fmt.Errorf("rules: alert rule %q: %w", spec.Name, err)
	}
	a := &alertRule{spec: spec}
	s.amu.Lock()
	s.alerts = append(s.alerts, a)
	s.amu.Unlock()
	a.eval(s)
	s.goBackground(spec.Interval.D(), func() { a.eval(s) })
	return nil
}

func (a *alertRule) eval(s *Server) {
	now := time.Now()
	q, err := query.ParseLogsQL(a.spec.Query)
	if err != nil {
		a.fail(now, err)
		return
	}
	q.SetWindow(now.Add(-a.spec.Window.D()).UnixNano(), now.UnixNano())
	q.SetNow(q.To)

	st, held, err := s.ruleStore(a.spec.Tenant)
	if err != nil {
		a.fail(now, err)
		return
	}
	if held != nil {
		defer held.inFlight.Add(-1)
	}
	q.Bind(context.Background(), query.Limits{
		Timeout:  ruleEvalTimeout,
		MaxBytes: s.limits.MaxQueryBytes,
	})

	v := float64(query.Count(st, q))
	if err := q.StopErr(); err != nil {
		a.fail(now, err)
		return
	}
	holds := crossed(v, a.spec.Op, a.spec.Threshold)

	a.mu.Lock()
	defer a.mu.Unlock()
	a.value, a.lastRun, a.lastErr = v, now, ""
	a.evaluations++
	if !holds {
		// The condition stopped holding: the alert clears and the clock
		// resets. With an all-history window this branch was unreachable for
		// any monotonically counting query, which is most of them.
		a.since, a.firing = time.Time{}, false
		return
	}
	if a.since.IsZero() {
		a.since = now
	}
	// `for`: the condition must hold for this long before it fires. Without
	// it a single spike inside one evaluation window pages someone, and the
	// alert clears before they finish reading it.
	a.firing = now.Sub(a.since) >= a.spec.For.D()
}

func (a *alertRule) fail(now time.Time, err error) {
	a.mu.Lock()
	a.lastRun, a.lastErr = now, err.Error()
	a.evaluations++
	a.mu.Unlock()
	obs.L().Warn("alert rule evaluation failed",
		obs.FieldEvent, "alert.eval_failed", "rule", a.spec.Name,
		obs.FieldErrorClass, string(obs.ClassBudget), "error", err)
}

// crossed compares against the threshold.
//
// The operator is validated at load, so the default here is unreachable for a
// configured rule. It used to be the ONLY check: an unknown operator fell
// through to false and the alert never fired, which is the worst way for a
// rule to be wrong -- it is present, listed, and silent.
func crossed(v float64, op string, t float64) bool {
	switch op {
	case ">":
		return v > t
	case ">=":
		return v >= t
	case "<":
		return v < t
	case "<=":
		return v <= t
	case "==":
		return v == t
	case "!=":
		return v != t
	}
	return false
}

// alertsHandler reports every alert's current state, including its health.
func (s *Server) alertsHandler(w http.ResponseWriter, r *http.Request) {
	type out struct {
		Name      string            `json:"name"`
		Tenant    string            `json:"tenant,omitempty"`
		Firing    bool              `json:"firing"`
		Value     float64           `json:"value"`
		Op        string            `json:"op"`
		Threshold float64           `json:"threshold"`
		Window    string            `json:"window"`
		For       string            `json:"for,omitempty"`
		Labels    map[string]string `json:"labels,omitempty"`
		// The rule's own health. An alert that has not evaluated in an hour
		// because its query keeps failing reports firing=false, which is
		// indistinguishable from "everything is fine" -- the single most
		// dangerous thing an alerting system can say.
		LastEvaluation string `json:"last_evaluation,omitempty"`
		Evaluations    int64  `json:"evaluations"`
		Error          string `json:"error,omitempty"`
		Since          string `json:"pending_since,omitempty"`
	}
	s.amu.Lock()
	rules := append([]*alertRule(nil), s.alerts...)
	s.amu.Unlock()
	res := make([]out, 0, len(rules))
	for _, a := range rules {
		a.mu.Lock()
		o := out{
			Name: a.spec.Name, Tenant: a.spec.Tenant,
			Firing: a.firing, Value: a.value,
			Op: a.spec.Op, Threshold: a.spec.Threshold,
			Window: a.spec.Window.D().String(),
			Labels: a.spec.Labels,
			Error:  a.lastErr, Evaluations: a.evaluations,
		}
		if a.spec.For > 0 {
			o.For = a.spec.For.D().String()
		}
		if !a.lastRun.IsZero() {
			o.LastEvaluation = a.lastRun.UTC().Format(time.RFC3339Nano)
		}
		if !a.since.IsZero() && !a.firing {
			o.Since = a.since.UTC().Format(time.RFC3339Nano)
		}
		a.mu.Unlock()
		res = append(res, o)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"alerts": res})
}
