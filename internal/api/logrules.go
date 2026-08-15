package api

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
	obs "github.com/sebishogun/simdlogs/internal/observability"
	"github.com/sebishogun/simdlogs/internal/query"
	"github.com/sebishogun/simdlogs/internal/storage"
)

// Metrics from logs: a LogsQL query evaluated on a timer and published as a
// gauge. VictoriaLogs leans on vmalert for this; here it is native.
//
// # What was wrong with the first version
//
// Every evaluation ran `From, To = 0, 1<<62` -- ALL HISTORY. That is wrong
// twice. A gauge whose answer is "how many errors are there, ever" only goes
// up, so it can never fall back below a threshold and an alert built on it
// fires once and forever. And the cost of each evaluation grows with the store
// while the interval does not, so a rule that took 10 ms on Monday takes ten
// seconds a month later and the ticker running it has no idea. Every rule has
// a window now, and the window is what makes both the answer and the cost
// stable.
//
// The rule's name went into the exposition unvalidated. An invalid metric name
// does not fail its own rule -- it corrupts the whole /metrics output, so one
// bad rule takes the scrape down for every other series this server publishes,
// and the failure lands nowhere near its cause.
//
// `by` on an arbitrary field is one series per distinct value, which on a log
// server is unbounded by construction (`by: request_id` is a series per
// request). It is capped, and a capped rule SAYS so rather than silently
// publishing whichever subset the store's ordering produced.

// trimFloatMetric formats a gauge value compactly (whole counts render without
// a decimal point).
func trimFloatMetric(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// logRule is one metrics-from-logs rule and its last evaluation.
type logRule struct {
	spec config.MetricRule

	mu     sync.Mutex
	series map[string]float64 // label value -> count ("" key when no by)
	// The last evaluation's outcome. A rule failing for an hour used to look
	// exactly like a rule reporting zero: eval swallowed its error and left the
	// previous series standing, so /metrics kept publishing a stale number with
	// nothing to say it was stale.
	lastRun     time.Time
	lastErr     string
	truncated   bool
	evaluations int64
}

// AddMetricRule registers a rule from a spec, validates it, evaluates it once,
// and starts its ticker under the server's background lifecycle.
func (s *Server) AddMetricRule(spec config.MetricRule) error {
	rs := config.RuleSet{Metrics: []config.MetricRule{spec}}
	if err := rs.Validate(); err != nil {
		return err
	}
	spec = rs.Metrics[0] // validated and defaulted
	if _, err := query.ParseLogsQL(spec.Query); err != nil {
		return fmt.Errorf("rules: metric rule %q: %w", spec.Name, err)
	}
	r := &logRule{spec: spec, series: map[string]float64{}}
	s.rmu.Lock()
	s.rules = append(s.rules, r)
	s.rmu.Unlock()
	r.eval(s)
	s.goBackground(spec.Interval.D(), func() { r.eval(s) })
	return nil
}

// eval runs the rule over its window and swaps in the fresh series.
func (r *logRule) eval(s *Server) {
	now := time.Now()
	q, err := query.ParseLogsQL(r.spec.Query)
	if err != nil {
		r.fail(now, err)
		return
	}
	// The WINDOW, not all history.
	q.To = now.UnixNano()
	q.From = now.Add(-r.spec.Window.D()).UnixNano()
	q.Now = q.To

	st, err := s.ruleStore(r.spec.Tenant)
	if err != nil {
		r.fail(now, err)
		return
	}

	// Through the same budget every other read obeys. A rule is the one query
	// nobody is watching -- it runs on a timer with no client to hang up -- so
	// an unbounded one is unbounded forever.
	q.Bind(context.Background(), query.Limits{
		Timeout:      ruleEvalTimeout,
		MaxGroupKeys: s.limits.MaxGroupKeys,
		MaxBytes:     s.limits.MaxQueryBytes,
	})

	res := map[string]float64{}
	truncated := false
	if r.spec.By == "" {
		res[""] = float64(query.Count(st, q))
	} else {
		vcs := query.StatsByField(st, q, r.spec.By)
		if len(vcs) > r.spec.MaxSeries {
			// Capped, and said so. A truncated set published without a signal
			// is a gauge that silently answers about a subset -- and which
			// subset depends on the store's internal ordering. Sorted by count
			// first, so the subset kept is at least the largest one.
			sort.Slice(vcs, func(a, b int) bool { return vcs[a].Count > vcs[b].Count })
			vcs = vcs[:r.spec.MaxSeries]
			truncated = true
		}
		for _, vc := range vcs {
			res[vc.Value] = float64(vc.Count)
		}
	}
	if err := q.StopErr(); err != nil {
		r.fail(now, err)
		return
	}

	r.mu.Lock()
	r.series, r.lastRun, r.lastErr = res, now, ""
	r.truncated = truncated
	r.evaluations++
	r.mu.Unlock()

	if truncated {
		obs.L().Warn("metric rule truncated its series",
			obs.FieldEvent, "rule.truncated", "rule", r.spec.Name,
			"cap", r.spec.MaxSeries, "by", r.spec.By)
	}
}

// fail records an evaluation that did not complete and KEEPS the previous
// series: a gauge that drops to zero because a query failed is worse than a
// stale one, because zero is a value an alert acts on.
func (r *logRule) fail(now time.Time, err error) {
	r.mu.Lock()
	r.lastRun, r.lastErr = now, err.Error()
	r.evaluations++
	r.mu.Unlock()
	obs.L().Warn("metric rule evaluation failed",
		obs.FieldEvent, "rule.eval_failed", "rule", r.spec.Name,
		obs.FieldErrorClass, string(obs.ClassBudget), "error", err)
}

// ruleEvalTimeout bounds one evaluation. A rule has no client to hang up, so
// nothing else would ever stop it.
const ruleEvalTimeout = 30 * time.Second

// ruleStore resolves the tenant a rule runs against. Empty is the default
// tenant, which is what every rule was hard-wired to.
func (s *Server) ruleStore(tenant string) (*storage.Store, error) {
	if tenant == "" {
		return s.def.store, nil
	}
	acct, proj, _ := strings.Cut(tenant, ":")
	if proj == "" {
		proj = "0"
	}
	tn, err := s.tenant(acct, proj)
	if err != nil {
		return nil, err
	}
	return tn.store, nil
}

// writeRuleMetrics appends each rule's series, plus the rule's own health.
func (s *Server) writeRuleMetrics(w io.Writer) {
	s.rmu.Lock()
	rules := append([]*logRule(nil), s.rules...)
	s.rmu.Unlock()
	for _, r := range rules {
		r.mu.Lock()
		series, spec := r.series, r.spec
		lastRun, lastErr, truncated, evals := r.lastRun, r.lastErr, r.truncated, r.evaluations
		r.mu.Unlock()

		name := "logs_" + spec.Name
		fmt.Fprintf(w, "# TYPE %s gauge\n", name)
		if spec.By == "" {
			fmt.Fprintf(w, "%s %s\n", name, trimFloatMetric(series[""]))
		} else {
			for val, v := range series {
				fmt.Fprintf(w, "%s{%s=\"%s\"} %s\n",
					name, spec.By, escapeLabel(val), trimFloatMetric(v))
			}
		}
		// The rule's OWN health, so a rule that has been failing for an hour
		// is distinguishable from one reporting zero. Labelled by rule name
		// only -- a fixed set -- so this does not reintroduce the cardinality
		// problem it exists to report.
		lbl := fmt.Sprintf("{rule=\"%s\"}", escapeLabel(spec.Name))
		fmt.Fprintf(w, "# TYPE simdlogs_rule_evaluations_total counter\n")
		fmt.Fprintf(w, "simdlogs_rule_evaluations_total%s %d\n", lbl, evals)
		var age float64
		if !lastRun.IsZero() {
			age = time.Since(lastRun).Seconds()
		}
		fmt.Fprintf(w, "# TYPE simdlogs_rule_last_evaluation_seconds gauge\n")
		fmt.Fprintf(w, "simdlogs_rule_last_evaluation_seconds%s %s\n", lbl, trimFloatMetric(age))
		fmt.Fprintf(w, "# TYPE simdlogs_rule_failing gauge\n")
		fmt.Fprintf(w, "simdlogs_rule_failing%s %d\n", lbl, boolInt(lastErr != ""))
		fmt.Fprintf(w, "# TYPE simdlogs_rule_series_truncated gauge\n")
		fmt.Fprintf(w, "simdlogs_rule_series_truncated%s %d\n", lbl, boolInt(truncated))
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// escapeLabel escapes a Prometheus label value (backslash, quote, newline).
func escapeLabel(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	r := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n")
	return r.Replace(v)
}
