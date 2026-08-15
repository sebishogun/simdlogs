package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Rules: metrics-from-logs and alerts, as configuration.
//
// # Why they need a window
//
// Both rule kinds evaluated over ALL HISTORY -- `From, To = 0, 1<<62` -- on a
// timer. That is wrong twice. It is wrong operationally, because a rule whose
// answer is "how many errors are there" over a store's entire lifetime only
// ever goes up: it cannot fall back below a threshold, so an alert that fires
// once fires forever, and a metrics-from-logs gauge is a monotonically
// increasing number labelled as a gauge. And it is wrong mechanically, because
// the cost of every evaluation grows with the store while the interval does
// not, so a rule that took 10 ms on Monday takes 10 s a month later and the
// timer that runs it has no idea.
//
// A window makes both right: `rate over the last 5 minutes` falls when the
// errors stop, and costs the same on day 400 as on day 1.
//
// # Why the names are validated
//
// A rule's name becomes a Prometheus metric name and its `by` field becomes a
// label name. Neither was checked. An invalid name does not fail the rule --
// it corrupts the whole `/metrics` exposition, so one bad rule takes down the
// scrape for every other metric the server publishes. The failure lands
// nowhere near its cause.
//
// # Why cardinality is capped
//
// `by` on an arbitrary field emits one series per distinct value. On a log
// server that is unbounded by construction -- `by: request_id` is a series per
// request -- and it falls over on the MONITORING system rather than here. The
// built-in metrics avoid this by exposing only aggregates; a rule cannot,
// because selecting a field is the whole point, so it gets a ceiling instead.

// RuleSet is the file `-rules.file` points at.
type RuleSet struct {
	Metrics []MetricRule `json:"metrics"`
	Alerts  []AlertRule  `json:"alerts"`
}

// MetricRule publishes a LogsQL count as a gauge.
type MetricRule struct {
	// Name becomes the metric `logs_<name>`.
	Name string `json:"name"`
	// Tenant is `account:project`; empty means the default tenant. A rule
	// hard-wired to the default tenant is a rule a multi-tenant deployment
	// cannot use, which is what these were.
	Tenant string `json:"tenant"`
	// Query is LogsQL.
	Query string `json:"query"`
	// By is an optional field to group by: one labelled series per distinct
	// value, bounded by MaxSeries.
	By string `json:"by"`
	// Window is how far back each evaluation looks. Required and positive:
	// see the package comment.
	Window Duration `json:"window"`
	// Interval is how often it is evaluated. Defaults to Window when zero,
	// which is the rate a window that size can actually sustain.
	Interval Duration `json:"interval"`
	// MaxSeries caps distinct label values. 0 takes the default.
	MaxSeries int `json:"max_series"`
}

// AlertRule fires when a LogsQL count crosses a threshold.
type AlertRule struct {
	Name      string   `json:"name"`
	Tenant    string   `json:"tenant"`
	Query     string   `json:"query"`
	Op        string   `json:"op"` // > >= < <= == !=
	Threshold float64  `json:"threshold"`
	Window    Duration `json:"window"`
	Interval  Duration `json:"interval"`
	// For is how long the condition must hold before the alert fires. 0 fires
	// on the first crossing, which is what these did and what makes an alert
	// flap on a single spike.
	For Duration `json:"for"`
	// Labels are attached to the alert. Static only: a label whose value comes
	// from the data is the cardinality problem again, one alert at a time.
	Labels map[string]string `json:"labels"`
}

// Duration is a JSON-friendly time.Duration: `"5m"` rather than 300000000000.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		var n int64
		if err2 := json.Unmarshal(b, &n); err2 == nil {
			return fmt.Errorf("rules: durations are strings like \"5m\", not %d", n)
		}
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("rules: %q is not a duration: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// D is the time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// Rule ceilings. Defaults rather than hard limits, except MaxSeries' own
// ceiling: an operator may raise the series cap and may not remove it.
const (
	DefaultRuleMaxSeries = 1000
	// MaxRuleSeriesCeiling is the highest a rule's series cap may be set to.
	// A thousand series per rule is already a lot for a log-derived gauge;
	// ten thousand is the point past which the scrape itself is the problem.
	MaxRuleSeriesCeiling = 10_000
	// MinRuleInterval stops a rule configured at "1ns" from becoming a busy
	// loop against the store.
	MinRuleInterval = time.Second
	// MaxRuleWindow bounds a single evaluation's scan. A rule is a recurring
	// query, and a recurring query over a year of data is a scheduled outage.
	MaxRuleWindow = 24 * time.Hour
)

var alertOps = map[string]bool{">": true, ">=": true, "<": true, "<=": true, "==": true, "!=": true}

// Validate checks every rule and normalizes defaults.
//
// It returns the FIRST error with the rule's name in it. A rule file is
// human-written and a report that says "invalid" without saying which line is
// a report the human has to bisect by hand.
func (rs *RuleSet) Validate() error {
	seen := map[string]bool{}
	for i := range rs.Metrics {
		m := &rs.Metrics[i]
		if err := validName(m.Name); err != nil {
			return fmt.Errorf("rules: metric rule %d: %w", i, err)
		}
		if seen["m/"+m.Name] {
			return fmt.Errorf("rules: two metric rules are named %q", m.Name)
		}
		seen["m/"+m.Name] = true
		if strings.TrimSpace(m.Query) == "" {
			return fmt.Errorf("rules: metric rule %q has no query", m.Name)
		}
		if m.By != "" {
			// The `by` field becomes a LABEL name, and an invalid label name
			// corrupts the whole exposition rather than just this rule.
			if err := validName(m.By); err != nil {
				return fmt.Errorf("rules: metric rule %q: by: %w", m.Name, err)
			}
		}
		if err := checkWindow(m.Name, m.Window); err != nil {
			return err
		}
		if m.Interval == 0 {
			m.Interval = m.Window
		}
		if err := checkInterval(m.Name, m.Interval); err != nil {
			return err
		}
		if m.MaxSeries == 0 {
			m.MaxSeries = DefaultRuleMaxSeries
		}
		if m.MaxSeries < 1 || m.MaxSeries > MaxRuleSeriesCeiling {
			return fmt.Errorf("rules: metric rule %q: max_series %d is outside 1..%d",
				m.Name, m.MaxSeries, MaxRuleSeriesCeiling)
		}
	}
	for i := range rs.Alerts {
		a := &rs.Alerts[i]
		if err := validName(a.Name); err != nil {
			return fmt.Errorf("rules: alert rule %d: %w", i, err)
		}
		if seen["a/"+a.Name] {
			return fmt.Errorf("rules: two alert rules are named %q", a.Name)
		}
		seen["a/"+a.Name] = true
		if strings.TrimSpace(a.Query) == "" {
			return fmt.Errorf("rules: alert rule %q has no query", a.Name)
		}
		if !alertOps[a.Op] {
			// Unvalidated, an unknown operator fell through `crossed`'s
			// default and the alert simply never fired -- a rule that is
			// configured, listed, and silent.
			return fmt.Errorf("rules: alert rule %q: op %q is not one of > >= < <= == !=",
				a.Name, a.Op)
		}
		if err := checkWindow(a.Name, a.Window); err != nil {
			return err
		}
		if a.Interval == 0 {
			a.Interval = a.Window
		}
		if err := checkInterval(a.Name, a.Interval); err != nil {
			return err
		}
		if a.For < 0 {
			return fmt.Errorf("rules: alert rule %q: for must not be negative", a.Name)
		}
		for k := range a.Labels {
			if err := validName(k); err != nil {
				return fmt.Errorf("rules: alert rule %q: label: %w", a.Name, err)
			}
		}
	}
	return nil
}

func checkWindow(name string, w Duration) error {
	if w <= 0 {
		return fmt.Errorf(
			"rules: %q has no window; a rule without one evaluates over ALL history, so its "+
				"answer only ever grows and its cost grows with the store", name)
	}
	if w.D() > MaxRuleWindow {
		return fmt.Errorf("rules: %q has a window of %s; the maximum is %s, because a "+
			"recurring query over more than that is a scheduled outage",
			name, w.D(), MaxRuleWindow)
	}
	return nil
}

func checkInterval(name string, iv Duration) error {
	if iv.D() < MinRuleInterval {
		return fmt.Errorf("rules: %q has an interval of %s; the minimum is %s",
			name, iv.D(), MinRuleInterval)
	}
	return nil
}

// validName accepts what Prometheus accepts for a metric or label name:
// [a-zA-Z_][a-zA-Z0-9_]*.
//
// Checked because the name is CONCATENATED into the exposition. An invalid one
// does not fail its own rule, it corrupts the whole scrape -- so one bad rule
// takes /metrics down for every other series the server publishes, and the
// failure lands nowhere near its cause.
func validName(s string) error {
	if s == "" {
		return fmt.Errorf("name is empty")
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c == '_' ||
			(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(i > 0 && c >= '0' && c <= '9')
		if !ok {
			return fmt.Errorf("%q is not a valid metric or label name "+
				"([a-zA-Z_][a-zA-Z0-9_]*)", s)
		}
	}
	return nil
}

// LoadRules reads and validates a rule file.
func LoadRules(path string) (*RuleSet, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rs RuleSet
	dec := json.NewDecoder(strings.NewReader(string(b)))
	// Strict: a misspelled key in a rule file is a rule that does not do what
	// its author believes, and silence is how it stays that way.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rs); err != nil {
		return nil, fmt.Errorf("rules: %s: %w", path, err)
	}
	if err := rs.Validate(); err != nil {
		return nil, err
	}
	return &rs, nil
}
