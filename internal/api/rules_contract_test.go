package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sebishogun/simdlogs/internal/config"
)

// What a rule is allowed to be, and what it does when it cannot run.
//
// The first version of both rule kinds evaluated over ALL history on a timer.
// That makes a metrics-from-logs gauge monotonic -- it can only ever go up, so
// it cannot fall back below a threshold -- and makes an alert fire once and
// forever. It also makes each evaluation cost grow with the store while the
// interval does not, so a rule that took 10 ms on Monday takes ten seconds a
// month later and the ticker running it has no idea.

func ruleServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

// A rule's window is the window it evaluates over -- rows outside it are not
// counted, which is what lets a gauge fall and an alert clear.
func TestARuleSeesOnlyItsWindow(t *testing.T) {
	srv, ts := ruleServer(t)
	now := time.Now().UnixNano()
	postBody(t, ts,
		recentAt(now-int64(30*time.Second), "error")+ // inside a 5m window
			recentAt(now-int64(2*time.Hour), "error")) // outside it

	if err := srv.AddMetricRule(config.MetricRule{
		Name: "recent_errors", Query: "level:=error",
		Window: config.Duration(5 * time.Minute), Interval: config.Duration(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	body := scrapeBody(t, ts)
	if !strings.Contains(body, "logs_recent_errors 1") {
		t.Fatalf("the rule counted rows outside its window:\n%s",
			grepLines(body, "logs_recent_errors"))
	}
}

// An alert clears when the condition stops holding. With an all-history window
// it could not: the count only ever grows.
func TestAnAlertClears(t *testing.T) {
	srv, ts := ruleServer(t)
	now := time.Now().UnixNano()
	// Two errors, both OLD enough to fall outside a one-minute window.
	postBody(t, ts,
		recentAt(now-int64(10*time.Minute), "error")+
			recentAt(now-int64(11*time.Minute), "error"))

	if err := srv.AddAlertRule(config.AlertRule{
		Name: "errors_now", Query: "level:=error", Op: ">", Threshold: 0,
		Window: config.Duration(time.Minute), Interval: config.Duration(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Alerts []struct {
			Name   string  `json:"name"`
			Firing bool    `json:"firing"`
			Value  float64 `json:"value"`
		}
	}
	getJSON(t, ts.URL+"/alerts", &resp)
	if len(resp.Alerts) != 1 {
		t.Fatalf("%d alerts", len(resp.Alerts))
	}
	if resp.Alerts[0].Firing {
		t.Fatalf("an alert over a one-minute window is firing on ten-minute-old data "+
			"(value %v); it can never clear", resp.Alerts[0].Value)
	}
}

// `for` holds an alert pending. Without it a single spike inside one
// evaluation window pages someone and clears before they finish reading it.
func TestAnAlertHoldsPendingForItsForDuration(t *testing.T) {
	srv, ts := ruleServer(t)
	now := time.Now().UnixNano()
	postBody(t, ts, recentAt(now-int64(time.Second), "error"))

	if err := srv.AddAlertRule(config.AlertRule{
		Name: "sustained", Query: "level:=error", Op: ">", Threshold: 0,
		Window: config.Duration(time.Minute), Interval: config.Duration(time.Hour),
		For: config.Duration(time.Hour), // far longer than this test runs
	}); err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Alerts []struct {
			Name         string `json:"name"`
			Firing       bool   `json:"firing"`
			PendingSince string `json:"pending_since"`
		}
	}
	getJSON(t, ts.URL+"/alerts", &resp)
	if resp.Alerts[0].Firing {
		t.Fatal("an alert with for=1h fired on its first evaluation")
	}
	if resp.Alerts[0].PendingSince == "" {
		t.Error("a pending alert does not report when the condition started")
	}
}

// An alert reports its own health. One that has not evaluated because its
// query keeps failing reports firing=false, which is indistinguishable from
// "everything is fine" -- the most dangerous thing an alerting system can say.
func TestARuleReportsItsOwnHealth(t *testing.T) {
	srv, ts := ruleServer(t)
	if err := srv.AddAlertRule(config.AlertRule{
		Name: "healthy", Query: "*", Op: ">", Threshold: 0,
		Window: config.Duration(time.Minute), Interval: config.Duration(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Alerts []struct {
			Name           string `json:"name"`
			Evaluations    int64  `json:"evaluations"`
			LastEvaluation string `json:"last_evaluation"`
			Error          string `json:"error"`
		}
	}
	getJSON(t, ts.URL+"/alerts", &resp)
	a := resp.Alerts[0]
	if a.Evaluations < 1 || a.LastEvaluation == "" {
		t.Fatalf("the alert does not report when it last ran: %+v", a)
	}
	if a.Error != "" {
		t.Errorf("a healthy rule reports an error: %q", a.Error)
	}

	// And a metric rule publishes its health as series.
	if err := srv.AddMetricRule(config.MetricRule{
		Name: "health_probe", Query: "*",
		Window: config.Duration(time.Minute), Interval: config.Duration(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	body := scrapeBody(t, ts)
	for _, want := range []string{
		`simdlogs_rule_evaluations_total{rule="health_probe"}`,
		`simdlogs_rule_last_evaluation_seconds{rule="health_probe"}`,
		`simdlogs_rule_failing{rule="health_probe"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
}

// A rule's series are capped, and a capped rule says so.
func TestAMetricRuleCapsItsCardinality(t *testing.T) {
	srv, ts := ruleServer(t)
	now := time.Now().UnixNano()
	var body strings.Builder
	for i := 0; i < 50; i++ {
		body.WriteString(recentAt(now-int64(i)*int64(time.Second), fmt.Sprintf("l%d", i)))
	}
	postBody(t, ts, body.String())

	if err := srv.AddMetricRule(config.MetricRule{
		Name: "capped", Query: "*", By: "level",
		Window: config.Duration(time.Hour), Interval: config.Duration(time.Hour),
		MaxSeries: 5,
	}); err != nil {
		t.Fatal(err)
	}
	scraped := scrapeBody(t, ts)
	n := strings.Count(scraped, "logs_capped{")
	if n > 5 {
		t.Fatalf("%d series published against a cap of 5", n)
	}
	if !strings.Contains(scraped, `simdlogs_rule_series_truncated{rule="capped"} 1`) {
		t.Errorf("the rule truncated its series and did not say so:\n%s",
			grepLines(scraped, "capped"))
	}
}

// Configuration is validated, and each refusal names the rule.
func TestRuleConfigurationIsValidated(t *testing.T) {
	ok := config.MetricRule{Name: "good", Query: "*", Window: config.Duration(time.Minute)}
	for _, tc := range []struct {
		name string
		mut  func(*config.MetricRule)
		want string
	}{
		{"no window", func(m *config.MetricRule) { m.Window = 0 }, "ALL history"},
		{"window too long", func(m *config.MetricRule) { m.Window = config.Duration(48 * time.Hour) }, "maximum"},
		{"bad metric name", func(m *config.MetricRule) { m.Name = "has-a-dash" }, "valid metric"},
		{"bad label name", func(m *config.MetricRule) { m.By = "has a space" }, "valid metric"},
		{"empty query", func(m *config.MetricRule) { m.Query = "  " }, "no query"},
		{"interval too small", func(m *config.MetricRule) {
			m.Interval = config.Duration(time.Millisecond)
		}, "minimum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := ok
			tc.mut(&m)
			rs := config.RuleSet{Metrics: []config.MetricRule{m}}
			err := rs.Validate()
			if err == nil {
				t.Fatalf("%+v was accepted", m)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not explain the problem (want %q)", err, tc.want)
			}
		})
	}

	// An unknown alert operator is refused. Unvalidated it fell through
	// crossed()'s default and the alert never fired: present, listed, silent.
	rs := config.RuleSet{Alerts: []config.AlertRule{{
		Name: "a", Query: "*", Op: "=>", Threshold: 1, Window: config.Duration(time.Minute),
	}}}
	if err := rs.Validate(); err == nil {
		t.Fatal("the operator `=>` was accepted; the alert would never fire")
	}
}

// A rule file is loaded, validated and strict about unknown keys.
func TestRuleFileLoads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.json")
	good := `{
	  "metrics": [{"name":"errors","query":"level:=error","window":"5m"}],
	  "alerts":  [{"name":"too_many","query":"level:=error","op":">",
	               "threshold":10,"window":"5m","for":"10m",
	               "labels":{"severity":"page"}}]
	}`
	if err := os.WriteFile(path, []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	rs, err := config.LoadRules(path)
	if err != nil {
		t.Fatalf("a valid rule file was refused: %v", err)
	}
	if len(rs.Metrics) != 1 || len(rs.Alerts) != 1 {
		t.Fatalf("%+v", rs)
	}
	// Interval defaults to the window: the rate a window that size sustains.
	if rs.Metrics[0].Interval != rs.Metrics[0].Window {
		t.Errorf("interval = %s, want the window %s",
			rs.Metrics[0].Interval.D(), rs.Metrics[0].Window.D())
	}

	// A misspelled key is a rule that does not do what its author believes,
	// and silence is how it stays that way.
	bad := `{"metrics":[{"name":"e","query":"*","window":"5m","windwo":"1h"}]}`
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.LoadRules(path); err == nil {
		t.Fatal("a misspelled key was accepted")
	}

	// A duration must be a duration string, not a bare number: 300 is
	// ambiguous between seconds and nanoseconds and Go would read it as
	// nanoseconds, which is not what anyone writing 300 means.
	bad = `{"metrics":[{"name":"e","query":"*","window":300}]}`
	os.WriteFile(path, []byte(bad), 0o600)
	if _, err := config.LoadRules(path); err == nil {
		t.Fatal("a bare number was accepted as a duration")
	}
}

// --- helpers -------------------------------------------------------------

func scrapeBody(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	r, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return string(b)
}

func grepLines(body, needle string) string {
	var out []string
	for _, l := range strings.Split(body, "\n") {
		if strings.Contains(l, needle) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
