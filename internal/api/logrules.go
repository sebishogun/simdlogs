package api

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sebishogun/simdlogs/internal/query"
)

// trimFloatMetric formats a gauge value compactly (whole counts render without
// a decimal point).
func trimFloatMetric(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// logRule is a metrics-from-logs rule: a LogsQL query evaluated on a timer, its
// result(s) exposed as a Prometheus gauge on /metrics. With a by field it emits
// one labeled series per value (a group-by count); without, a single count.
// This is VictoriaLogs-via-vmalert's "metrics from logs", but native.
type logRule struct {
	name   string
	by     string
	raw    string
	mu     sync.Mutex
	series map[string]float64 // label value -> count ("" key when no by)
}

// AddMetricRule registers a rule and evaluates it once now; a positive interval
// starts a background re-evaluation ticker (it runs for the process lifetime).
// interval 0 evaluates once -- the form tests use.
func (s *Server) AddMetricRule(name, logsql, by string, interval time.Duration) error {
	if _, err := query.ParseLogsQL(logsql); err != nil { // validate up front
		return err
	}
	r := &logRule{name: name, by: by, raw: logsql, series: map[string]float64{}}
	s.rmu.Lock()
	s.rules = append(s.rules, r)
	s.rmu.Unlock()
	r.eval(s)
	// Under the server's background lifecycle, like the alert rules: a bare
	// ticker here outlived the stores it queries.
	s.goBackground(interval, func() { r.eval(s) })
	return nil
}

// eval runs the rule against the default tenant's store and swaps in the fresh
// series. A parse error (should not happen -- validated in AddMetricRule) leaves
// the last result standing.
func (r *logRule) eval(s *Server) {
	q, err := query.ParseLogsQL(r.raw)
	if err != nil {
		return
	}
	q.From, q.To = 0, int64(1)<<62
	res := map[string]float64{}
	if r.by == "" {
		res[""] = float64(query.Count(s.def.store, q))
	} else {
		for _, vc := range query.StatsByField(s.def.store, q, r.by) {
			res[vc.Value] = float64(vc.Count)
		}
	}
	r.mu.Lock()
	r.series = res
	r.mu.Unlock()
}

// writeRuleMetrics appends each rule's series in Prometheus exposition format.
func (s *Server) writeRuleMetrics(w io.Writer) {
	s.rmu.Lock()
	rules := append([]*logRule(nil), s.rules...)
	s.rmu.Unlock()
	for _, r := range rules {
		r.mu.Lock()
		series, by := r.series, r.by
		r.mu.Unlock()
		name := "logs_" + r.name
		fmt.Fprintf(w, "# TYPE %s gauge\n", name)
		if by == "" {
			fmt.Fprintf(w, "%s %s\n", name, trimFloatMetric(series[""]))
			continue
		}
		for val, v := range series {
			fmt.Fprintf(w, "%s{%s=\"%s\"} %s\n", name, by, escapeLabel(val), trimFloatMetric(v))
		}
	}
}

// escapeLabel escapes a Prometheus label value (backslash, quote, newline).
func escapeLabel(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	r := strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "\n", "\\n")
	return r.Replace(v)
}
