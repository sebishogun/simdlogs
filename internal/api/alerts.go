package api

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/sebishogun/simdlogs/internal/query"
)

// alertRule fires when a LogsQL count crosses a threshold -- a native alerting
// engine over the logs (VictoriaLogs leans on vmalert for this). It is
// evaluated on a timer and its state is exposed at /alerts.
type alertRule struct {
	name, raw, op string
	threshold     float64
	mu            sync.Mutex
	firing        bool
	value         float64
}

// AddAlertRule registers an alert (op is one of > >= < <= ==), evaluates it once
// now, and re-evaluates every interval (>0) in the background.
func (s *Server) AddAlertRule(name, logsql, op string, threshold float64, interval time.Duration) error {
	if _, err := query.ParseLogsQL(logsql); err != nil {
		return err
	}
	a := &alertRule{name: name, raw: logsql, op: op, threshold: threshold}
	s.amu.Lock()
	s.alerts = append(s.alerts, a)
	s.amu.Unlock()
	a.eval(s)
	// Under the server's background lifecycle: this loop used to be a bare
	// ticker with no stop, running for the life of the process even after
	// the stores it queries had closed.
	s.goBackground(interval, func() { a.eval(s) })
	return nil
}

func (a *alertRule) eval(s *Server) {
	q, err := query.ParseLogsQL(a.raw)
	if err != nil {
		return
	}
	q.From, q.To = 0, int64(1)<<62
	v := float64(query.Count(s.def.store, q))
	fire := crossed(v, a.op, a.threshold)
	a.mu.Lock()
	a.value, a.firing = v, fire
	a.mu.Unlock()
}

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
	}
	return false
}

// alertsHandler reports every alert's current state.
func (s *Server) alertsHandler(w http.ResponseWriter, r *http.Request) {
	type out struct {
		Name      string  `json:"name"`
		Firing    bool    `json:"firing"`
		Value     float64 `json:"value"`
		Op        string  `json:"op"`
		Threshold float64 `json:"threshold"`
	}
	s.amu.Lock()
	rules := append([]*alertRule(nil), s.alerts...)
	s.amu.Unlock()
	res := make([]out, 0, len(rules))
	for _, a := range rules {
		a.mu.Lock()
		res = append(res, out{a.name, a.firing, a.value, a.op, a.threshold})
		a.mu.Unlock()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"alerts": res})
}
