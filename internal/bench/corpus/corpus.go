// Package corpus generates a realistic, deterministic log corpus: the
// fixed input both engines are measured on. Same seed, same bytes --
// verified by hashing two runs -- so the benchmark contract rests on a
// reproducible corpus rather than a captured one nobody can regenerate.
package corpus

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"
)

// Record is one log line's logical content before serialization.
type Record struct {
	Time    time.Time
	Level   string
	Service string
	Host    string
	TraceID string
	Message string
}

var (
	levels   = []string{"debug", "info", "info", "info", "warn", "error"}
	services = []string{"api", "auth", "billing", "cache", "db", "gateway", "worker", "scheduler"}
	verbs    = []string{"handled", "rejected", "retried", "timed out", "cached", "flushed", "committed"}
	nouns    = []string{"request", "session", "transaction", "connection", "job", "batch", "lease"}
)

// Gen streams n deterministic records from seed to fn, starting at a fixed
// epoch and advancing time realistically (bursty, monotonic).
func Gen(seed int64, n int, fn func(Record)) {
	rng := rand.New(rand.NewSource(seed))
	t := time.Unix(1_700_000_000, 0).UTC()
	hosts := make([]string, 32)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("node-%02d", i)
	}
	for i := 0; i < n; i++ {
		t = t.Add(time.Duration(rng.Intn(2000)) * time.Microsecond)
		svc := services[rng.Intn(len(services))]
		rec := Record{
			Time:    t,
			Level:   levels[rng.Intn(len(levels))],
			Service: svc,
			Host:    hosts[rng.Intn(len(hosts))],
			TraceID: traceID(rng),
			Message: fmt.Sprintf("%s %s id=%d in %dms",
				verbs[rng.Intn(len(verbs))], nouns[rng.Intn(len(nouns))],
				rng.Intn(1_000_000), rng.Intn(5000)),
		}
		fn(rec)
	}
}

func traceID(rng *rand.Rand) string {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], rng.Uint64())
	const hex = "0123456789abcdef"
	out := make([]byte, 16)
	for i, x := range b {
		out[2*i] = hex[x>>4]
		out[2*i+1] = hex[x&0xf]
	}
	return string(out)
}
