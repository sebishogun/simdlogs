package corpus

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

// RealisticRecord is a production-shaped log line: ~15 fields mixing low-,
// medium-, and high-cardinality columns plus a templated message. Unlike the
// lean 3-field corpus (which is a micro-benchmark for group-pruning and the
// scaling curve), this one resembles real logs -- skewed field distributions,
// repetitive templated text, bursty and slightly out-of-order timestamps --
// so compression, materialization width, and the message-heavy query classes
// (substring, regexp, pattern) are measured on data that compresses like the
// real thing, not on unique hex that is a worst case for everyone.
type RealisticRecord struct {
	Time   time.Time
	Fields []KV
}

// KV is one ordered field.
type KV struct{ Key, Value string }

var (
	rLevels    = []string{"info", "info", "info", "info", "info", "info", "info", "warn", "error", "debug"}
	rServices  = []string{"api", "auth", "billing", "cache", "db", "gateway", "worker", "scheduler", "search", "ingest", "notify", "admin"}
	rRegions   = []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "sa-east-1"}
	rMethods   = []string{"GET", "GET", "GET", "GET", "GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"}
	rStatuses  = []string{"200", "200", "200", "200", "200", "200", "200", "200", "304", "404", "500", "503"}
	rResources = []string{"users", "orders", "sessions", "items", "carts", "payments", "invoices", "events"}
)

// GenRealistic streams n deterministic production-shaped records from seed.
func GenRealistic(seed int64, n int, fn func(RealisticRecord)) {
	rng := rand.New(rand.NewSource(seed))
	hostZipf := rand.NewZipf(rng, 1.2, 1, 1023)   // ~1024 hosts, skewed
	userZipf := rand.NewZipf(rng, 1.1, 1, 199999) // ~200k users, skewed
	podZipf := rand.NewZipf(rng, 1.3, 1, 8191)    // ~8k pods
	t := time.Unix(1_700_000_000, 0).UTC()

	for i := 0; i < n; i++ {
		t = t.Add(time.Duration(1+rng.Intn(800)) * time.Microsecond) // bursty
		et := t
		if rng.Intn(200) == 0 { // ~0.5% arrive slightly out of order
			et = t.Add(-time.Duration(rng.Intn(2000)) * time.Millisecond)
		}
		host := fmt.Sprintf("node-%04d", hostZipf.Uint64())
		user := "u" + strconv.FormatUint(userZipf.Uint64(), 10)
		method := rMethods[rng.Intn(len(rMethods))]
		res := rResources[rng.Intn(len(rResources))]
		path := "/api/v1/" + res + "/" + strconv.Itoa(rng.Intn(100000))
		status := rStatuses[rng.Intn(len(rStatuses))]
		lat := 1 + int(rng.ExpFloat64()*40) // right-skewed latency
		bytes := 64 + rng.Intn(65536)

		fn(RealisticRecord{
			Time: et,
			Fields: []KV{
				{"level", rLevels[rng.Intn(len(rLevels))]},
				{"service", rServices[rng.Intn(len(rServices))]},
				{"host", host},
				{"region", rRegions[rng.Intn(len(rRegions))]},
				{"pod", host + "-" + strconv.FormatUint(podZipf.Uint64(), 16)},
				{"container", res + "-svc"},
				{"method", method},
				{"path", path},
				{"status", status},
				{"user_id", user},
				{"trace_id", traceID(rng)},
				{"span_id", strconv.FormatUint(rng.Uint64()>>32, 16)},
				{"latency_ms", strconv.Itoa(lat)},
				{"bytes", strconv.Itoa(bytes)},
				{"_msg", realMsg(rng, method, path, status, lat, host, user, bytes)},
			},
		})
	}
}

// realMsg builds a templated message: a handful of shapes with variable
// slots, the repetition real log text has and unique hex does not.
func realMsg(rng *rand.Rand, method, path, status string, lat int, host, user string, bytes int) string {
	switch rng.Intn(6) {
	case 0:
		return method + " " + path + " " + status + " " + strconv.Itoa(lat) + "ms"
	case 1:
		return "request handled for " + user + " in " + strconv.Itoa(lat) + "ms"
	case 2:
		return "connection to " + host + " timed out after " + strconv.Itoa(lat) + "ms"
	case 3:
		return "user " + user + " authenticated from " + host
	case 4:
		return "served " + strconv.Itoa(bytes) + " bytes in " + strconv.Itoa(lat) + "ms"
	default:
		return "cache miss for " + path + " on " + host
	}
}
