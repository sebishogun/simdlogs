package api

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

// A cluster backup captures the topology, and refuses to capture a short shard.

// clusterOf builds a router over n shards of r replicas each.
func clusterOf(t *testing.T, n, r int) (*Server, *httptest.Server, [][]*httptest.Server) {
	t.Helper()
	nodes := make([][]*httptest.Server, n)
	var urls []string
	for i := 0; i < n; i++ {
		nodes[i] = make([]*httptest.Server, r)
		for j := 0; j < r; j++ {
			nodes[i][j] = realShard(t, nil)
			urls = append(urls, nodes[i][j].URL)
		}
	}
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends(urls)
	srv.SetReplicas(r)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts, nodes
}

func takeClusterBackup(t *testing.T, router *httptest.Server) (int, []byte) {
	t.Helper()
	resp, err := http.Post(router.URL+"/admin/cluster/backup", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// readArchive returns the manifest and the entry names, in order.
func readArchive(t *testing.T, blob []byte) (ClusterManifest, []string) {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(blob))
	var man ClusterManifest
	var names []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading the archive: %v (entries so far: %v)", err, names)
		}
		names = append(names, h.Name)
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == clusterManifestName {
			if err := json.Unmarshal(body, &man); err != nil {
				t.Fatalf("the manifest does not parse: %v", err)
			}
		}
	}
	return man, names
}

func TestAClusterBackupRecordsItsTopology(t *testing.T) {
	_, router, nodes := clusterOf(t, 2, 2)
	// One write per shard, sent to both replicas so each shard is complete.
	for i := range nodes {
		for _, n := range nodes[i] {
			postLines(t, n.URL, line("2026-06-01T12:00:0"+string(rune('0'+i))+"Z", "shard-data"))
		}
	}

	code, blob := takeClusterBackup(t, router)
	if code != 200 {
		t.Fatalf("%d: %.300s", code, blob)
	}
	man, names := readArchive(t, blob)

	if names[0] != clusterManifestName {
		t.Fatalf("the first entry is %q; a reader must be able to validate before "+
			"streaming the shard data", names[0])
	}
	if len(man.Shards) != 2 {
		t.Fatalf("the manifest records %d shards, want 2", len(man.Shards))
	}
	if man.Format != ClusterBackupFormat || man.Protocol != ProtocolVersion {
		t.Errorf("format %d protocol %d", man.Format, man.Protocol)
	}
	for i, sb := range man.Shards {
		if sb.Shard != i {
			t.Errorf("entry %d is shard %d", i, sb.Shard)
		}
		if sb.Replicas != 2 {
			t.Errorf("shard %d records %d replicas, want 2", i, sb.Replicas)
		}
		if sb.SourceURL == "" {
			t.Errorf("shard %d does not record which replica it came from", i)
		}
		if sb.Rows != 1 || sb.Groups != 1 {
			t.Errorf("shard %d records %d rows in %d groups, want 1 and 1",
				i, sb.Rows, sb.Groups)
		}
		if !hasEntry(names, sb.Archive) {
			t.Errorf("the manifest names %q but the archive has %v", sb.Archive, names)
		}
	}
}

// The property a directory of per-node backups cannot have: a shard with no
// complete replica is refused, not captured short.
func hasEntry(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}

func TestAShardWithNoCompleteReplicaRefusesTheBackup(t *testing.T) {
	_, router, nodes := clusterOf(t, 1, 2)
	// Each replica holds something the other does not, so neither is complete.
	postLines(t, nodes[0][0].URL, line("2026-06-01T12:00:00Z", "only-on-0"))
	postLines(t, nodes[0][1].URL, line("2026-06-01T12:00:01Z", "only-on-1"))

	code, body := takeClusterBackup(t, router)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("%d, want 503: a backup taken now would be short without saying so: %.300s",
			code, body)
	}
	if !strings.Contains(string(body), "repair") {
		t.Errorf("the refusal does not say what to do about it: %.300s", body)
	}

	// After repair, one replica holds the whole shard and the backup succeeds.
	rep := runRepair(t, router)
	if rep.Copied != 2 {
		t.Fatalf("repair copied %d, want 2: %+v", rep.Copied, rep)
	}
	code, blob := takeClusterBackup(t, router)
	if code != 200 {
		t.Fatalf("after repair: %d %.300s", code, blob)
	}
	man, _ := readArchive(t, blob)
	if man.Shards[0].Rows != 2 {
		t.Fatalf("the backup captured %d rows, want both", man.Shards[0].Rows)
	}
}

// A backup is refused on a node that is not a router.
func TestAClusterBackupOnAStorageNodeIsRefused(t *testing.T) {
	node := realShard(t, nil)
	code, body := takeClusterBackup(t, node)
	if code != http.StatusNotImplemented {
		t.Fatalf("%d, want 501: %.200s", code, body)
	}
	if !strings.Contains(string(body), "/admin/backup") {
		t.Errorf("the refusal does not name the single-node form: %.200s", body)
	}
}

// Restore validation refuses the mismatches that silently misplace data.
func TestRestoreValidationRefusesAMismatchedArchive(t *testing.T) {
	good := ClusterManifest{
		Format: ClusterBackupFormat, Protocol: ProtocolVersion,
		Shards: []ShardBackup{
			{Shard: 0, Archive: "shard-0.tar"},
			{Shard: 1, Archive: "shard-1.tar"},
		},
	}
	if err := ValidateClusterBackup(good, 2); err != nil {
		t.Fatalf("a matching archive was refused: %v", err)
	}
	if err := ValidateClusterBackup(good, 0); err != nil {
		t.Fatalf("an unspecified topology was refused: %v", err)
	}

	for _, tc := range []struct {
		name   string
		man    ClusterManifest
		shards int
		want   string
	}{
		{"a newer format", ClusterManifest{Format: ClusterBackupFormat + 1,
			Protocol: ProtocolVersion, Shards: good.Shards}, 2, "format"},
		{"another protocol", ClusterManifest{Format: ClusterBackupFormat,
			Protocol: ProtocolVersion + 1, Shards: good.Shards}, 2, "protocol"},
		{"no shards", ClusterManifest{Format: ClusterBackupFormat,
			Protocol: ProtocolVersion}, 0, "no shards"},
		{"a different shard count", good, 3, "shards"},
		{"the same shard twice", ClusterManifest{
			Format: ClusterBackupFormat, Protocol: ProtocolVersion,
			Shards: []ShardBackup{
				{Shard: 0, Archive: "a.tar"}, {Shard: 0, Archive: "b.tar"},
			}}, 2, "twice"},
		{"a shard naming no archive", ClusterManifest{
			Format: ClusterBackupFormat, Protocol: ProtocolVersion,
			Shards: []ShardBackup{{Shard: 0}}}, 1, "names no archive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateClusterBackup(tc.man, tc.shards)
			if err == nil {
				t.Fatal("accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not say why (%q): %v", tc.want, err)
			}
		})
	}
}

// The spread between shard watermarks is reported, since the archives are taken
// at different moments and only the operator knows whether that matters.
func TestTheManifestReportsHowFarApartTheShardsWere(t *testing.T) {
	m := ClusterManifest{Shards: []ShardBackup{
		{Shard: 0, HighWatermark: 1000},
		{Shard: 1, HighWatermark: 4000},
		{Shard: 2, HighWatermark: 2000},
	}}
	if got := m.Spread(); got != 3000 {
		t.Fatalf("spread %d, want 3000", got)
	}
	if got := (ClusterManifest{}).Spread(); got != 0 {
		t.Fatalf("an empty manifest has spread %d", got)
	}
}

// The spread the runbook tells an operator to read is in the archive the SERVER
// produces.
//
// docs/runbooks/backup-restore.md computes cluster RPO from the skew between
// shard archives. It used to name `ClusterManifest.Spread()` -- a method on an
// internal/ type, reachable from no endpoint and no command -- so the runbook
// asked for a number an operator had no way to obtain, and the method itself
// had no production caller at all.
//
// Driven through /admin/cluster/backup and read out of the tar. The first
// version of this test did `man.SpreadNanos = man.Spread()` ITSELF and then
// marshalled, so commenting out the production assignment left the whole
// internal/api package green -- while the test's own doc claimed it "has to
// fail if the field stops being marshalled". It asserted on a manifest it had
// populated. TestTheManifestReportsHowFarApartTheShardsWere above pins the
// arithmetic; this one pins that the server calls it and ships the result.
func TestTheClusterBackupArchiveCarriesTheSpread(t *testing.T) {
	// Three shards whose newest row is a day apart, so the spread is a number
	// and not zero -- an expected value of zero cannot tell a computed answer
	// from an absent field.
	var urls []string
	for i := 0; i < 3; i++ {
		sh := realShard(t, []string{fmt.Sprintf(
			`{"_time":"2024-01-0%dT00:00:00Z","_msg":"m","shard":"%d"}`, i+1, i)})
		urls = append(urls, sh.URL)
	}
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends(urls)
	srv.SetReplicas(1)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	code, blob := takeClusterBackup(t, ts)
	if code != 200 {
		t.Fatalf("the backup answered %d: %.300s", code, blob)
	}
	man, names := readArchive(t, blob)
	if len(man.Shards) != 3 {
		t.Fatalf("the manifest names %d shards, want 3 (entries %v)", len(man.Shards), names)
	}

	// From the RAW manifest bytes: a field the server never set decodes to 0
	// through the struct and is indistinguishable from a computed zero.
	var asMap map[string]any
	if err := json.Unmarshal(manifestBytes(t, blob), &asMap); err != nil {
		t.Fatalf("the manifest does not parse: %v", err)
	}
	v, ok := asMap["spreadNanos"]
	if !ok {
		keys := make([]string, 0, len(asMap))
		for k := range asMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		t.Fatalf("cluster.json has no spreadNanos, and the runbook tells an operator "+
			"to read it from this file. Keys: %v", keys)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("spreadNanos is %T, not a number", v)
	}

	lo, hi := man.Shards[0].HighWatermark, man.Shards[0].HighWatermark
	for _, sb := range man.Shards[1:] {
		if sb.HighWatermark < lo {
			lo = sb.HighWatermark
		}
		if sb.HighWatermark > hi {
			hi = sb.HighWatermark
		}
	}
	if hi == lo {
		t.Fatalf("every shard reported watermark %d, so this run cannot tell a "+
			"computed spread from an unset field", hi)
	}
	if want := hi - lo; int64(f) != want {
		t.Errorf("cluster.json says spreadNanos=%d; the shard watermarks in the same "+
			"file span %d", int64(f), want)
	}
}

// manifestBytes returns cluster.json's raw bytes from the archive.
func manifestBytes(t *testing.T, blob []byte) []byte {
	t.Helper()
	tr := tar.NewReader(bytes.NewReader(blob))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Name == clusterManifestName {
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			return b
		}
	}
	t.Fatalf("the archive has no %s", clusterManifestName)
	return nil
}
