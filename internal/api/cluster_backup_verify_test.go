package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// A cluster backup refuses a shard archive it cannot read, and one from a
// replica that says it is incomplete.
//
// Two defects, one place:
//
//  1. spool sets resp.Complete from X-Simdlogs-Complete, the READ path enforces
//     it, and the backup path -- four lines from where it is parsed -- did not
//     look. A node serving 1 of 2 groups under quarantine, failing its own
//     readiness probe, produced a valid cluster backup at HTTP 200.
//
//  2. spool accepted any 2xx and any body. A 200 with an empty body, a 204, a
//     truncated archive and an HTML error page all produced a well-formed
//     cluster tar whose manifest claimed the rows. ValidateClusterBackup checks
//     the manifest against the entry NAMES and never looks inside them, so
//     nothing read the bytes until a restore did.
//
// This is the worst place in the system for a silent partial: the archive is
// what an operator restores from after losing the original, so the gap surfaces
// exactly when the data it is missing is the only copy.

// backupShard answers /internal/replica/state like a real node and
// /admin/backup with whatever the test wants.
func backupShard(t *testing.T, complete bool, status int, body []byte) *httptest.Server {
	t.Helper()
	inner := realShard(t, nil)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/backup" {
			proxyTo(w, r, inner.URL)
			return
		}
		writeEnvelope(w.Header(), 0, 0, complete, 1, true, "gen-test", "")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(status)
		w.Write(body)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// clusterBackup asks the router for a cluster backup and reports what happened.
// A refusal aborts the connection mid-stream on purpose, so a transport error
// IS the refusal.
func clusterBackup(t *testing.T, router *httptest.Server) (int, int, error) {
	t.Helper()
	resp, err := http.Get(router.URL + "/admin/cluster/backup")
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	b, rerr := io.ReadAll(resp.Body)
	return resp.StatusCode, len(b), rerr
}

func backupRouter(t *testing.T, shards ...string) *httptest.Server {
	t.Helper()
	srv, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	srv.SetBackends(shards)
	srv.SetReplicas(1)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func TestAClusterBackupRefusesAnUnusableShardArchive(t *testing.T) {
	for _, tc := range []struct {
		name     string
		complete bool
		status   int
		body     []byte
	}{
		{"empty body at 200", true, 200, nil},
		{"204 no content", true, 204, nil},
		{"an HTML error page", true, 200, []byte("<html><body>502 Bad Gateway</body></html>")},
		{"a truncated archive", true, 200, []byte("simdlogs-backup\x00truncated")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sh := backupShard(t, tc.complete, tc.status, tc.body)
			ts := backupRouter(t, sh.URL)

			code, n, err := clusterBackup(t, ts)
			// Refused: either a non-2xx, or a 200 whose body is aborted
			// mid-stream so the tar has no footer and `curl -fsS` does not
			// exit 0 on a plausible archive.
			if err != nil {
				return // aborted transfer: the refusal
			}
			if code/100 == 2 && n > 0 {
				t.Errorf("the cluster backup answered %d with %d bytes for a shard whose "+
					"archive is %s -- a well-formed tar whose manifest claims rows nothing "+
					"read", code, n, tc.name)
			}
		})
	}
}

// The good path still works: a gate that refuses everything is not a gate.
func TestAClusterBackupOfAHealthyShardStillSucceeds(t *testing.T) {
	node := realShard(t, nil)
	postLines(t, node.URL, line("2024-01-01T00:00:00Z", "hello"))
	ts := backupRouter(t, node.URL)

	code, n, err := clusterBackup(t, ts)
	if err != nil {
		t.Fatalf("a healthy cluster backup was aborted: %v", err)
	}
	if code/100 != 2 || n == 0 {
		t.Fatalf("a healthy cluster backup answered %d with %d bytes", code, n)
	}
}

// A replica that says it is incomplete is refused even when its archive is
// PERFECTLY VALID.
//
// This case cannot live in the table above, and the reason is the defect it
// replaces: that table paired complete=false with a truncated body, so
// VerifyBackup refused it and the completeness branch never decided anything.
// Reverting `if !resp.Complete` left the whole package green.
//
// Here the shard proxies its REAL /admin/backup body -- an archive VerifyBackup
// accepts -- and only the envelope header says false. Nothing but the
// completeness check can refuse it.
func TestAClusterBackupRefusesACompleteFalseReplicaWithAValidArchive(t *testing.T) {
	inner := realShard(t, nil)
	postLines(t, inner.URL, line("2024-01-01T00:00:00Z", "hello"))

	sh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/backup" {
			proxyTo(w, r, inner.URL)
			return
		}
		// The real archive, with complete=false stamped over the envelope.
		req, _ := http.NewRequest(r.Method, inner.URL+r.URL.RequestURI(), nil)
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.Header().Set(HdrComplete, "false")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(resp.StatusCode)
		w.Write(body)
	}))
	t.Cleanup(sh.Close)

	ts := backupRouter(t, sh.URL)
	code, n, err := clusterBackup(t, ts)
	if err != nil {
		return // aborted transfer: the refusal
	}
	if code/100 == 2 && n > 0 {
		t.Errorf("the cluster backup answered %d with %d bytes from a replica reporting "+
			"%s=false. Its archive is valid, so only the completeness check can refuse "+
			"it -- and a node serving 1 of 2 groups under quarantine, failing its own "+
			"readiness probe, is exactly this case", code, n, HdrComplete)
	}
}
