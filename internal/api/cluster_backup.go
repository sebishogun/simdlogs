package api

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	obs "github.com/sebishogun/simdlogs/internal/observability"

	"github.com/sebishogun/simdlogs/internal/storage"
)

// A backup of a cluster, rather than a backup of each machine.
//
// # Why per-node backups do not compose
//
// Backing up every storage node and keeping the archives in a directory looks
// like a cluster backup and is not one:
//
//   - Nothing records the TOPOLOGY. Restoring three archives into a two-shard
//     cluster silently drops a third of the data, or duplicates it, depending
//     on how the operator maps them -- and no file in the set says which shard
//     an archive came from.
//   - Replicas of one shard are not interchangeable. Take the archive from a
//     replica that had missed writes and the restored cluster is missing them,
//     with nothing anywhere to say so.
//   - The archives are taken at different moments, so the restored cluster is
//     a state the cluster was never in. For an append-only log store that is
//     survivable, and it still has to be RECORDED: a per-shard high watermark
//     is what lets an operator see how far apart the pieces are.
//
// # What this writes
//
// One tar holding a cluster manifest and one archive per shard, taken from a
// replica that holds the whole shard.
//
// "Complete" is checked, not assumed: the replicas of a shard are asked for
// their inventories first, and the chosen one must hold every group any of them
// holds. If no replica is complete, the backup FAILS. A cluster backup that
// silently captured the shortest replica is worse than no backup, because it
// looks like one.

// ClusterBackupFormat is the archive layout version. Bumped when the layout or
// the manifest's meaning changes, so a restore refuses what it cannot read
// rather than reading it wrongly.
const ClusterBackupFormat = 1

// clusterManifestName is the manifest's path inside the archive. First entry,
// so a reader can validate before streaming gigabytes of shard data.
const clusterManifestName = "cluster.json"

// ClusterManifest describes a cluster backup.
type ClusterManifest struct {
	Format      int   `json:"format"`
	CreatedUnix int64 `json:"createdUnix"`
	// Protocol is the internal protocol version of the router that took it,
	// which is what the shard archives' shapes are tied to.
	Protocol int `json:"protocol"`
	// Shards is one entry per shard, in shard order.
	Shards []ShardBackup `json:"shards"`
	// SpreadNanos is Spread() at the moment the manifest was written: how far
	// apart the shard archives were taken.
	//
	// It is in the manifest because docs/runbooks/backup-restore.md tells an
	// operator to compute their cluster RPO from `ClusterManifest.Spread()` --
	// a method in an internal/ package, reachable from no endpoint and no
	// command, so the runbook named a number nobody could obtain. Recording it
	// is the whole fix: the archive now carries it and `jq .spreadNanos
	// cluster.json` answers the question the runbook asks.
	SpreadNanos int64 `json:"spreadNanos"`
}

// ShardBackup is one shard's part of a cluster backup.
type ShardBackup struct {
	Shard int `json:"shard"`
	// Archive is the entry name inside the tar.
	Archive string `json:"archive"`
	// SourceURL is the replica the archive came from, recorded so a restore can
	// be traced back and an operator can tell two backups apart.
	SourceURL string `json:"sourceURL"`
	// Replicas is how many replicas this shard had when the backup was taken.
	// A restore into a different topology is refused on this.
	Replicas int `json:"replicas"`
	// ReplicasConsulted is how many of them answered when the source was
	// chosen. A backup is refused unless it equals Replicas, so this is a
	// record rather than a condition -- but it is the field that makes the
	// archive say which it was instead of leaving a reader to assume.
	ReplicasConsulted int `json:"replicasConsulted"`
	// HighWatermark is the newest timestamp this shard's data covered.
	// Recorded per shard because the archives are taken at different moments,
	// and the spread between them is the thing an operator needs to see.
	HighWatermark int64 `json:"highWatermark"`
	// Groups and Rows are what the source held, so a restore can check that
	// what it unpacked is what was captured.
	Groups int `json:"groups"`
	Rows   int `json:"rows"`
	// Receipts is the source's write-id count at capture.
	Receipts int `json:"receipts"`
}

// clusterBackup streams a coordinated backup of every shard.
func (s *Server) clusterBackup(w http.ResponseWriter, r *http.Request) {
	if !s.clusterTenant(w, r) {
		return
	}
	if len(s.backendList()) == 0 {
		s.writeErr(w, r, adminSpec(), http.StatusNotImplemented,
			"simdlogs: a cluster backup captures every shard, and this node has no "+
				"backends configured, so it is not a router. /admin/backup takes this "+
				"node's own store")
		return
	}
	shards := s.shards()
	obs.Audit(r.Context(), obs.EventClusterBackup, subjectOf(r), obs.OutcomeOK,
		obs.FieldTenant, tenantKeyOf(r), obs.FieldRoute, r.URL.Path,
		"shards", len(shards))

	// Choose the sources BEFORE writing a byte. A backup that streamed the
	// first shard and then discovered the second had no complete replica would
	// have to fail mid-tar, leaving a truncated archive that looks like a
	// finished one to anything that does not read the manifest.
	man := ClusterManifest{
		Format:      ClusterBackupFormat,
		CreatedUnix: time.Now().Unix(),
		Protocol:    ProtocolVersion,
	}
	sources := make([]string, len(shards))
	for i, replicas := range shards {
		states := make([]ReplicaState, 0, len(replicas))
		for j, u := range replicas {
			states = append(states, s.askReplicaState(r, i, j, u))
		}
		// An UNREACHABLE replica is refused, not skipped.
		//
		// completeReplica builds its union from reachable replicas only, so a
		// replica that did not answer can never make the chosen source look
		// short -- and the only check was `reachable == 0`. Measured: one shard,
		// two replicas, one row written only to replica 2. With both up the
		// backup held 3 groups / 3 rows; with replica 2 down it held 2 groups /
		// 2 rows, HTTP 200, a valid tar that passes ValidateClusterBackup, and
		// nothing anywhere saying a replica was never asked. repairCluster marks
		// a shard incomplete on exactly this condition; the backup path did not.
		var unreachable []string
		for j, st := range states {
			if st.Err != "" {
				unreachable = append(unreachable, fmt.Sprintf("%d(%s)", j, st.Err))
			}
		}
		if len(unreachable) > 0 {
			s.writeErr(w, r, adminSpec(), http.StatusServiceUnavailable, fmt.Sprintf(
				"simdlogs: shard %d has %d of %d replicas unreachable (%s). What "+
					"those replicas hold cannot be compared against what the "+
					"reachable ones hold, so a backup taken now could be short with "+
					"no way to tell from the archive. Bring them back, or run "+
					"/admin/cluster/repair once they return, and try again",
				i, len(unreachable), len(replicas), strings.Join(unreachable, ",")))
			return
		}
		src, why := completeReplica(states)
		if src < 0 {
			s.writeErr(w, r, adminSpec(), http.StatusServiceUnavailable, fmt.Sprintf(
				"simdlogs: shard %d has no replica holding the whole shard, so a "+
					"backup taken now would be short without saying so: %s. Run "+
					"/admin/cluster/repair and try again", i, why))
			return
		}
		st := states[src]
		sources[i] = st.URL
		man.Shards = append(man.Shards, ShardBackup{
			Shard:     i,
			Archive:   fmt.Sprintf("shard-%d.tar", i),
			SourceURL: st.URL,
			Replicas:  len(replicas),
			// Replicas alone could not distinguish "two replicas, both asked"
			// from "two replicas, one asked", which is the difference between a
			// complete archive and a short one.
			ReplicasConsulted: len(states) - len(unreachable),
			HighWatermark:     st.HighWatermark,
			Groups:            len(st.Groups),
			Rows:              st.rows(),
			Receipts:          st.Receipts,
		})
	}

	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", `attachment; filename="simdlogs-cluster-backup.tar"`)
	// NOT `defer tw.Close()`.
	//
	// tar.Writer.Close writes the archive's two zero blocks -- its footer. A
	// deferred Close therefore runs on the mid-stream failure path below and
	// finishes the archive, so a client that lost half its shards receives a
	// WELL-FORMED tar containing the rest, with HTTP 200 and a clean `curl
	// -fsS`. The comment on that path used to claim the opposite ("abandoned
	// WITHOUT its closing blocks"), which is a guarantee the code did not make.
	//
	// Close is called explicitly on the success path only; every failure after
	// the first byte aborts the connection instead, which is what the
	// single-node handler already does.
	tw := tar.NewWriter(w)

	man.SpreadNanos = man.Spread()

	blob, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		s.writeErr(w, r, adminSpec(), http.StatusInternalServerError, err.Error())
		return
	}
	if err := writeTarFile(tw, clusterManifestName, blob); err != nil {
		obs.L().Error("cluster backup failed writing its manifest",
			obs.FieldEvent, "cluster.backup_failed", "err", err)
		// Bytes may already be out; abort rather than finish the archive.
		panic(http.ErrAbortHandler)
	}

	for i, sb := range man.Shards {
		// One replica per shard, the one chosen above. Its own /admin/backup is
		// snapshot-consistent and self-validating already (task 5.1); this
		// wraps those archives with the topology they were missing.
		// Spooled to a temp file, not buffered.
		//
		// A shard's backup is as large as the shard, so `do`'s 256 MiB
		// in-memory ceiling discarded every real one as malformed -- and the
		// cluster backup then captured no shard data at all. The file also
		// gives the tar header the size it needs before the body is written.
		f, size, resp, cleanup := s.peers.spool(r, i, 0, sources[i], "/admin/backup")
		if !resp.OK() {
			cleanup()
			// Mid-stream failure, and the manifest is already on the wire.
			//
			// The connection is ABORTED, so the client sees a truncated
			// transfer. Returning here let the deferred Close write the tar
			// footer, and the caller got a well-formed archive missing a shard
			// with HTTP 200 -- an operator's `curl -fsS` exits 0 and the gap is
			// found at restore time, or later.
			obs.L().Error("cluster backup failed mid-stream",
				obs.FieldEvent, "cluster.backup_failed", "shard", i,
				"source", sources[i], "class", string(resp.Class), "err", resp.Err)
			panic(http.ErrAbortHandler)
		}
		// The completeness header, which this path read and then ignored.
		//
		// cluster_client.go sets resp.Complete from X-Simdlogs-Complete on the
		// spooled response, the READ path enforces it, and four lines from where
		// it is parsed the backup path did not look. A node serving 1 of 2
		// groups under quarantine -- failing its own readiness probe -- produced
		// a valid cluster backup at HTTP 200. That is the worst place in the
		// system for a silent partial: the archive is what an operator restores
		// from after losing the original, so the gap surfaces when the data it
		// is missing is the only copy.
		if !resp.Complete {
			cleanup()
			obs.L().Error("cluster backup refused: source replica is not complete",
				obs.FieldEvent, "cluster.backup_failed", "shard", i,
				"source", sources[i],
				"reason", "peer did not report "+HdrComplete+"=true")
			panic(http.ErrAbortHandler)
		}
		// VERIFY the shard archive before it goes into the cluster tar.
		//
		// spool accepted any 2xx and any body: a 200 with an empty body, a 204,
		// a truncated archive and an HTML error page all produced a well-formed
		// cluster tar whose manifest claimed the rows. ValidateClusterBackup
		// checks the manifest against the entry NAMES and does not look inside
		// them, so nothing anywhere read the bytes -- until a restore did.
		//
		// storage.VerifyBackup is the streaming check the single-node backup
		// already ships and had no caller here. It costs one extra read of the
		// spooled file, which is local disk, against an archive that is
		// otherwise unchecked until the day it is needed.
		if _, err := storage.VerifyBackup(f); err != nil {
			cleanup()
			obs.L().Error("cluster backup refused: shard archive did not verify",
				obs.FieldEvent, "cluster.backup_failed", "shard", i,
				"source", sources[i], "bytes", size, "err", err)
			panic(http.ErrAbortHandler)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			cleanup()
			obs.L().Error("cluster backup failed rewinding a verified shard archive",
				obs.FieldEvent, "cluster.backup_failed", "shard", i, "err", err)
			panic(http.ErrAbortHandler)
		}
		err := streamTarFile(tw, sb.Archive, f, size)
		cleanup()
		if err != nil {
			obs.L().Error("cluster backup failed writing a shard",
				obs.FieldEvent, "cluster.backup_failed", "shard", i, "err", err)
			panic(http.ErrAbortHandler)
		}
	}
	// The footer, on the success path only. Its absence is what makes a failed
	// backup unreadable rather than plausibly complete.
	if err := tw.Close(); err != nil {
		obs.L().Error("cluster backup failed closing the archive",
			obs.FieldEvent, "cluster.backup_failed", "err", err)
		panic(http.ErrAbortHandler)
	}
	obs.L().Info("cluster backup complete",
		obs.FieldEvent, "cluster.backup", "shards", len(man.Shards))
}

// completeReplica picks a replica holding every group the shard's reachable
// replicas hold, and explains a refusal.
//
// Ties break on the lowest index, so two runs against an in-step shard choose
// the same source and produce comparable archives.
func completeReplica(states []ReplicaState) (int, string) {
	union := map[string]bool{}
	reachable := 0
	for _, st := range states {
		if st.Err != "" {
			continue
		}
		reachable++
		for _, g := range st.Groups {
			union[g.Digest] = true
		}
	}
	if reachable == 0 {
		return -1, "no replica answered"
	}
	best, bestHave := -1, -1
	for i, st := range states {
		if st.Err != "" {
			continue
		}
		have := map[string]bool{}
		for _, g := range st.Groups {
			have[g.Digest] = true
		}
		missing := 0
		for d := range union {
			if !have[d] {
				missing++
			}
		}
		if missing == 0 {
			return i, ""
		}
		if n := len(union) - missing; n > bestHave {
			best, bestHave = i, n
		}
	}
	return -1, fmt.Sprintf(
		"the shard's replicas hold %d groups between them and the best single "+
			"replica (index %d) holds %d", len(union), best, bestHave)
}

// streamTarFile writes one entry from a reader of known length.
//
// The length is why the source is spooled rather than piped: a tar entry
// declares its size in the header, before any of its bytes, so a body of
// unknown length cannot be written into one without buffering it somewhere.
// A file is that somewhere, and it bounds memory at one copy buffer whatever
// the shard's size.
func streamTarFile(tw *tar.Writer, name string, src io.Reader, size int64) error {
	if err := tw.WriteHeader(&tar.Header{
		Name: name, Mode: 0o600, Size: size,
		ModTime: time.Unix(0, 0), Format: tar.FormatPAX,
	}); err != nil {
		return err
	}
	n, err := io.Copy(tw, src)
	if err != nil {
		return err
	}
	if n != size {
		// A short copy would leave the tar declaring more than it carries,
		// which is an archive that parses and is wrong.
		return fmt.Errorf("shard archive %s: copied %d bytes of a declared %d",
			name, n, size)
	}
	return nil
}

// writeTarFile writes one entry.
func writeTarFile(tw *tar.Writer, name string, body []byte) error {
	if err := tw.WriteHeader(&tar.Header{
		Name: name, Mode: 0o600, Size: int64(len(body)),
		// A fixed time, not time.Now(): two backups of identical data should
		// differ only where the data differs, and per-entry timestamps make
		// every archive unique for no reason. The capture time is in the
		// manifest, where a reader can see it.
		ModTime: time.Unix(0, 0), Format: tar.FormatPAX,
	}); err != nil {
		return err
	}
	_, err := tw.Write(body)
	return err
}

// ValidateClusterBackup reads an archive's manifest and checks it against the
// topology it is about to be restored into.
//
// Called BEFORE anything is unpacked. A restore that discovers the mismatch
// halfway has already written some of it.
func ValidateClusterBackup(man ClusterManifest, intoShards int) error {
	if man.Format != ClusterBackupFormat {
		return fmt.Errorf(
			"simdlogs: this archive is cluster backup format %d and this build reads %d",
			man.Format, ClusterBackupFormat)
	}
	if man.Protocol != ProtocolVersion {
		return fmt.Errorf(
			"simdlogs: this archive was taken by a router speaking protocol %d and "+
				"this build speaks %d", man.Protocol, ProtocolVersion)
	}
	if len(man.Shards) == 0 {
		return fmt.Errorf("simdlogs: this archive records no shards")
	}
	if intoShards > 0 && len(man.Shards) != intoShards {
		// The failure a per-node backup set cannot even detect: the data is
		// sharded by a function of the shard COUNT, so restoring an N-shard
		// backup into an M-shard cluster puts rows where no query looks for
		// them.
		return fmt.Errorf(
			"simdlogs: this archive holds %d shards and the target cluster has %d. "+
				"Restoring it would place rows where no query looks for them",
			len(man.Shards), intoShards)
	}
	seen := map[int]bool{}
	for _, sb := range man.Shards {
		if sb.Shard < 0 || sb.Shard >= len(man.Shards) {
			return fmt.Errorf("simdlogs: shard index %d is outside the archive's %d shards",
				sb.Shard, len(man.Shards))
		}
		if seen[sb.Shard] {
			// Two archives claiming the same shard: restoring both would double
			// that shard's rows and leave another shard with none.
			return fmt.Errorf("simdlogs: shard %d appears twice in this archive", sb.Shard)
		}
		seen[sb.Shard] = true
		if sb.Archive == "" {
			return fmt.Errorf("simdlogs: shard %d names no archive", sb.Shard)
		}
	}
	return nil
}

// Spread is how far apart the shard archives were taken, in nanoseconds
// between the earliest and latest high watermark.
//
// Reported rather than enforced. There is no bound that is right for every
// deployment, and a threshold this code invented would either refuse good
// backups or bless bad ones. An operator comparing it against their own ingest
// rate can tell whether the spread matters.
func (m ClusterManifest) Spread() int64 {
	if len(m.Shards) == 0 {
		return 0
	}
	lo, hi := m.Shards[0].HighWatermark, m.Shards[0].HighWatermark
	for _, sb := range m.Shards[1:] {
		if sb.HighWatermark < lo {
			lo = sb.HighWatermark
		}
		if sb.HighWatermark > hi {
			hi = sb.HighWatermark
		}
	}
	return hi - lo
}
