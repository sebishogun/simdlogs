# Deployment

## The binary

Static, no cgo, no runtime dependencies. One process, one data directory.

    simdlogs -storage /var/lib/simdlogs -addr :9428

`simdlogs -version` prints the version, the commit, and the Go and platform it
was built for. A binary built from a working tree says `dev` and `unknown`
rather than a plausible-looking version — an operator reading a support ticket
needs to be able to tell a release from someone's laptop build.

## Container

    docker build --build-arg VERSION=v1.0.0 --build-arg COMMIT=$(git rev-parse --short HEAD) -t simdlogs .
    docker run -p 9428:9428 -v simdlogs-data:/data simdlogs

The image is `scratch` plus one static binary and the CA bundle. No shell, no
package manager, no libc — every one of those is attack surface in an image
whose only job is to run one process, and their absence is what makes the
image's contents equal to its provenance.

It runs as uid `65532`, by number rather than by name, because a scratch image
has no `/etc/passwd` and a numeric id is what a `runAsNonRoot` check reads
anyway.

### Kubernetes

    securityContext:
      runAsNonRoot: true
      runAsUser: 65532
      readOnlyRootFilesystem: true      # nothing is written outside /data
      allowPrivilegeEscalation: false
      capabilities: {drop: [ALL]}
    volumeMounts:
      - {name: data, mountPath: /data}

`readOnlyRootFilesystem: true` works because the process writes only under its
storage directory. If a future change writes elsewhere, this is the setting
that catches it — at startup, loudly, rather than in an audit months later.

**The data directory must be a real volume.** `/data` is declared `VOLUME` so a
misconfiguration is visible, but a container that stores a log store in its
writable layer loses it on every redeploy, and a system that silently loses
logs on upgrade is worse than one that refuses to start.

## Sizing

| | Driven by |
|---|---|
| memory | the working set of a query, not the store size — groups are mapped and paged in |
| disk | ingest rate × retention, plus the index: 16-18% for the per-column index and 25-27% for postings, measured (`docs/wrong.md`). 6% is the bloom section alone |
| CPU | query concurrency; ingest is cheap per row and parallel across shards |

Address space matters more than resident memory: every group in the store is
mapped. A 64-bit build is required; `docs/verification.md` says why 32-bit is not
offered.

## Cluster

A select-router holds no data. Give it the backend list and no storage of
consequence:

    simdlogs -addr :9428 -storage /var/lib/simdlogs-router \
      -select-backends http://node1:9428,http://node2:9428,http://node3:9428 \
      -replicas 1

The flag is `-select-backends`, and the URLs carry their scheme. Written
`-backends node1:9428` the server exits with `flag provided but not defined`.

Storage nodes are ordinary single-node deployments. Shard membership is
**position in the backend list**, so the list must be identical on every router
and a node's position must not change: a replacement in the wrong position
joins the wrong shard.

Rolling upgrades go storage nodes first, routers last — see
`docs/compatibility.md`.

## Operational endpoints

| Path | For |
|---|---|
| `/-/ready` | load-balancer readiness; red on disk pressure, degraded storage, shutdown |
| `/health`, `/-/healthy` | liveness only; green whenever the process is alive, including while draining |
| `/metrics` | Prometheus; the metric names are a contract |
| `/flags` | this process's configuration, as parsed |

Liveness and readiness are deliberately different. A full disk fails readiness
and not liveness: failing liveness would kill the process, which would restart
onto the same full disk and lose the rows a graceful drain flushes.

## Backups

See `docs/runbooks/backup-restore.md`. The short version: `/admin/backup` per
tenant, `/admin/cluster/backup` for a cluster, and validate with
`simdlogs restore -dry-run` before you need it rather than after.
