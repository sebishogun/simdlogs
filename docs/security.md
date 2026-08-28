# Security

What this server defends against, what it does not, and where each claim is
tested. A claim with no test beside it is a claim nobody has checked.

## Threat model

simdlogs stores logs. Logs carry credentials, session identifiers, internal
hostnames and personal data, so the asset is the stored data and the primary
harm is disclosure across a tenant boundary. Ingest endpoints exist to accept
whatever an agent sends, so every parser reads attacker-controlled bytes by
design.

Assumed: the operator controls the network between nodes and the auth
configuration file. Not assumed: that clients are well-behaved, that agents send
valid payloads, or that a peer on the internal protocol is honest.

## Authentication and authorization

Bearer tokens are stored as SHA-256 hashes and looked up by hash. An attacker
attacking the comparison would have to steer the digest, which needs the token
they are trying to find — so no constant-time primitive is load-bearing here,
and none is claimed.

| Property | Test |
|---|---|
| A rejected credential is answered identically however close the guess | `TestDrillARejectedCredentialRevealsNothing` |
| No credential answers 401 | same — it asserts the STATUS; it does not compare the body against a rejected guess, and both hold in code |
| A valid token lacking a role gets 403, distinguishable from a bad token | same — it asserts the status and that the two answers differ; nothing inspects the body for the role name, though `auth.go` does emit it |
| A role cannot reach another role's routes, including the internal ones | `TestDrillARoleCannotReachAnotherRolesRoutes` |

Surrounding whitespace in `Authorization: Bearer <token>` is trimmed, for
drop-in compatibility with clients that add a stray space. The trim is by
position, not by content, so it cannot branch on how close a guess was.

## Tenant isolation

A credential is scoped to tenants. A request may name a tenant in `AccountID` /
`ProjectID`, and naming one the credential does not hold is refused rather than
honoured — `TestDrillATenantCannotReadAnother` checks every read route, both
the implicit case and the explicit claim.

In router mode the tenant headers are stamped by the router's own resolver and
forwarded to the shards. The client's `Authorization` is **not** forwarded: the
router authenticates to peers as itself, and a client credential travelling
further than the node it was presented to is how one node's compromise becomes
the cluster's. The forwarded set is an explicit list, not a copy of the header
set.

**The router presents no credential of its own to peers.** It forwards the
resolved tenant headers and the protocol headers, and nothing else; there is no
`Authorization` and no client certificate (`newClusterClient(nil)`). So
`PeerUnauthorized`'s message names a credential that is never sent, and a
deployment that needs authenticated peer traffic has to provide it at the
network layer. That is a gap, stated rather than implied by the absence of a
row in this table.

## The internal cluster protocol

`X-Simdlogs-Internal` selects the envelope response shape. It grants nothing:
the internal replica endpoints are admin-authorized, and a client setting the
header without an admin credential gets 401/403
(`TestDrillAClientCannotForgeTheInternalProtocolHeader`). Completeness and high
watermark are computed by the answering node, never read from the request — a
forged inbound watermark is not echoed.

A peer speaking an unknown protocol version is refused, not merged. A peer's
4xx is a typed failure, not a body to merge (see `docs/wrong.md` entry 40 for
what that cost before it was true).

## Resource limits

| Limit | Test |
|---|---|
| Request body size | `TestGuardRejectsOversizedBody` |
| Decompression ratio | `TestGuardRejectsDecompressionBomb` |
| Syslog frame size | `TestSyslogOversizedFrameIsRejected` |
| Syslog datagram size | `TestSyslogUDPOversizedDatagramIsDropped` asserts the KERNEL refuses one over 65507 and that one at the ceiling is stored. The server-side drop branch is unreachable by construction and is untested |
| Peer response size | `TestAnOversizedPeerResponseIsDiscarded` — discarded, not truncated. A shard BACKUP bypasses this ceiling deliberately by spooling to a temp file, since a backup is as large as the shard |
| Concurrent queries, per tenant | `TestPerTenantAdmissionRefusesOnlyTheTenantAtItsLimit` |
| Tenant count | `TestTenantCountIsBounded` |
| Repair transfer | 64 groups and a 1 GiB accounting budget cluster-wide per pass; 2 GiB hard ceiling if a peer understates the final group; 1 GiB hard ceiling per group. Exact fetch and adopt ceilings are covered by `TestSpoolBoundIsExact` and `TestTheAdoptBoundIsExact`; aggregate pass exhaustion is not covered end to end |

Nine ingest envelopes are fuzzed for panics, determinism, and the property that
the reported accepted count equals the rows that landed
(`.github/workflows/fuzz.yml`, 23 targets across four packages). `_bulk` --
reached from `/​_bulk` and `/insert/elasticsearch/_bulk` -- has no fuzz target;
`FuzzESSearchBody` covers `_search`, which is a different parser.

## What this does not defend against

- **An operator with the data directory.** Groups are not encrypted at rest.
- **A compromised peer within the cluster.** Anti-entropy validates what it is
  given — hashed against the digest requested and parsed as a group — so a peer
  cannot write arbitrary bytes into another's store. It can still serve wrong
  ANSWERS for its own shard, and nothing cross-checks a shard's rows against a
  second replica at read time.
- **Traffic analysis.** Query timing reveals roughly how much matched.
- **A malicious auth configuration.** The file is trusted input.
- **Denial of service by a credentialed client.** Limits bound one request and
  one tenant; a client with a valid credential and many tenants can still make
  the machine work hard.
