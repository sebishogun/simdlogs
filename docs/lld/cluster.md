# LLD: cluster

Source: `internal/api/cluster.go`.

> **Status: not yet production-safe, for reasons stated below rather than the
> ones this banner used to give.** The four stale merges it described are
> fixed, and every route is classified and gated (see "The route surface"), so
> those sentences were describing a state the file's own tables had already
> moved past — a status block that ages is worse than none, because it is the
> first thing a reader believes.
>
> What is actually outstanding: the published benchmark table has no
> machine-checked provenance (`docs/release-readiness.md`), repair is an
> operator action with no lineage tracking so it refuses rather than resolves a
> compaction or retention divergence, and there is no single command that
> restores a cluster archive. Correctness defects found by review are fixed and
> tested; the list of what remains is in `docs/release-readiness.md` and
> `CHANGELOG.md`, and both are kept current.

## The internal protocol

`cluster_protocol.go`, `cluster_client.go`. Internal responses carry an
envelope in headers — version, shard, replica, completeness, high watermark,
error class, trace id — and the public response bodies are no longer the merge
state.

They were. A router called the same public endpoints a client calls and merged
the public bodies, which has three consequences and all of them are bad. A
public response has no field for "I skipped two groups because my disk is
degraded", because a client has no use for one, so a router merging public
bodies **reports a short answer as a whole one**. A public shape is a promise
to clients and an internal one is a promise between versions of this binary;
tying them together means neither can move. And there was no version at all, so
a router talking to a node from another release silently merged whatever came
back — a field that moved produced a plausible wrong answer rather than an
error.

An unknown version is **refused, not negotiated**, in both directions. A router
that spoke two versions would have to write every merge twice and test it once;
during a rolling upgrade the honest behaviour is that a node on an unknown
version is one this router cannot use, reported as incomplete where the caller
can see it.

| class | meaning | another replica? |
| --- | --- | --- |
| `unavailable` | refused, timed out, TLS failed, DNS | yes |
| `version_mismatch` | speaks a protocol this binary does not | yes — the rest of the shard may be upgraded |
| `overloaded` | refused for a budget reason | yes |
| `unauthorized` | this **router's** credential was refused | no — every replica refuses it identically, so retrying turns one 401 into N and delays the report |
| `malformed` | unparseable or oversized | no — it will return the same thing |
| `degraded` | answered from an incomplete store | — |

**The client.** Every peer call used `http.DefaultClient`, which has no timeout
at all: a peer that accepts the connection and never answers held a router
goroutine, a pool slot and the caller's request for as long as the caller
waited — times the number of shards. It also shares a process-wide transport
with any other outbound HTTP and cannot carry a client certificate, so mTLS
between nodes was not expressible. The peer client has its own transport with
dial, TLS-handshake and response-header timeouts, its own pool, and no client
timeout — the caller's context carries the deadline, and a second one here
would cut a query the caller was still waiting for.

The body is **bounded** (256 MiB) and an oversized one is **discarded, not
truncated**: a truncated JSON document is unparseable and a truncated NDJSON
stream is a partial answer indistinguishable from a complete one. Redirects are
not followed — a peer does not redirect, and following one would send the
router's credential to whatever host the response named.

**Headers are forwarded explicitly**, not copied wholesale: the resolved
`AccountID`/`ProjectID` and the tracing headers, and nothing else. Copying the
set would forward the client's `Authorization` and cookies to every storage
node, which is how one node's compromise becomes the cluster's — the router
authenticates to peers as *itself*.

Failures are **returned, not swallowed**. Every replica error was a bare
`continue` and the whole shard a `return nil, false`, so a caller could not
tell "this shard has no data" from "every replica is down" from "the credential
was refused" — and the merge treated all three as an empty contribution.
`fanOutPeers` returns one `PeerResponse` per shard including the failures;
`bodiesOf` keeps the old `[][]byte` shape for merges that have not yet learned
to report completeness (task 8.3).

## Merges, per endpoint

Fixture-tested in `cluster_envelope_test.go` against the **exact bodies a
storage node emits today**, so a merge written against a remembered envelope
fails in a test rather than in a cluster. Four were stale; all four are fixed.

**Shared values envelope.** Every one of `streams`, `stream_ids`,
`field_names`, `field_values`, `stream_field_names` and `stream_field_values`
answers through `writeValues` on a storage node, which emits `{"values":
[...]}`. The router asked for `"streams"` and `"stream_ids"` on their
respective paths — keys **no backend has ever sent** — so both merged an absent
field and answered an empty list, under a key a storage node does not use
either. The same path returned a different shape depending on deployment mode.
The key parameter is gone rather than corrected: a parameter that must always
take one value is a way to get it wrong again.

**Hits.** The router decoded `{"_time": ..., "hits": ...}`, a bag of
`{time, count}` objects, against an endpoint that returns the dense
`{fields, timestamps[], values[], total}` shape — so every field was absent and
a cluster histogram answered one bogus series, `[{"_time":"","hits":0}]`. The
*same* stale shape was in the embedded UI (7.4): two independent readers of one
endpoint, both written against a remembered envelope. Series now merge by label
set, then buckets sum per timestamp.

**Stats range.** Series were concatenated, so one present on three shards
appeared three times with identical labels. That is not a valid matrix — a
Prometheus client draws three lines, every point repeatedly, and any
aggregation counts each shard as a separate series. Every number was
individually correct and the answer was wrong. Identical label sets merge and
points at a timestamp sum.

> **Additive statistics only.** Summing shards is correct for counts and
> wrong for averages, quantiles and distinct counts. `stats_query_range` over a
> cluster is meaningful only for additive aggregates; a non-additive one merged
> this way returns a confident wrong number, and this is stated rather than
> papered over by summing something that must not be summed.

**Elasticsearch `_search`.** Hits were concatenated and returned whole, so
`size: 10` across three shards returned thirty documents — an ES client that
renders `size` results shows three pages on one, and one that paginates by
`from` skips two thirds of the corpus per step. `from`/`size` now apply to the
merged hits; `hits.total` was already the cluster-wide count and stays it.

## Distributed query planning

The router used to send the whole query string to every shard and concatenate
the final rows. For a bare filter that is right. For a pipeline it is wrong, and
wrong in the way that is hardest to catch: every answer is a plausible number.

| Query | What three shards returned | The answer |
|---|---|---|
| `* \| stats count() n` | three rows, each a shard-local count | a client reading the first row gets a third of the count |
| `* \| sort by (t) \| limit 10` | each shard's top ten, concatenated | thirty rows; the true top ten only by luck |
| `* \| uniq by (user)` | each shard's distinct users | anyone active on two shards counted twice |

`query.ClassifyPipe` puts every pipe in one of four classes:

- **row-local** — each output row is a function of one input row (filters,
  `fields`, `rename`, `delete`, `format`, `extract`, `math`, the `unpack`
  family, …). Applying it per shard and merging equals applying it after
  merging, so it runs where the data already is.
- **mergeable-aggregate** — `count`, `sum`, `min`, `max`, `sum_len`,
  `count_empty`: partial state a coordinator can combine.
- **global-order** — `sort`, `limit`, `offset`, `tail`, `top`, `uniq`, `rank`.
  A shard cannot know whether its own best row is the cluster's.
- **coordinator-only** — subqueries (`join`, `union`, `stream_context`),
  introspection pipes, `sample`, and any aggregate with no mergeable partial
  state.

`sample` is classed coordinator-only although its shape is row-local: each
shard sampling 10% of its own rows does yield 10% of the cluster, but the
selection is per shard, so a caller asking for a sample of the cluster gets a
stratified one instead.

**The default is coordinator-only.** A pipe the classifier has not been taught
about runs at the coordinator — slower and correct. The other default, "assume
row-local", is precisely how a newly added pipe would start silently returning
per-shard answers.

`query.PlanDistributed` splits at the FIRST non-row-local pipe: the prefix goes
to the shards, everything from that pipe onward runs once at the coordinator
over the merged rows. `planQuery` cuts the shard query from the raw text head
plus that many pipe segments and then CHECKS the cut against the parse,
refusing when they disagree. It is not rebuilt from the parsed
filter plus that many pipe segments, counted rather than edited textually —
`pipeSegments` walks the text with quote and nesting awareness so `_msg:~"a|b"`
is not cut in half and half a regex sent to every shard.

A prefix is also why a count suffices: the nth kept pipe is the nth text
segment after the head, by construction. A planner that reordered or took from
the middle could not name its pipes in the original text, and re-serialising a
parsed pipe would mean writing a printer for every pipe in the language and
keeping it exactly in step with the parser.

**Order into the coordinator half is ascending.** A storage node scans
oldest-first, so a pipe that reads position — `offset`, `limit` without a sort,
`tail` — must see the same order it would see on one node. The bare-select path
keeps newest-first, because there `limit` means "the newest N".

### Refused rather than answered

An aggregate with no mergeable partial state is a 400 carrying the reason, not
a number:

| Aggregate | Why | What to do instead |
|---|---|---|
| `quantile()` | a quantile of a union is not any function of the shards' quantiles — the median of medians is not the median | needs a mergeable sketch (t-digest, DDSketch) with a documented error bound; not in this build |
| `avg()` | averaging averages is wrong whenever shards hold different row counts: 10, 20, 30 over 1, 1 and 1000 rows averages to 20 where the true mean is 29.97 | `sum()` and `count()`, which do merge |
| `uniq()`, `count_uniq()` | summing per-shard distinct counts double-counts every value on more than one shard | needs an HLL sketch on the wire |
| `histogram()`, `rate()` | no merge in this build; `rate` would need each shard's window coverage | — |

A wrong percentile looks exactly like a latency, and capacity decisions get
made from it. Refusing is the answer until a sketch exists and is tested
against the exact single-node result.

### The test that would have caught it

`TestSingleNodeAndClusterAgree` (`internal/api/cluster_differential_test.go`)
ingests one corpus into a single node and the same corpus split across three
real storage nodes behind a real router, then asserts the answers are identical
for fifteen pipe shapes. With the planner disabled, ten of the fifteen fail.

The fixture itself needed a fix: `level` was `i%3` and the shard was also `i%3`,
so each level lived entirely on one shard and per-shard aggregation happened to
produce exactly the right answer — `| stats by (level)` passed against the
defect it was written to catch. The group-by key must not be a function of the
shard index.

## The route surface

46 routes. Each is federated, router-local by design, or refused — and the
classification is machine-checked against the mux, not maintained by hand.

A select-router holds no data. Only a handler that knows it is a router fans
out; every other one runs against that empty local store and answers 200 with
nothing. `{"facets":[]}` and `{"count":0}` are indistinguishable from a cluster
that genuinely holds no matching data, so nothing reports a problem and a
dashboard shows an empty panel.

Counting `len(s.backends) > 0` branches finds the handlers that DO federate —
14 of 46. It cannot find the ones nobody remembered.
`TestNoRouterReadSilentlyReadsTheEmptyLocalStore` sends the same request to a
router and to a storage node holding the data, and fails when the storage node
answers with something and the router answers with nothing. It found three:
`/select/logsql/facets`, plain `/select/logsql/stats_query`, and `/select/sql`.

Three companion tests close the gaps a single comparison leaves:

- `TestEverySurfaceRouteIsClassified` walks the mux's own registered paths, so a
  new handler that nobody classified fails rather than going uncovered.
- `TestEveryWriteRouteForwardsToTheShards` checks all 13 ingest routes, because
  each handler decides for itself whether to forward. A router that kept a copy
  would put rows where no read ever looks — reads fan out. "Kept nothing" is
  observed by removing the backends and asking the same process again.
- `TestARefusedSurfaceSaysSo` requires a refused surface to answer an error
  with the reason. All four answer 501; the test accepts any 4xx/5xx, so the
  specific code is a convention rather than a gate. Never 200-and-empty, which a client reads as "the cluster holds
  nothing".

Two of the fixtures had to be repaired before they measured anything: the hits
request used the default time window, which does not cover the corpus, and the
stream-field endpoints ran against a node with no stream fields configured. Both
subtests were skipping, and a skip reads as covered.

## Read completeness

Every merge consumed a `[][]byte` with a nil entry for a shard that did not
answer, and merged the rest. A cluster read with one shard down returned the
other shards' rows, **HTTP 200**, with nothing anywhere in the response to say
a third of the data was missing — indistinguishable from a query that genuinely
matched fewer rows. Confident, plausible and silent.

**The rule: a read fails unless every shard contributed a complete answer.**
Not "every shard answered" — a shard serving from a degraded store answers, and
says so in the envelope, and that answer is missing data too. It counts as
missing, and the only difference is that it looks fine.

A refusal is **503** with the shards named:

```
X-Simdlogs-Shards-Total: 3
X-Simdlogs-Shards-Answered: 2
X-Simdlogs-Shards-Missing: 1(unavailable)
```

**Partial answers are opt-in.** `allow_partial_response=1` — the reference's own
parameter name — is answered **206** with `X-Simdlogs-Partial: true` and the
same missing-shard headers. A dashboard that would rather draw something than
nothing is a real use, and so is an operator triaging with a node down; but it
has to be asked for, because a caller who did not ask has no way to know, and a
monitoring system built on silently-partial answers alerts on the wrong thing
at the worst time.

206 rather than 200-with-a-header because a client that checks `resp.ok` sees
200 for both. The gate returns the `http.ResponseWriter` the handler must use,
and on a partial answer that writer forces 206 on the first write — the status
cannot be set in the gate itself, because every handler sets its own
Content-Type and then writes, so a `WriteHeader` there would be overtaken by
the handler's first `Write` sending 200. `federatedSelect` writes its status
explicitly for the same reason in reverse: an empty result writes no bytes at
all, and a handler that returns without writing sends 200.

`simdlogs_cluster_partial_reads_total` counts them, because a dashboard quietly
running on partial answers is exactly the state this makes visible.

All fourteen federated reads are covered by one-shard-down and all-shards-down
tests (`cluster_failure_test.go`). The list used to be hand-kept and had
drifted to nine while this page said "all nine"; the set is now derived from
`surfaceRoutes()` by `TestEveryFederatedReadIsInTheCompletenessSuite`, which
fails in both directions -- a federated read missing from the suite, and an
entry in the suite that does not federate.

The nine names that used to end this paragraph were the old sentence's list,
left attached to a new clause after the colon -- so they read as examples of
routes that do NOT federate, which is the opposite of what they are, and they
contradicted "fourteen" four lines above. The set is derived now, so there is
no list here to go stale.

## Replicated writes: idempotency and consistency

`forwardWrite` replicated to every member of a shard and relayed the **last**
response's status. Replica A refusing on its own quota and replica B accepting
answered whichever finished last, so the same write was reported stored or
refused by a coin flip — and in the other order, a retry duplicated into the
replica that had already taken it.

**Consistency is explicit.** `X-Simdlogs-Consistency: one | quorum | all`,
default **all**. Quorum is the usual production choice *because a repair
process reconciles the replicas that missed a write* — and this has none yet
(task 8.7). Without one, "quorum" means a replica silently missing data forever
and a read that lands on it returning a short answer with nothing to say so.
Defaulting to the strictest level is the honest position for a system that
cannot yet heal; the default moves when repair is proven, not before.

**Every write carries an id.** The router mints one (or accepts the client's,
so a retry can name the write it repeats) and sends the same id to every
replica. A replica that already committed it answers `X-Simdlogs-Duplicate:
true` and stores **nothing** — which counts as an acknowledgement, because the
data is there and that is the only thing the level asks about.

That is what makes a retry safe. The router cannot tell "did not commit" from
"committed and the answer was lost" — the connection drops while the response
comes back, the router times out, the process is killed between the fsync and
the reply — and those need opposite responses. Without receipts, retrying
duplicates every row on the replicas that did commit, *silently*, because a log
store has no primary key and a duplicated line looks exactly like a line that
happened twice; not retrying loses the rows on the ones that did not. A failed
write therefore returns its id, so the retry is safe by construction.

Ids are cryptographically random, not counters (which collide across routers —
the multi-writer case this exists for) and not content hashes (which would fold
two genuinely distinct identical batches into one, dropping data the client
sent on purpose). A client-supplied id is validated to 8–64 hex characters
before it reaches the manifest.

**Where the receipt is committed, and the window that leaves.**
`AppendGroupIdempotent` commits the id in the *same* manifest record as the
group — one record is one transaction, so the rows and the receipt become
durable together. The server's ingest path cannot use it: the writer batches
rows from many requests, so no single group is "this request's rows". It uses
`CommitReceipt` after the flush instead, which leaves a window — a crash
between the group commit and the receipt commit keeps the rows and loses the
receipt, so a retry stores them again. Given a choice between a duplicate and a
loss that window takes the duplicate; recording the receipt first would lose
the rows while claiming they were stored and refuse the retry that would have
saved them.

A replicated write therefore pays a **flush**. An ordinary client write carries
no id and pays nothing.

**Retention is bounded** at 65536 ids, and the bound is the honest limit: a
retry arriving after that many further writes is not recognised and will
duplicate. A count rather than a duration because the manifest has no clock —
records carry a sequence, not a timestamp.

**Per-replica detail is authorized.** Replica URLs and their individual
failures are the cluster's topology; an ingest client is told `acked`,
`required` and the write id, an operator is told which machine failed. On a
server with no `-auth.config` everything is open, as it is everywhere else.

## Roles

- **Storage node** — a default node: owns tenants' stores, serves ingest and
  queries locally.
- **Select router** — a node started with `-select-backends
  http://node1:9428,http://node2:9428,...`: the vmselect role. Ingest paths
  forward to storage nodes; select paths fan out and merge.

A node is one or the other by flag; there is no automatic membership, no
leader election, no consensus, and no cross-node transaction protocol. Shards
and replicas are the static configuration the operator passes at startup.
This is application-level sharding and replication, deliberately not a
consensus system — the README says the same thing.

## Sharding and replication

`SetReplicas(r)` groups the backends into shards of `max(1, r)` consecutive
URLs (`shards()`). `r = 1` is plain sharding; `r > 1` replicates each shard's
data to every member.

- **Write** (`routeWrites` → `forwardWrite`): a write path (`/insert*`,
  `/_bulk`, `/v1/logs`, `/v1/input`, `/api/*`, `/loki*`) is forwarded with a
  round-robin cursor (`s.rr`, atomic) to one shard, and to EVERY replica in
  that shard, with the request headers cloned (tenant headers ride along).
  The last response is relayed; if no replica answered at all the client gets
  a 502. A replica loss never loses data, and a burst of inserts spreads
  across shards.
- **Read** (`getFromShard`): each shard is asked via one replica at a time —
  a downed replica (connect error or ≥ 500) is skipped for the next in the
  shard, so replicated data is read exactly once per shard and never
  double-counted.

## Merges per endpoint

### How a read reaches a shard

A read fan-out is a **GET** with its parameters in the query string, until the
encoded parameter set exceeds `maxPeerQueryBytes` (60 KiB encoded, about 30 KB
of query text). Past that the request becomes a **POST** carrying the whole
resolved parameter set as an `application/x-www-form-urlencoded` body, with the
query string **empty**.

Why the whole set and not the overflow: `federatedSelect` writes the planned
query into the shard URL before the size is known, so a scheme that moved only
the caller's form left `query` stuck in the request line — the one endpoint the
change existed for. And Go's `ParseForm` copies `PostForm` before the URL query,
so a set split across both has the body winning; sending one resolved set
removes the question instead of relying on the two halves staying disjoint.

Every shard handler reads `r.FormValue`, which is the union of both, so the
switch is invisible to them. `ingestOptions` is the one deliberate
`r.URL.Query()` reader and is on the ingest paths, which never take this route.

A bound the coordinator applies itself — `limit`, `max_values_per_field` — is
stripped from **both** the query string and the parsed form before the request
goes out. Stripping only the query string was a silent wrong answer over POST:
each shard truncated to its own top N and the coordinator summed the truncated
lists, so a value with five cluster-wide hits was reported with four, at
HTTP 200.


| Endpoint | Merge | Status |
|---|---|---|
| `/select/logsql/query` | `federatedSelect`, after `planQuery` splits the pipeline (see **Distributed query planning**). Bare filters and the row-local prefix run on the shards; rows merge in time order (newest first, parsed from each line's `_time`) and `limit` applies across the merged set. Rows are kept as slices into each shard's response body (no per-row string copies — that was the router's dominant cost); the timestamp parse uses the fast RFC3339Nano path, and only a query with a coordinator half pays the decode back into fields. | works |
| `/select/logsql/hits` | `federatedHits` decodes the dense series shape the backend actually answers (`{"hits":[{"fields":..,"timestamps":[..],"values":[..],"total":N}]}`) and merges by label set, summing buckets per timestamp. Fixed in 8.4; it previously decoded the older object shape and answered one bogus zero bucket for any non-empty store. | works |
| `/select/logsql/stats_query` (plain, no `by=` param) | `federatedVector` decodes the Prometheus instant-vector envelope the backend actually answers and sums values per metric label set. Fixed in 8.6; it previously decoded a `{"count":N}` field no backend emits, so the router answered `{"count":0}` for every query against every cluster. | works |
| `/select/logsql/stats_query` (`by=` param) | The backend's `by=` extension answers `{"stats":[{value,hits}]}`, which `federatedStatsQuery` decodes and sums per value. | works |
| `/select/logsql/stats_query_range` | `federatedMatrix`: each shard's series concatenated (shards hold disjoint groups, so a series is one shard's); decodes the backend's `{"data":{"result":[...]}}` envelope. | works |
| `/select/logsql/field_values`, `stream_field_values`, `field_names`, `stream_field_names` | All four route to `federatedValueCounts` (key `"values"`): backends answer `{"values":[...]}` and hits are SUMMED per value, sorted by count desc then value. (`federatedStrings` exists in `cluster.go` but is not wired to any route.) | works |
| `/select/logsql/streams` | Routes to `federatedValueCounts`, which reads the `{"values":[...]}` envelope the backend actually answers. Fixed in 8.4; the router previously looked for a `"streams"` key that no backend emits and answered an empty list however many streams the shards held. | works |
| `/select/logsql/stream_ids` | Same path and same 8.4 fix (the key was `"stream_ids"` against the same `{"values":[...]}` envelope). | works |
| `/select/logsql/facets` | `federatedFacets`: hits sum per (field, value), then `limit` and `max_values_per_field` apply to the MERGED list, so "the top 10 values" is the cluster's. Added in 8.6; the handler had no router branch at all and faceted the router's empty store. | works |
| `/select/sql` | `federatedSQL`: federated when the parsed pipeline is entirely row-local, refused with the reason otherwise. Added in 8.6; it previously queried the router's empty store. | partial by design |
| `/select/logsql/tail` | Refused (501). A cluster tail is a long-lived stream from every shard merged by arrival time, and the merge has no completeness signal — a shard that stops answering drops out with nothing to say so. It previously tailed the router's empty store and streamed forever without ever yielding a row. | refused |
| `/select/vector` | Refused (501). k-NN across shards needs each shard's top k merged by distance; one shard's neighbours, or a concatenation, answer a different question. | refused |
| `/admin/backup`, `/admin/acknowledge-degraded` | Refused (501). A router's backup of its own empty store restores as an empty cluster, and acknowledging degradation here clears nothing on the shards that are degraded. Coordinated forms are task 8.7. | refused |
| `/_search` | `federatedESSearch`: hits merged, `hits.total` summed, `relation: "eq"`. | works |
| `/_count` | `federatedESCount`: counts summed. | works |

**What a federated read requires of the request**, and where it is checked:

- **`query` is required**, on every route in the table above whose parameters
  come from the form and the URL — the same rule a single node applies in
  `parseRequest`. `fanOutChecked` refuses a blank one with the node's own
  `missing \`query\` arg` before any shard is asked. Fanning out an empty
  selector instead made every shard refuse and the caller saw
  `503 … N of N shards could not answer completely`, which sends an operator to
  inspect the storage nodes for a parameter their own request did not carry.
- **Both form encodings are read.** `application/x-www-form-urlencoded` and
  `multipart/form-data`, because a single node reads both — `FormValue` calls
  `ParseMultipartForm`. A multipart body that will not parse is ignored rather
  than fatal, again matching the node, and the URL query is then the whole
  request.
- **A body under any other content type is ignored**, as a node ignores it. The
  request is then refused for the reason that is true (no `query`), not for the
  header.
- **Three routes carry a document, not a form**: `/_search`, `/_count` and
  `/select/vector`. Their body is a JSON document,
  read unconditionally whatever the content type says — `curl -d` sends a form
  content type by default, and treating that as parameters left a real query
  document unread. They carry `routeSpec.form = false` for the same reason: the
  request-shape guard parses a multipart body up front so its temp file can be
  removed, and doing that on those three routes **consumed the document** —
  measured, `/_count` with a JSON body under `multipart/form-data` answered
  `400 simdlogs: EOF` on a node and `503` on a router, where every other
  framing of the same document answers `200`. `/select/vector` is the one that
  reads parameters *as well as* a document, so `form` cannot be set correctly
  for it either way: its `start` and `end` come from the URL now, the same rule
  `protocols.go` states for the ingest routes.
- **A parameter sent in both the URL and the body resolves as a node resolves
  it.** `r.Form` already encodes Go's own precedence — body-first for a
  urlencoded form, URL-first for multipart — so `withFormInURL` forwards
  `r.Form`'s values verbatim rather than re-deciding, and the plan's own keys
  (`query`, and `limit` when the router runs coordinator pipes) are marked so a
  caller's form cannot overwrite the planned query. Before this, the same
  request answered `30` on a node and `10` on a router, both at HTTP 200.

**Which routes parse a form at all** is per route, not global. `routeSpec.form`
selects it, and it is wrong in both directions: parsing where the handler reads
the raw body consumes it (the three document routes above), and *not* parsing
where the handler calls `FormValue` leaks one multipart temp file per request,
because the handler parses on a request copy the server never sees and
`finishRequest` cleans up the original. Neither failure is visible in an
ordinary response, so the list is not maintained by inspection:
there are **two** gates, one against a plain server and one against an
authenticated server. Each posts a spilling multipart body to every path
`Handler()` registered and fails on any file left behind.

Both are needed because the leak needs a middleware that replaced the request,
and with authentication off `withTenant` makes no copy for the health routes --
so the one configuration in which `/health`, `/-/healthy` and `/-/ready` leaked
was the one the first version of the gate never built.

The plain gate, `TestNoRouteLeavesAMultipartTempFileBehind`, carries a positive
control: a handler wired like a route whose spec forgot `form: true`, which must
leave a file. Its check is an absence and an absence has two causes, so without
the control a spill threshold too high to spill would pass.

`TestNoRouteLeavesATempFileBehindOnAnAuthenticatedServer` has no control. What
establishes that it can see a leak is the mutation: reverting `format` to
`r.FormValue` reddens it naming all three health routes, and leaves the plain
gate green.

The plain gate found `/internal/replica/group`,
whose handler read `digest` with `r.FormValue` before `io.ReadAll`-ing the body
it was about to adopt — the URL is where the client already sends it.

Tenant headers (`AccountID`, `ProjectID`) propagate on every fan-out, so each
backend answers for the same tenant.

The merge for `select` is exact: rows are independent, so
concatenate-sort-limit is the correct distributed answer. Do not generalize
from it: the four broken rows above are stale-envelope defects, not design
limits, and until they are fixed the only correct statement is that **no
router-mode merge beyond `select` (and the working rows above) is verified**.

## Not federated in router mode

Every route is now classified and the classification is checked against the mux
(see "The route surface"), so this list is derived rather than remembered.

**Refused with 501**, because they cannot be answered correctly across shards
in this build: `/select/logsql/tail`, `/select/vector`, `/admin/backup`,
`/admin/acknowledge-degraded`.

**Router-local by design** — the answer is about this process, not the data:
`/metrics`, `/alerts`, `/flags`, `/health`, `/-/healthy`, `/-/ready`, `/vmui`,
`/`, and the two `/internal/replica/*` endpoints (one replica's own inventory;
the cluster-wide view is `/admin/cluster/repair`).

This section previously listed `/select/logsql/facets` and `/select/sql` as
having no router branch. Both have one — `federatedFacets` and `federatedSQL`,
added in 8.6 — and all six introspection endpoints merge through
`federatedValueCounts`. The text had not moved with the code, and a reader
checking whether an endpoint was safe would have been told the wrong thing by
the same file that said the right thing four hundred lines earlier.

## Failure behavior

- A downed replica: reads skip to the next in the shard; if all fail, that
  shard contributes nothing (a partial answer, not an error).
- A write with all replicas of the chosen shard unreachable: 502
  `all replicas unreachable`.
- Reads tolerate it by construction; writes are synchronous to every replica
  of the chosen shard (replication is write-path, not background).

## Measured state

`docs/wrong.md` ("Cluster scaling: at one-node scale the cluster is pure
overhead"): the federated path measured as pure overhead when the cluster is
a single node — expected, since a one-node cluster should run a storage
node. The cluster scaling benchmark (`internal/bench/cluster_test.go`)
exercises the router with real fan-out, and the zero-copy federated merge
(byte slices into response bodies) is the fix that kept the router off the
hot path for big results. None of that measures answer correctness — the
envelope defects above are exactly what such a benchmark cannot see.

## Repair, and what makes it safe

A write goes to every replica of its shard. Any of them can miss one, and
nothing brought it back into line: the replica stayed short forever, and a read
that landed on it returned fewer rows with nothing to say so. That is why
`ConsistencyAll` is the default write level — without repair, "quorum" means a
replica permanently missing data.

Groups are immutable once sealed, so this is set reconciliation, not conflict
resolution. Each replica reports the digests of the groups it holds; the union
is what the shard should hold; each replica is sent what it lacks.

**Reconciliation is by content, never by manifest id.** An id is assigned by the
store that wrote it — `nextID++` — so two replicas agree only by coincidence of
ordering, and they stop agreeing precisely when one misses a write:

| | replica A | replica B (missed W2) |
|---|---|---|
| id 1 | W1 | W1 |
| id 2 | W2 | **W3** |
| id 3 | W3 | — |

B is not missing id 2; B has id 2, holding different rows. Repair by id would
copy A's id 3 into B and leave B holding W3 twice — turning a missing write into
a duplicated one, which is worse, because a gap shows up as a short answer and a
duplicate shows up as a plausible larger number.

Three properties do the safety work:

- **Repair only adds.** Nothing deletes, so no pass can remove the last good
  copy of anything, and a replica holding data the others lack hands it over
  rather than losing it. Verified both directions, not just leader-to-follower.
- **The receiver validates.** Bytes are hashed against the digest that was asked
  for and parsed as a group before anything is committed, so a peer that is
  compromised or on another format version cannot write into the store.
- **Bounded per pass** — 64 groups, 1 GiB, counted cluster-wide rather than per
  shard — and the report says what it left, so
  "still diverging" is visible rather than inferred. An unreachable replica is
  never read as an empty one: its silence would otherwise make the union wrong
  in both directions at once.

A repaired group is committed under a fresh local id and consumes no write
receipt. It is not a client write, and taking an idempotency token a real retry
would need is a cost with no benefit.

## Coordinated backup

Backing up every node and keeping the archives in a directory is not a cluster
backup: nothing records the topology, replicas of one shard are not
interchangeable, and the archives are taken at different moments.

`/admin/cluster/backup` writes one tar holding a manifest and one archive per
shard, taken from a replica that holds the **whole** shard. Completeness is
checked, not assumed — the replicas are asked for their inventories first, and
if no replica of some shard is complete the backup **fails with a 503 naming
repair**. A cluster backup that silently captured the shortest replica is worse
than no backup, because it looks like one.

The manifest is the first tar entry, so a reader validates before streaming
gigabytes. `ValidateClusterBackup` refuses a newer format, another protocol
version, a shard count that does not match the target topology, a shard
appearing twice, and a shard naming no archive. The shard-count check is the one
a directory of per-node archives cannot even express: restoring an N-shard
backup into an M-shard cluster puts rows where no query looks for them.

Per-shard high watermarks are recorded and `spreadNanos` in the archive's `cluster.json` reports the distance
between the earliest and latest. Reported, not enforced — no bound is right for
every deployment, and a threshold invented here would either refuse good backups
or bless bad ones.

## Chaos

Seven scenarios, one rule: an exact answer or an explicit failure, never a
smaller number with HTTP 200.

| Scenario | Required behaviour |
|---|---|
| Replica kill | exact; the surviving replica holds the shard |
| Whole shard down | 503, never one shard's rows as the cluster's |
| Stalled peer | bounded by the router (10 s header timeout), not by the caller |
| Stale replica | repair converges both replicas, then the answer no longer depends on which is read |
| Corrupt group | never served as data; the store refuses to open over damaged groups |
| Rolling restart | exact throughout, replica by replica |
| Disk full | 507 with the budget named, and the refusal reaches the client through the router |

## What this is not

No consensus, no leader election, no transactions, no automatic membership
discovery, and no shard rebalancing when the backend list changes (the operator
restarts the router with the new list).

Query planning splits row-local work from coordinator work and refuses what it
cannot merge, but it does not push aggregates down: a mergeable `count`/`sum`
still runs once at the coordinator over the merged rows rather than as partial
state on the wire. That is correct and slower than it could be. Pushing one
down means putting a partial state on the wire, and a partial state that is
subtly wrong is a number nobody can spot.

There is still no merge for avg/quantile/distinct stats — those are refused with
the reason. Repair reconciles replicas of a shard and does not move data between
shards, so it cannot help a rebalance. A restore of a cluster backup is validated
here but performed by the operator per shard; there is no single restore command
that unpacks a cluster archive. Those are roadmap items with measurable exits,
not current behavior.
