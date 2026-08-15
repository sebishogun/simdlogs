# Changelog

Every entry below is a commit in this repository. Nothing here is written from
a plan: an entry exists because the change is in the code, the tests and the
history, which is the only way a changelog stays true.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

No version has been tagged. This section is what a first release would carry.

### Added

- Add checksummed bounds-safe group format (`8b66753`)
- Add exclusive storage lock and durable manifest (`88ffce9`)
- Add bounded server configuration (`cad57de`)
- Enforce HTTP ingestion contracts (`775fbc0`)
- Enforce authenticated tenant RBAC (`6f9e928`)
- Compact small immutable groups safely (`abc941e`)
- Enforce disk and tenant storage budgets (`d1478f1`)
- Govern query concurrency and memory (`9311c44`)
- Stream unpiped query results (`b80727e`)
- Add stable cursor pagination (`5bc2e1f`)
- Connect bounded vector ingest and search (`09744d8`)
- Add truthful health and readiness (`ac75d82`)
- Add production observability contracts (`0eb4db3`)
- Make log rules operationally usable (`c4c7d56`)
- Version the internal cluster wire protocol (`cbef3c4`)
- Add idempotent consistent cluster writes (`e4853bd`)
- Add correct distributed LogsQL execution (`09ed47b`)
- Make the cluster route surface complete (`328cbbc`)
- Add static-cluster repair and backup (`0216ec1`)

### Fixed

- Preserve durability and stream identity in parallel ingest (`d1b6618`)
- Make every storage replacement durably atomic (`eef82fe`)
- Make retention durable and leak-free (`0b5a797`)
- Serialize safe tiering transitions (`fcee86b`)
- Drain background services on shutdown (`035b845`)
- Correct Elasticsearch bulk reject attribution and allocation (`b42c710`)
- Syslog frame ceiling, listener death, and the time-based flush (`e941cef`)
- Define write failure and retry behavior; make backups self-validating (`90569d3`)
- Close the storage budget's bypasses and platform gaps (`3db1075`)
- Make query limits semantics-preserving (`8991960`)
- Make stream_context stream-correct (`f7fdf90`)
- The storage-budget fixes reintroduced the shape they fixed (`86af987`)
- Enforce ES and SQL query contracts (`c19f44b`)
- Land two adversarial reviews of the query and storage work (`582e703`)
- Make the embedded UI match secured APIs (`d8be2fc`)
- Make cluster read completeness explicit (`8f25466`)
- Merge the shared values envelope the backends actually send (`0c2429c`)
- Merge dense hit series by label set and timestamp (`5ae5ccf`)
- Merge stats-range series with identical labels (`e4e1dd5`)
- Apply ES from/size to the merged cluster hits (`87e9bcf`)

### Not included

Documentation, chores and refactors are in the git history and not here: a
changelog is what changed for a user of the software, and 132 chore commits
would bury the 39 that did.

**Known limitations at this point**, stated because a changelog that lists only
what works is an advertisement:

- No incremental backup; RPO is bounded by how often a full capture is
  affordable (`docs/runbooks/backup-restore.md`).
- Repair is an operator action, not automatic, and moves data only between
  replicas of one shard — it cannot help a rebalance.
- Non-mergeable aggregates (`quantile`, `avg`, `uniq`, `count_uniq`,
  `histogram`, `rate`) are refused across shards rather than answered, until a
  mergeable sketch exists and is tested against the exact single-node answer.
- `/select/logsql/tail` and `/select/vector` are not federated; they answer 501
  on a router.
- No single command restores a cluster archive; the archive is validated and
  the unpacking is per shard.
- linux/386 is not supported: a dependency does not compile for a 32-bit int.
- The published scale and head-to-head numbers have not been re-measured on a
  quiet machine since the harness was corrected. The quiet-machine gate now
  refuses to produce a number above load average 1.
