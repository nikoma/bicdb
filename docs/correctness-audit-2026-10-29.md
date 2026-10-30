# Correctness audit, 2026-10-29

## Baseline and scope

The initial local `main` was `1338f52`; GitHub `main` was already `6c72420`.
The work was isolated from the staged rollback/removal patch in the original
checkout. No staged removals were incorporated or discarded.

Review started with the secure-delete resurrection fix (`9d8a9e5`) and the
UPDATE/DELETE predicate-fallback fix (`e1d5bca`), then covered the subsequent
replication, bootstrap, archive/PITR, flashback, index, election, live-query,
constraint and SQL compatibility changes through the GitHub baseline. The scan
traced durability publication order, restart state reconstruction, authorization,
commit-sequence retention, role transitions, planner fallbacks, and query
resource isolation. Regression suites cover both embedded and server-paged
storage where those paths differ.

## Findings addressed

| Finding | Consequence before the fix | Retained evidence |
| --- | --- | --- |
| Final archive CRC/decoding errors treated as torn crash tails | Recovery could silently omit committed data; reopening could truncate corruption evidence | `checksum_corruption_is_never_repaired_as_a_crash_tail` |
| Explicit PITR commit target beyond the available archive accepted | A restore could report success without reaching the requested recovery point | `replay_refuses_a_missing_requested_suffix_and_invalid_append_is_atomic` |
| Archive cursor advanced before append durability; malformed batches wrote prefixes | Retry could skip unarchived commits or remain stuck after a partial append | Full-batch validation before mutation; durable-prefix retry, torn-append rollback and poisoned-writer handling |
| Empty archive had no cluster metadata | Empty database backup/restore drills failed despite a valid base | `an_empty_database_also_has_a_verifiable_archive_identity` |
| Bootstrap installation preceded database ownership locking | A second opener could rename a running server's directory underneath it | `a_staged_bootstrap_cannot_replace_a_live_database` |
| HA metadata changed its in-memory ballot before disk publication succeeded | A failed write could influence later election decisions without durable evidence | `failed_ha_metadata_write_does_not_publish_a_ballot_in_memory` |
| Imported catalog operations followed durable row WAL without catalog redo | Restart could advance the applied watermark past an unapplied rename/drop/index operation | `replica_catalog_redo_finishes_a_rename_after_its_row_wal_committed` |
| Standby index reconciliation swallowed failures | A failed or stale index catalog could be acknowledged and survive promotion | Reconciliation propagates failure and retains pending catalog redo until success |
| Native role checks used startup configuration after promotion | A promoted node could accept imports and report an obsolete apply position | `planned_switchover_drains_durably_and_requires_the_exact_target_prefix` |
| Public collection DROP bypassed standby write refusal | Embedded callers could delete standby data directly | `a_standby_refuses_direct_collection_drops` |
| Imported WAL encoded the replay machine's current timestamp | Cascaded replication/recovery could change commit-time PITR semantics | `imported_commits_keep_their_original_timestamp_when_reexported` |
| Async quorum fast path ignored permanent closure | Fenced/shut-down writer authority could still return confirmation success | `closing_writer_authority_also_stops_async_confirmations`; existing confirmed-quorum-prefix behavior retained |
| External attachment mutations shipped references without blob bytes | A standby or log-only restore could acknowledge unreadable data | `external_attachments_require_a_snapshot_and_cannot_be_acknowledged_as_missing_blobs`; mutations fail before commit until the stream can transport blobs |
| Named live-query deltas used public channel broadcasting | Another connection with the same name or a LISTEN could receive another subscriber's result | `registered_changes_are_private_even_when_subscription_names_collide` |
| Dispatcher evaluated through an uncancelable describe path while holding a database read lock | A costly subscription could block peers; recursive locking could deadlock behind a waiting writer | Actual cancellable execution, per-round budgets and scoped database reads |
| Subscription notification queue discarded old deltas on overflow | Client result state could diverge silently | Bounded private batches; explicit connection-wide subscription invalidation on backlog overflow |

Promotion now closes the current runtime's writer authority until restart, so
it cannot accept writes in the interval before the new primary's replication
and lease configuration is installed. A planned source drain survives restart
and never automatically reopens for writes after a failed handoff.

## Existing local work

Local connector, live-query, incremental live-query, relocation-test and WeWoBo
compatibility branches were compared with their corresponding squash merges
and the newer GitHub tree. Their useful implementations were already present
in GitHub `main` (`0f69db3`, `d4653ce`, `276b5aa`, `7d77103`, `7787642` and later
compatibility commits). Reapplying those old diffs would duplicate changes and
risk overwriting subsequent fixes. Clean worktrees belonging to that work were
preserved.

## New verification and protection evidence

The retained tests exercise archive corruption and gaps, full restore/source
comparison, scratch promotion/write/restart, exact-prefix planned handoff,
durable drain, stale/wrong-target refusal, actual three-process switchover and
primary death, bounded freshness including fixed snapshots, independent
role/tenant admission, retained subscription memory, and private notifications.
The operational interfaces and limits are documented in
[verification-and-workload-protection.md](verification-and-workload-protection.md).

The audit is not a claim that every possible crash, filesystem failure,
concurrency schedule, or extension allocation has been exhausted. The scheduled
checks provide recurring evidence rather than such a proof. Mixed-version
rolling upgrades and resumable partial snapshot byte transfers remain outside
this implementation; native replication resumes at durable commit boundaries.
Full checks are proportional to data/index size and require capacity planning;
full-table comparisons on the replication connection can delay that stream
while hashing, so benchmark the configured verification cadence against the
failover and synchronous-commit timeouts for large corpora.

## Local validation

- Core unit, backup, paged backup, flashback, live replication, archive/PITR,
  signed journal and transaction-isolation suites: **502 passed**, 3 existing
  ignored unit tests.
- SQL unit, SIS/WeWoBo compatibility, schema privileges, foreign-key authorization
  and transaction uniqueness: **405 passed**, 3 existing ignored unit tests.
- Pgwire unit, full protocol, live-query and scheduled-verifier integration:
  **283 passed**.
- CLI three-process primary-death and planned-switchover tests: **2 passed**.
- `cargo check --workspace --all-targets --locked`, formatting, and whitespace
  checks passed. Existing compiler warnings remain; no failing test was disabled.

The async-authority change initially exposed a real standalone graceful-shutdown
regression. Shutdown now preserves in-flight standalone query draining while
native HA still closes writer authority and wakes quorum waiters; the full
165-test protocol suite passed after that correction.

The original squashed feature tips are retained in the merge ancestry without
reapplying their old file contents over subsequent fixes.
