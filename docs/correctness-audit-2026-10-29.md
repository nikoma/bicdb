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

## Whole-platform follow-up

The follow-up expands execution to every workspace package, all core and SQL
integration targets, protocol clients, and the platform Python/JavaScript
contracts. Static review also follows application receipts/outboxes, native
state acknowledgement, peer synchronization, constraint enforcement and query
materialization. This is a finite audit, not proof that the platform has no bugs.

Strict comparisons run against an isolated PostgreSQL 18.4 database, with seeded
query/mutation cases and focused regressions. They check rows, errors, affected
row counts, column names and available wire type OIDs. No fixture in
`fixtures/correctness-audit` allows an expected difference.

| Additional finding | Correction and retained regression |
| --- | --- |
| JSONB B-tree probes matched object serialization rather than SQL equality | Keep those indexes durable but exclude them from SQL query planning; scan/index/reopen and predicate-sensitive UPDATE/DELETE coverage in `platform_correctness_audit` |
| Paged streaming ORDER BY used JSONB equality keys as ordered keys | Use the semantic materializing sorter; regression checks limited results after reopen |
| Overlapping UNIQUE JSON writes could both commit equivalent objects | Reserve canonical constraint keys until transaction/savepoint completion, then check current committed rows plus pending writes |
| Child insertion could race parent deletion/update and leave an orphan | Child checks and parent actions reserve the same referenced key, with current-state integrity reads |
| Nullable composite references were incorrectly restricted or cascaded | Apply MATCH SIMPLE's any-NULL exemption to parent actions as well as insert checks |
| PL/pgSQL `NEW.field := ...` silently assigned an unrelated local | Preserve and assign the record path; returned trigger rows now contain the change |
| Generated columns preceded BEFORE-trigger changes | Generate from the final trigger postimage in INSERT, UPDATE, upsert and COPY |
| COPY validated rows before BEFORE triggers | Validate postimages and roll back all copied rows and trigger effects on failure |
| Referential actions stopped at immediate children and bypassed child triggers | Traverse descendants with cancellation/depth bounds and cycle tracking, execute row triggers and generated-column/constraint checks |
| Autocommit referential effects could survive a later statement failure | Put the root mutation and all dependent effects in one statement transaction; late-error rollback regression retained |
| Scalar subquery aggregate metadata fell back to observed int8 values | Resolve the subquery's own relation environment, including empty MIN/MAX results |
| Aliased base-table qualifier copies were mistaken for ambiguous columns | Deduplicate the same physical column within its base relation during type inference, retaining ambiguity across separate relations and derived outputs |
| Catalog, plan and role caches keyed a database allocation address | Use the monotonic database instance ID; a deterministic address-reuse test checks table metadata and superuser isolation |
| Native index replacement with unchanged name/count retained stale SQL lookup plans | Invalidate by index generation, and invalidate transaction lookup answers after a new commit |
| Compiled routine identity used only definition/argument digests and lengths | Key the cache by complete definitions and arguments so a hash collision cannot select another program |
| Routine type caches shared catalog generations across databases, with collision-prone XOR stamps | Allocate distinct scope tokens for the complete database/catalog generation tuple; regression executes shared compiled code over different integer types |
| Case-folded hashes and catalog maps aliased quoted table/view/index/sequence/schema names | Preserve exact identities, durably reserve table/view row-type OIDs, keep view IDs on replacement, and verify them after reopen; new namespace/relation/index/sequence hash collisions are rejected explicitly |
| Legacy duplicate catalog IDs were invisible to physical checksums | Offline and scheduled verification now check SQL type, relation, namespace and constraint OID uniqueness, with injected legacy-corruption regressions through SQL, CLI and pgwire |
| Constraint OID arithmetic exceeded PostgreSQL's unsigned 32-bit range | Allocate bounded synthetic identifiers, detect collisions before exposing filtered catalogs, and test definition lookup and foreign-key joins |
| Index catalog vectors used one-based subscripts and omitted per-column sort options | Preserve PostgreSQL zero-based intvector subscripts and expose direction/null-order flags; an actual Prisma introspection run found the mismatch |
| COUNT output names differed from PostgreSQL, breaking unaliased derived-column references | Return `count` consistently for COUNT(*), COUNT(expr), and COUNT(DISTINCT expr); regression checks both storage modes and outer references |
| Legacy virtual-query predicates accepted unsupported expressions as true, including WHERE FALSE | Use the shared SQL row executor for virtual relations; sequence filters evaluate SQL truth and propagate errors |
| Policy DDL stripped table schemas and could alter a same-named public table | Resolve CREATE/ALTER/DROP POLICY through the shared relation-name encoder before checking ownership |
| Explicit sequence names discarded schema qualifiers, sharing counters and default/ownership bindings | Resolve the same namespace-aware key for CREATE, runtime calls, defaults and ALTER ownership; preserve quoted identities and test independent counters through reopen |
| Virtual catalog SELECT * changed order and exposed system tableoid when rows existed | Project declared column order for both empty and populated relations, retaining explicit system-column access |
| Empty information_schema relations lost column/parameter types; several catalogs hardcoded public and physical names | Declare supported columns independently of data, preserve object namespaces/logical names, and expose every composite key and the referenced side of foreign-key usage |
| View catalogs omitted `pg_views` and assigned nonpublic views to public | Expose typed `pg_views` rows and preserve view namespace/owner in relation catalogs |
| Dynamic native-role checks rejected externally managed Cell replicas | Initialize import eligibility from the configured role even when the native transport is disabled; persisted promotion still wins over startup configuration |
| PostgreSQL oracle re-executed row-returning statements to collect metadata | Describe prepared statements without execution; `returning_once` detects repeated mutations |
| Oracle treated differing wire type OIDs as advisory | Available OID mismatches now fail strict comparisons; unit regression retained |

The constraint reservations deliberately return retryable transaction conflicts
on contention. Foreign-key reservations currently serialize writers referring
to the same parent key; they do not implement PostgreSQL's shared key locks.
JSONB B-tree queries can be slower because their existing physical key format
does not implement SQL JSONB equality/order; JSONB GIN containment remains
independent. Referential cascades fail explicitly beyond 64 levels. A composite
primary-key identity change remains an explicit unsupported operation, rather
than silently applying an incomplete update.

To reproduce strict oracle coverage with a disposable PostgreSQL 18.4 target:

```sh
python3 scripts/platform/generate_correctness_fixtures.py /tmp/bicdb-audit-fixtures --seeds 128
cargo run -p bicdb-cli --features bench --locked -- compat nightmare \
  /tmp/bicdb-audit-db --fixtures /tmp/bicdb-audit-fixtures \
  --pg-host 127.0.0.1 --pg-port "$PG18_HOST_PORT" \
  --pg-database compat --pg-user postgres --pg-password "$PG18_PASSWORD" \
  --json-out /tmp/bicdb-audit-results.json
```

The runner recreates its BicDB scratch path and the fixtures create/drop their
named tables in PostgreSQL: both targets must be disposable test databases.

### Sequence-name compatibility

Older explicit CREATE SEQUENCE statements discarded nonpublic schema qualifiers.
Those existing records are public sequence records: their original intended
namespace cannot be recovered reliably from the stored name. Public records and
existing SERIAL keys remain readable. Before changing an application that relied
on the old alias, inspect its counter/default/ownership bindings and create the
intended qualified sequence with the required current value. The fix preserves
separate namespaces rather than guessing a migration or moving existing values.

### Legacy catalog identities

New table and view row-type identities are durably reserved and do not reuse
an identifier after DROP or rollback. Existing persisted table type identities
are retained. Synthetic identifiers for quoted names now preserve case. Refresh cached
namespace, index and sequence catalog OIDs when upgrading.
`bicdb verify` and the scheduled verifier reject pre-existing duplicate SQL
catalog identifiers; they do not guess which historical composite value or
application-held OID should be reassigned. Resolve a reported legacy collision
from a verified backup and the named object definitions before depending on
that catalog. This check evaluates metadata only and does not execute stored
functions or rewrite user rows.

## Follow-up execution evidence

- The focused platform audit has **24 passing regressions**, including both
  storage modes, constraint races, trigger effects, namespace isolation, catalog
  identity/reopen checks and injected legacy type aliases.
- The main SQL compatibility target has **416 passing tests** after the catalog
  corrections. The PostgreSQL 18.4 oracle has **153/153 passing cases**, **1,397
  statements**, and **zero expected-difference fixtures**.
- All seven declared client drivers passed against BicDB and PostgreSQL 18.4.
  Prisma 6.10.1 produced both expected application models on each target.
  Its smoke requires generated models and can assert the exact expected model set,
  catching internal metadata leaks as well as engine failures with status zero.
- Offline verification rejects injected catalog aliases, and the scheduled
  verifier publishes the failure through pgwire. The scheduled full restore,
  promotion, write and restart test still passes. Five gated paged-recovery CLI
  tests also passed.

The workspace workflow runs every discovered Cargo test target and doctest,
including each target's required feature flags, plus the platform Python and
JavaScript contracts. KVM images, live S3 credentials and optional model/runtime
fixtures remain environment-dependent checks, separate from tests that ran.

### Internal metadata boundary

Prisma exposed a native type-OID ledger as an application model. The SQL collection
filter also omitted namespace, extension and user-type storage. All internal storage
names now share one exact-name filter and cannot be created, read or mutated as SQL
relations, even by the SQL bootstrap role. Mutation authorization rejects hidden and
nonexistent relations before the legacy collection-owner fallback. Table/view/sequence
DDL cannot publish a user schema over those native collections. A regression checks
catalog visibility, SELECT, INSERT, UPDATE, DELETE, COPY, TRUNCATE, DROP, ALTER, GRANT
and CREATE paths for both ordinary and bootstrap roles, verifies metadata is unchanged,
and preserves user relations with the same logical name in a private schema.
