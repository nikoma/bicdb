# Durability failure handling (fail-closed)

Status: implemented for the page log, the page file, checkpoints, the
transaction log (`transactions.log`), the transaction-log cut, commit
outcomes, standby import and the synchronous-quorum confirmation. The
remaining sites in the inventory (section 7) are listed with their current
behaviour and are the scope of a follow-up change.

## 1. Why

When a durability-critical write or `fsync` fails, the state on disk is
unknown. On Linux a failed `fsync` can drop the dirty pages it could not
write and clear the error, so a later `fsync` of the same file can succeed
even though the earlier data never reached the disk. Retrying is therefore
not recovery. Before this change:

- `Wal::sync_through` returned the error to the committer whose sync failed,
  but the log stayed usable. Group-commit waiters retried their own `fsync`,
  and a later successful `fsync` published `durable_lsn` past records whose
  writeback may have failed.
- `transactions.log` repaired a failed append and accepted the next batch on
  the same descriptor.
- A transaction-log cut that failed after renaming the new log over the old
  one returned before `invalidate()`. The writer kept its descriptor on the
  old, unlinked inode, so later commits were acknowledged and then vanished on
  restart.
- A failed directory `fsync` while sealing a page-log segment was ignored.

## 2. State machine

There is one durability state per database handle, shared (one `Arc`) by the
page file, the page log, the transaction log, the database and its
synchronous-quorum tracker
(`bicdb_page::DurabilityHealth`, `crates/bicdb-page/src/health.rs`).

```
                first durability-critical failure
   Healthy ─────────────────────────────────────────▶ WriteFailed { site, error, at }
      ▲                                                     │ (no way back on this handle)
      │                                                     │ close / process exit
      │     open: recovery, then (marker present)           ▼
      └──── verification checkpoint succeeds ◀──────── reopen
                                                            │
                     recovery or verification fails ───────▶ open fails, marker kept
```

- **Healthy.** Normal operation.
- **WriteFailed.** Set by the first failure. The site, the error text and the
  wall-clock time are kept, and later failures do not overwrite them. Nothing
  on the handle clears it. There is no in-place "clear the error and retry".
- At the first failure the database writes `durability-failed.json` (the
  failure as JSON) into its directory, best effort, with a file and directory
  `fsync`. A `durability.write_failed` event (JSON, severity `critical`) goes
  to stderr.
- **Reopen.** A new handle runs normal recovery. If the marker exists, the
  open then runs the verification step: `checkpoint_for_resume` end to end
  (transaction-log flush, page-log sync, page flush and page-file sync,
  checkpoint record, superblock publication, transaction-log cut), then a
  **rewrite** of `transactions.log` (copy to a new file, `fsync`, rename,
  directory `fsync`), and a final health check. The rewrite matters: after a
  failed `fsync`, frames recovery just read may exist only in the page
  cache, clean but never written, and an `fsync` of clean pages writes
  nothing. The copy writes every byte recovery used to a new, synced inode,
  so an OS crash after the reopen cannot open a hole in the log. Only when all of that
  succeeds is the marker removed (followed by a directory `fsync`) and writes
  are accepted. If verification fails, the open fails with `WriteFailed`
  (site `open.verify`) and the marker stays.
- If the marker could not be written (the disk refused it), the next open
  still runs full recovery, which itself writes and syncs the page file and
  truncates the logs' torn tails; only the extra verification checkpoint is
  skipped.

API: `BicDb::durability_state() -> DurabilityState` (`Healthy` or
`WriteFailed(WriteFailure { site, error, at_unix_millis })`). It is never
fenced itself. `PagedStore::durability_health()` and `Wal::health()` expose
the same state at the page layer.

Metrics (pgwire `/metrics`):

- `bicdb_durability_write_failed`: 0 or 1.
- `bicdb_durability_write_failed_timestamp_seconds{site="..."}`: the time of
  the first failure, labelled with its site.

## 3. What WriteFailed refuses

| Operation | Behaviour while WriteFailed |
|---|---|
| Any write path (`ensure_writable`, `begin_transaction`, commits, DDL, checkpoints, backups, log cuts) | `BicDbError::WriteFailed`, SQLSTATE `58030` |
| A commit in flight when the failure happens, including commits waiting on the failed sync | `BicDbError::CommitOutcomeUnknown`, SQLSTATE `08007`; receipt `RecoveryRequired` |
| A commit whose decision was durable before the failure but that completes after it | `CommitOutcomeUnknown` (`08007`). It is never reported committed after the failure. Recovery keeps it. |
| Reads that `check_recovery_state` guards (SQL `execute`, collection state, `get`, `stats`) | Refused with `WriteFailed`. This is the rule the database already used after a failed materialization, extended to I/O failures, so that a fenced handle never serves state whose durability is unknown. |
| Page-layer reads (`PagedStore::get`) | Continue. The page layer cannot know which rows the host considers in doubt; the host fences them. |
| `durability_state()`, metrics | Always available |
| Page log: append, sync, seal, reset | Refused. `durable_lsn` never advances again. |
| Page file: page writes, syncs | Refused. Once the log has failed, "logged implies durable" no longer holds, so a page could reach the file ahead of the only record that redoes it. |
| Transaction log: batch writes, auxiliary frames, flushes | Refused. `written_seq` never advances again. Pending entries stay pending. |
| Replication shipping (`replication_frames_after`) | Continues, but only up to `written_seq`, which is the durable prefix: frames written before the failure. Nothing past it is shipped. |
| Synchronous-quorum confirmation (`SyncQuorum::wait_for`) | Returns `CommitOutcomeUnknown` (`08007`) when the primary is fenced, before and after waiting, whatever the standbys acknowledged. |
| Standby import (`apply_imported_replication_commit`) | A fenced standby imports nothing, and its applied position (what it acknowledges) stops at its durable prefix. Any failure after the import's enqueue fences the standby, because its resident state has run ahead of its log. |

## 4. Transaction outcome semantics

| Case | Error to the caller | Receipt | After reopen and recovery |
|---|---|---|---|
| `transactions.log` write fails before any byte is written | `CommitOutcomeUnknown` (`08007`) | `RecoveryRequired` | Absent |
| `transactions.log` write lands but its O_DSYNC sync fails; tail repair succeeds | `08007` | `RecoveryRequired` | Absent (the repair truncated it) |
| As above, and the tail repair fails too | `08007` | `RecoveryRequired` | **Present.** The decision reached the disk; recovery honours it. |
| Decision durable in `transactions.log`; page-log commit sync or materialization fails afterwards | `08007` | `RecoveryRequired` | **Present.** Recovery replays the durable decision. |
| Commits waiting on a failing group sync (page log or transaction log) | `08007` for each | `RecoveryRequired` | Whatever reached the disk; never a commit acknowledged after the failure |
| Commit or transaction operation that fails before its decision on a database fenced meanwhile (for example by reading a row whose failed commit left a resident version the page store never committed) | `WriteFailed` (`58030`), never the incidental error | `Aborted` | Absent |
| Commit attempted after the failure | `WriteFailed` (`58030`) | `Aborted` / `Pending` (no decision was made) | Absent |
| A fence lands after the commit was durable and visible (for example its record-audit publication marker fails) | **Success.** The commit is not ambiguous, and a `58030` would make the client retry and duplicate it. The failure is logged (`durability.audit_publication_deferred`) and the database is fenced for everything after. | `Committed` | Present; recovery republishes its audit entries |
| A page-store-only commit (bulk load, index and full-text build steps) whose page-log commit fails | `08007` without a commit sequence (the page-log commit record is the decision and may be on disk) | — | All or nothing; resolve by re-reading the result |
| The backup write gate times out before the commit's bytes are written (a backup or branch held it) | The ordinary `Backup` error: the commit is withdrawn (its queued bytes become an empty slot, so no later batch can write it), so it definitively did not commit, and a retry is safe. **No fence.** | `Aborted` | Absent |
| Commit acknowledged before the failure | (was `Ok`) | `Committed` | Present (reconciled by the crash and fault matrices) |

"Outcome unknown" means: do not assume the commit committed, and do not
assume it rolled back. Recovery decides, and it keeps the commit exactly when
its decision record reached the disk. Nothing retries an ambiguous commit or
reinterprets it as rolled back.

### 4.1 Resolving an unknown outcome by identity

Every `08007` carries the identity needed to resolve it
(`crates/bicdb-core/src/db/commit_resolution.rs`):

| Part | Meaning |
|---|---|
| `incarnation` | Random id of the database handle (one per open), `BicDb::incarnation_id()` |
| `commit_seq` | The commit sequence the transaction reserved. Unique within an incarnation. `none` for page-store-only operations (bulk load, index builds), which are resolved by re-reading their result. |
| `transaction_id` | The transaction id (the receipt's `transaction_id()`). Absent for a quorum-wait `08007`, which knows only the sequence. |

Where it appears:

- **Embedded:** the fields of `BicDbError::CommitOutcomeUnknown`. The
  transaction's receipt holds `RecoveryRequired { sequence }` for the same
  `commit_seq`, and `observe_outcome().transaction_id()` for the same
  transaction id.
- **pgwire / SQL:** the error DETAIL is
  `commit_seq=<n|none> transaction_id=<t|none> incarnation=<uuid>`, and the
  HINT names the resolving statement.

Commit sequences and transaction ids are recovered from the log at open, so a
later incarnation can reuse the numbers of a commit that never reached the
disk; the incarnation is what makes the identity unique. Every open appends
`{incarnation, opened_at, recovered_through_seq}` to `incarnations.log`, with
`fsync`, after recovery and before the database accepts any commit. A failure
to write it fails the open. `recovered_through_seq` is the contiguous durable
prefix that recovery rebuilt.

Resolution of `(incarnation I, commit_seq N)` uses the first open after `I`:

- `N <= recovered_through_seq`: **committed.** Recovery kept it, and within
  `I` only that transaction held `N`.
- Otherwise: **not committed.**
- `I` is still the open handle, no later open is recorded, or `I`'s record
  was pruned (the journal keeps about 2,000 opens): **unresolved**. It never
  guesses.

The answer is stable: later commits that reuse `N` do not change it.

How to ask:

- Embedded: `BicDb::resolve_commit_outcome(incarnation, commit_seq)` returns
  `CommitResolution::{Committed, NotCommitted, Unresolved}`. It is not
  fenced.
- SQL (superuser, like every `bicdb_` administrative function):
  `SELECT bicdb_commit_outcome('<incarnation>', <commit_seq>)` returns
  `committed`, `not_committed` or `unresolved`.

**After a failover**, the promoted standby's history is the authoritative
one. A fenced primary ships only its durable prefix, so an uncertain commit
is never part of the standby's history: it did not commit in the cluster. The
old primary's own journal describes only its own disk, which may hold the
uncertain commit (a decision that reached the disk after a failed sync). Such
a primary has a divergent tail. The promoted primary detects it
(`standby_resume` answers `Snapshot`), and the old primary must be rebuilt
from a snapshot or bootstrap, never reopened as a primary. Resolve uncertain
commits against the promoted primary's history: they are not committed.

## 5. Error codes

| Error | SQLSTATE | Meaning |
|---|---|---|
| `BicDbError::WriteFailed { site, detail }` | `58030` (io_error) | The database is fenced by a durability failure. Reopen it. |
| `BicDbError::CommitOutcomeUnknown { sequence, transaction_id, incarnation, detail }` | `08007` (transaction_resolution_unknown) | This commit's outcome is unknown. Reopen; recovery decides; resolve by identity (section 4.1). DETAIL and HINT carry the identity. |
| `PageError::WriteFailed { site, detail }` | (page layer) | The page store is fenced; mapped to `BicDbError::WriteFailed`. |

`BicDbError::is_durability_failure()` is true for both database errors. The
same mapping is used by the SQL engine and by pgwire.

## 6. Operator procedure

1. **Detect.** Alert on `bicdb_durability_write_failed == 1`, on SQLSTATE
   `58030` or `08007`, or on the `durability.write_failed` event. The `site`
   names what failed (section 8).
2. **Stop and inspect.** Look at the device: kernel log (`dmesg` for I/O
   errors), free space, filesystem state, controller health. Do not keep the
   fenced process running in the hope that it recovers; it cannot.
3. **Fix the cause.** Replace or remount the device, free space, and so on.
   If the device is not trustworthy, fail over to a standby instead (the
   standby holds every commit the primary shipped, which is exactly its
   durable prefix) and rebuild this node from a backup or a standby
   bootstrap.
4. **Reopen.** Restart the server or reopen the database. Recovery replays
   the logs. Because `durability-failed.json` is present, the open runs the
   verification checkpoint and removes the marker only if it succeeds. Look
   for the `durability.verify_started` and `durability.verified` events.
5. **If the open fails** with `open.verify` or a recovery error, the marker
   stays and the database stays closed. Repeat steps 2 to 4. Do not delete
   the marker by hand to get past a failing verification: the marker is what
   makes the next open prove the disk works end to end.
6. **Reconcile clients.** Every client that received `08007` resolves it
   after the reopen with `bicdb_commit_outcome(incarnation, commit_seq)`
   from the error's DETAIL (section 4.1), and acts on `committed` or
   `not_committed`. After a failover, see the end of section 4.1. Clients
   that received `58030` made no decision and can retry.

## 7. Inventory of durability-critical write and sync sites

Columns: what the caller was told before this change; whether a later
operation could still acknowledge writes after the failure (before / now);
what recovery does. Line numbers are at the time of writing.

### 7.1 Fail-closed in this change

| Site (recorded name) | Code | Caller told before | Later acks before → now | Recovery |
|---|---|---|---|---|
| Page-log append (`page_log.write`) | `Wal::append`, `crates/bicdb-page/src/wal.rs:593` | Error | Yes: next append followed a torn record → No | Torn tail truncated; stops at the first bad record |
| Page-log sync (`page_log.sync`) | `Wal::sync_through`, `wal.rs:772` | Error to the syncer; **waiters retried their own fsync** | **Yes** (the qualification finding) → No; waiters fail; `durable_lsn` frozen | Replays committed images after the redo point |
| Segment seal sync / rename / directory sync (`page_log.seal_sync`, `page_log.seal_rename`, `page_log.seal_dir_sync`) | `Wal::seal_active_locked`, `wal.rs:523` | Error for sync and rename; **directory sync ignored** | Yes → No | Sealed segments discovered and replayed in order |
| Log reset at checkpoint (`page_log.reset`) | `Wal::reset`, `wal.rs:1058` | Error | Yes → No | A sealed segment left behind would be replayed over newer pages; now fenced |
| Checkpoint record | `Wal::checkpoint`, `wal.rs:856` (append + sync) | Error | Yes → No | Redo point from the superblock horizon |
| `finish_checkpoint` | `wal.rs:1524` (log sync, page flush, record, `publish_root`, reset) | Error | Yes → No | As above |
| Page-file sync (`page_file.sync`) | `PageStore::sync_data_observed`, `crates/bicdb-page/src/manager.rs:2466`; used by `flush`, `publish_root` (superblock), `flush_all`, tail reclaim | Error; dirty set kept for a retry | **Yes** (a retried sync could succeed and let the checkpoint truncate the log) → No | Replays the log from the redo point |
| Buffer-pool writeback and barrier | `BufferPool::write_back_frame` / `flush_all`, `crates/bicdb-page/src/pool.rs:1661`, `:1694`; barrier `WalWritebackBarrier`, `paged.rs` | Error; frame stays dirty | Yes → No (the barrier's log sync and the page write both refuse) | — |
| Transaction-log batch write (`transaction_log.write`) | `TxLogHandle::flush_buffer` → `storage::write_raw` (O_DSYNC), `crates/bicdb-core/src/db.rs:2699`, `storage.rs:408` | Error; tail repaired; entries kept pending | **Yes** (next committer retried the batch) → No; waiters fail; `written_seq` frozen | Frames read to the first bad checksum |
| Transaction-log auxiliary frames (`transaction_log.auxiliary`) | `TxLogHandle::append_auxiliary`, `db.rs:2823` | Error; poisoned only if the repair failed | Yes → No | Markers are an optimization; unmarked commits replay |
| Transaction-log flush before checkpoint/cut | `TxLogHandle::flush_pending`, `db.rs:2804` | Error | Yes → No | — |
| Commit decision (`write_durable`) | `commit_transaction_inner`, `crates/bicdb-core/src/db/spatial_paged.rs:2268` | Error (`XX000`), `recovery_required` set | No (the existing fence) → No, with site, `08007`, marker, verification | Decision replayed if on disk |
| Paged commit / abort / materialization | `in_paged_transaction_inner`, `spatial_paged.rs:1491` | Error, `recovery_required` set | No → No | Durable decision replayed |
| Transaction-log cut, commit point (`transaction_log.cut`; injection `transaction_log.cut_rename`, `transaction_log.cut_sync`) | `replace_log_after_cut` from `truncate_log_file` / `copy_transaction_log_tail`, `db.rs:18767-18813`; `cut_transaction_log`, `db/replication_live.rs:312` | Error; **stale descriptor kept** | **Yes, to an unlinked inode** → No | Reopen reads whichever log the directory holds |
| Standby import | `apply_imported_replication_commit`, `crates/bicdb-core/src/db/commit_snapshot.rs:185` | Error, no fence; resident state applied ahead of the log | Yes (a retry re-applied) → No | Reopen replays what reached the log |
| Standby import, journal-event and audit-publication appends after the durable write | same function, after `write_durable` | Error with `commit_seq` not advanced, so a retry re-applied a durable commit | Yes → No (fenced; covered by code, not by a dedicated test, because it needs record-journal replication) | Reopen recovers the commit and republishes its journal |
| Online extent migration copy sync (`page_file.sync`) | `PageStore::advance_extent_migration`, `crates/bicdb-page/src/manager.rs` | Plain error. The sync used the same segment descriptors the checkpoint syncs, so it consumed the writeback error, and the next checkpoint's sync succeeded and truncated the log over pages that may have been lost. | **Yes** → No | — |
| Deferred materializer (experimental transaction-log-only mode) | `DeferredMaterializer::materialize`, `db/deferred_materialize.rs:104` | Error; batch requeued for a later sync | **Yes** (a later sync could succeed) → No (the shared log health refuses) | Unmarked commits replay |
| Synchronous quorum | `SyncQuorum::wait_for`, `db/replication_live.rs:680` | Confirmation from standby acks only | Yes → No (`08007` once fenced) | — |

### 7.2 Not fatal by design

| Site | Why it does not fence |
|---|---|
| Page-file page write (`page_file.write`, `write_page_raw`, `manager.rs:2398`) | The frame stays dirty and the page log still holds its redo, so the write is retried; only a failed page-file sync fences. A short or torn write is also caught by the page checksum. |
| Backup write-gate timeout (`backup_gate.timeout`) | Nothing was written. The transaction log takes the gate before the append and fences only on errors from the write and the repair; the commit is withdrawn and reports the ordinary `Backup` error. |
| Superblock write in `publish_root` | Same as a page write. A failed publication leaves the older checkpoint LSN in the superblock, and recovery then replays from the start of the log, which is idempotent. |
| Opening the transaction-log handle | Nothing is written, so there is nothing whose durability is unknown. The commit stays pending and is retried. |
| Tail copy before the rename in a cut (`transaction_log.cut_copy`) | The live log is untouched until the rename; the checkpoint fails and is retried later. |
| `commit-seq.floor` (`persist_commit_seq_floor`, `db.rs:18721`) | Written before any truncation, and its values only rise; a failure stops the cut before it changes the log. |
| Recovery replay (`page_file.write` or `page_file.sync` during open) | The open fails and the log is left intact; the next open replays it again. |

### 7.3 Remaining sites (follow-up; behaviour unchanged here)

| Site | Code | Caller told | Later acks possible? | Recovery |
|---|---|---|---|---|
| `sync.log` appends | `SyncLog::append_ops` / `mark_synced`, `crates/bicdb-core/src/sync.rs:103`, `:142` → `storage::append_frames_with_sync_mode`, `storage.rs:231` | Error before the commit sequence is reserved (the commit aborts) | Yes. No torn-tail repair and no poison: the next append lands after a torn prefix. | `read_frames` truncates at the first bad frame, which also drops every later frame reported as written |
| Other generic frame appends (event segments, record-segment deltas, open-time audit publication) | `storage::append_frames*`, callers `event.rs:536`, `db/commit_snapshot.rs` (record segments), `db.rs` (audit publication at open) | Error | Yes (same as `sync.log`) | Same as `sync.log` |
| Sidecars: catalog, index catalog, planner stats, model registry, memory index | `storage::write_atomic` (`storage.rs:1307`), `write_frame_atomic` (`:1336`), `write_sidecar` (`db.rs:20698`) | Error. If the directory sync fails after the rename, the new file is already live while memory is rolled back. | Yes | Whatever file is in the directory |
| HA state, consensus state | `persist_ha_state` (`db.rs:18400`), `persist_consensus_state` (`db.rs:18480`) | Error, except `let _ = persist_ha_state(...)` on an apply-error path (`db.rs:18563`) | Yes | No directory `fsync` after the rename |
| HA bootstrap install | `db/ha_bootstrap.rs:184` | Directory sync ignored | — | Crash ordering after the ignored sync is not guaranteed |
| Commit-log archive | `LogArchive::append` / `sync` / `finish_segment`, `crates/bicdb-core/src/log_archive.rs:209`, `:324`, `:333` | Error; poisoned only if the rollback fails | Yes. `archive_commits` retries `sync()`, so a retried `fsync` can falsely succeed. | `open` truncates the torn tail |
| Page-WAL archive | `archive_wal_segments`, `db/spatial_paged.rs:1755`; directory sync ignored at `:1825` | Copy, sync and rename errors propagated | `mark_wal_archived_through` runs even when the directory sync failed, so local segments can be released before the archive is durable | — |
| FTS segment manifest | `crates/bicdb-core/src/fts_segment.rs:1867` | Directory sync ignored | — | Manifest re-read at open |
| Page-store relayout / migration | `crates/bicdb-page/src/manager.rs:2823` | Directory sync ignored | Offline tool | — |

## 8. Site names and fault injection

The recorded `site` and the injection name are the same strings.

| Injection site | Where |
|---|---|
| `page_log.write`, `page_log.sync` | `Wal::append`, `Wal::sync_through` |
| `page_log.seal_sync`, `page_log.seal_dir_sync` | `Wal::seal_active_locked` (rename failures record `page_log.seal_rename`) |
| `page_log.reset` | `Wal::reset` (truncate; sealed-segment removal records the same site) |
| `page_file.write` | `PageStore::write_page_raw` (not fatal) |
| `page_file.sync` | `PageStore::sync_data_observed` |
| `transaction_log.write` | `storage::write_raw`, before the write |
| `transaction_log.sync` | `storage::write_raw`, after the bytes were written (an O_DSYNC write whose sync failed) |
| `transaction_log.repair` | the tail repair after a failed append |
| `transaction_log.cut_rename`, `transaction_log.cut_sync` | `replace_log_after_cut` (recorded as `transaction_log.cut`) |
| `transaction_log.cut_copy` | `copy_transaction_log_tail`, before the rename (not fatal) |
| `page_file.sync` (migration) | `PageStore::advance_extent_migration` copy sync |
| `backup_gate.timeout` | the backup write-gate wait reports its timeout at once (not fatal) |
| `transaction_log.cut_window` | hook point only (`arm_io_hook`), between the cut's flush and its rename |

Other recorded sites: `transaction_log.auxiliary`, `paged.commit`,
`paged.abort`, `commit.materialize`, `standby.import`, `open.verify`.

Injection (`crates/bicdb-page/src/test_faults.rs`):

- `BICDB_TEST_FAIL_IO=<site>:<n>[+][,<site>:<n>[+]...]`: the `n`-th call
  (1-based, process-wide) of the site returns `EIO` instead of doing the
  I/O. With a trailing `+`, every later call fails too (a dead disk);
  without it, only the `n`-th call fails, which is the "failed once, then
  succeeded" pattern this design exists for.
- `test_faults::arm_io_failure(site, scope_dir, n, persistent)`: the same
  in process, counting only paths under `scope_dir`, so parallel tests do
  not interfere. The returned guard reports `fired()` and `seen()` and
  disarms on drop.
- `test_faults::arm_io_hook(site, scope_dir, n, hook)`: runs `hook` at the
  `n`-th call and then performs the I/O, so a test can make something happen
  while an I/O is in flight (a failure elsewhere during a successful
  `fsync`, a fence during a log cut).
- With nothing armed, an injection point is one atomic load.
- `BICDB_TEST_FAIL_IO` is a test-only setting that works in any build. When
  it is set, a `test_faults.io_failure_injection_enabled` event (severity
  `critical`) is logged at the first I/O, and a malformed entry is refused
  with a panic naming it, instead of silently arming nothing.

## 9. Qualification

| Test | What it proves |
|---|---|
| `bicdb-page` `fail_closed_io::a_failed_log_sync_is_sticky_and_no_later_sync_publishes_durability` | Regression for the qualification finding: after a one-shot `fsync` failure, no later sync is attempted or published, no append is accepted, `durable_lsn` is frozen, and recovery yields a consistent log |
| `fail_closed_io::a_failed_log_write_fences_the_log` | A failed append fences the log |
| `fail_closed_io::concurrent_commits_waiting_on_a_failing_sync_all_fail` | 8 and 32 group-commit waiters on a one-shot failing sync all fail; exactly one `fsync` is attempted |
| `fail_closed_io::a_failed_commit_sync_fences_the_paged_store_until_reopen` | The failed commit is invisible, page reads continue, every write path refuses, reopen recovers every acknowledged row and resumes writes |
| `fail_closed_io::a_failure_in_each_checkpoint_phase_fences_and_recovers` | Log sync, page-file sync, log append and log reset during a checkpoint, monolithic and segmented |
| `fail_closed_io::a_failed_segment_seal_fences_the_log` | Seal sync and seal directory sync (the latter used to be ignored) |
| `fail_closed_io::a_failed_page_write_is_retryable_and_not_a_durability_failure` | The deliberate non-fatal case |
| `fail_closed_io::a_failure_during_an_in_flight_sync_publishes_no_durability` | A failure recorded while an `fsync` is in flight (hook): the `fsync` that then succeeds publishes nothing |
| `fail_closed_io::a_failed_sync_during_online_extent_migration_fences_the_store` | The migration copy sync fences, and the checkpoint that would truncate the log is refused |
| `fail_closed_io::a_failure_during_recovery_replay_fails_the_open_and_a_retry_recovers` | Recovery replay failures |
| `bicdb-core` `durability_fail_closed::a_failed_transaction_log_write_fences_and_reports_an_unknown_outcome` | Embedded and paged: `08007`, receipt, fencing of writes and reads, marker, no retry, reopen and verification |
| `durability_fail_closed::an_unknown_outcome_resolves_to_what_reached_the_disk` | Sync failure with and without a failed tail repair: absent and present after recovery respectively |
| `durability_fail_closed::a_durable_decision_whose_page_commit_fails_is_kept_by_recovery` | A durable decision is honoured |
| `durability_fail_closed::concurrent_commits_around_a_failing_write_never_acknowledge_after_it` | 8 and 32 writers, embedded and paged: no writer sees `Ok` after its first error, every error is a durability failure, no write is attempted after the failure, every acknowledged commit survives |
| `durability_fail_closed::a_failed_checkpoint_fences_and_the_reopen_verifies` | Page-file sync, page-log sync and log reset inside `checkpoint_for_resume` |
| `durability_fail_closed::a_failed_log_cut_fences_after_the_rename_and_not_before` | The stale-descriptor cut bug; the copy failure before the rename stays retryable |
| `durability_fail_closed::a_fence_after_a_durable_commit_does_not_report_the_commit_as_refused` | Publication-marker failure after the commit: success, fenced afterwards, present after reopen |
| `durability_fail_closed::a_failed_bulk_load_commit_reports_an_unknown_outcome` | Page-store-only commit failure: `08007` without a sequence, batch all or nothing |
| `durability_fail_closed::a_backup_gate_timeout_does_not_fence` | Gate timeout: the commit is withdrawn (`Aborted`, absent), no fence, later commits visible |
| `durability_fail_closed::verification_rewrites_the_transaction_log` | The verifying open rewrites the log to a new inode; a normal open does not |
| `durability_fail_closed::a_fence_during_a_log_cut_stops_the_rename` | A fence in the cut window (hook) stops the rename |
| `durability_fail_closed::a_reopen_whose_verification_fails_stays_fenced` | Verification is required, and its failure keeps the marker |
| `durability_fail_closed::a_fenced_primary_ships_only_its_durable_prefix_and_the_standby_keeps_working` | Shipping stops at the durable prefix, the quorum confirms nothing, the standby stays healthy, and replication resumes after the primary's reopen |
| `durability_fail_closed::an_unknown_commit_resolves_by_identity_to_what_recovery_kept` | Six fault cases (embedded and paged): the identity in the error matches the receipt and the transaction; the failed handle answers `Unresolved`; after the reopen the answer matches what recovery kept, and stays the same after later commits reuse the sequence |
| `durability_fail_closed::a_primary_that_fails_mid_stream_is_replaced_by_a_promoted_standby` | The uncertain commit is on the old primary's disk but never shipped; the promoted standby does not have it and takes writes; the old primary's divergent tail is detected (`Snapshot`) |
| `bicdb-sql` `commit_outcome_resolution` | 08007 DETAIL/HINT over SQL, 58030 afterwards, `bicdb_commit_outcome` after the reopen agrees with the table |
| `commit_resolution` unit tests | Next-open rule, `Unresolved` cases, torn last record, pruning |
| `commit_durability_crash::repeat_core_matrices` (ignored; `DURABILITY_ROUNDS`, `DURABILITY_WORKERS`) | Merge-gate repetition of the fail-closed and crash matrices |
| `durability_fail_closed::a_failing_standby_is_fenced_and_catches_up_after_reopen` | Standby-side failure: its applied position stops, and it catches up after reopen |
| `commit_durability_crash::*_fails_closed_at_every_io_failure_site*` | Out-of-process matrix with the production path (with and without eviction) and the transaction-log-only path, using `examples/durability_child.rs`: 8 failure specs each. The child reports any acknowledgment after a failure and any non-durability error; the parent requires the marker, then reconciles against the acknowledged-commit ledger twice (every acknowledged commit present, all-or-nothing commits, index equals scan, no resurrected generation, identical second reopen) |
| `commit_durability_crash` crash matrices (unchanged) | No regression in crash recovery |
| `db::tx_log_write_failure_tests::partial_append_retry_keeps_every_acknowledged_frame_recoverable` | Updated: a real kernel short write (`RLIMIT_FSIZE`) is repaired, the failed handle refuses to retry, and a new handle (the reopen) appends cleanly over the repaired tail |
| pgwire and SQL `durability_sqlstate_tests` | `58030` and `08007` mappings |

Mutation checks (each mutation applied alone; the named tests must fail):

| Mutation | Caught by |
|---|---|
| The page log's sync loop no longer checks the health state (waiters retry, a later sync publishes) | `a_failed_log_sync_is_sticky_and_no_later_sync_publishes_durability`, `a_failed_log_write_fences_the_log`, `concurrent_commits_waiting_on_a_failing_sync_all_fail` |
| The transaction log's `write_durable` and `flush_buffer` no longer check the health state | `concurrent_commits_around_a_failing_write_never_acknowledge_after_it` (writes after the failure: 31 instead of 25) |
| The cut no longer fences after a failed rename or sync | `a_failed_log_cut_fences_after_the_rename_and_not_before` |

## 10. Known gaps (outside this change)

- **Partial page-log reset.** If removing the sealed segments fails part way
  through `Wal::reset`, or the process crashes there, the next recovery can
  lose checkpointed pages. Fencing stops this handle, but it does **not**
  make the on-disk state a partial reset leaves safe. Until the
  recovery-side fix (follow-up 1, "GAP-2") lands, treat a `page_log.reset`
  failure as needing that fix or a restore. It is scheduled to land
  directly after this change.
- **Reopening mid-migration.** A store whose online extent migration
  stopped before it had created every segment its page count implies does
  not reopen (`SegmentMissing`). This happens whether the migration step
  failed (now a fence) or crashed. It is an existing limitation of online
  migration, found while writing the migration fence test, and is tracked
  separately.

- **A recovery that refuses to open after an early stop.** About 1 in
  several hundred repeated runs leaves a state that recovery refuses to open
  with `page N is beyond the end of the file (N pages)`, both after a crash
  (`pool.after_writeback:40`) and after an injected early page-log sync
  failure. The binary built from `main`, without this change, refuses the
  same saved states identically. The saved crash state shows the cause: the
  log ends with page images of an uncommitted transaction that allocated
  pages 6 and 7, and the eviction just before the crash wrote one of its
  structural pages (which already references page 6) to the page file.
  Recovery skips the uncommitted images, so the stolen page points past the
  published page count. This is the "steal without undo" gap documented at
  `WalWritebackBarrier` in `crates/bicdb-page/src/paged.rs`, an existing
  recovery defect. It fails closed (the open is refused; nothing is silently
  discarded). The states are saved for the recovery work.

## 11. Durability-contract notes

This change does not alter what a successful commit guarantees, when a
commit is acknowledged in the healthy state, or what replication ships while
healthy. It only stops acknowledgments after a failure and reports the
affected outcomes as unknown. Points the reviewer should confirm:

1. **Quorum confirmation after a fence.** `SyncQuorum::wait_for` returns
   `08007` once the primary is fenced, even when standbys acknowledged the
   sequence. This is the fail-closed rule applied to the confirmation; it does
   not change how acknowledgments are counted.
2. **Strict post-failure rule.** A commit whose decision was durable before
   the failure but that reaches its acknowledgment after it gets `08007`
   rather than success. Recovery keeps it. This is conservative reporting,
   not a change in what is durable.
3. **Reads.** Reads that the existing recovery fence guarded are refused on
   a fenced handle, as they already were after a failed materialization.

All three were approved by the owner as proposed. Recovery-dependent reads
stay fenced for this release; any future safe-read exception needs separate
qualification. The incarnation journal adds one `fsync`ed append per open,
never on the commit path.

Not implemented, because each changes the durability contract or adds
synchronous I/O to a hot path, and needs a decision first:

- Making the page-log reset durable (an `fsync` after the truncation and a
  directory `fsync` after removing sealed segments).
- Adding the missing directory `fsync` to HA and consensus state files, and
  failing closed on the sidecar directory syncs.
- A torn-tail repair and poison for the generic frame appender
  (`sync.log`, event segments), and fail-closed handling of the commit-log
  archive's retried `sync()` and the page-WAL archive's ignored directory
  sync before `mark_wal_archived_through`.
