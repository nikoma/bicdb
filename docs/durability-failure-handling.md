# Durability failure handling (fail-closed)

Status: implemented for the page log, the page file, checkpoints, the
transaction log (`transactions.log`), the transaction-log cut, commit
outcomes, standby import and the synchronous-quorum confirmation, the
generic frame logs (`sync.log`, the event log, record segments; section
7.4), and the HA and consensus state files, the collection and index
catalogs, the other database sidecars and the standby bootstrap install
(section 7.5). The remaining sites in the inventory (section 7.3) are listed
with their current behaviour and are the scope of follow-up changes.

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
| Log reset at checkpoint (`page_log.reset_unlink`, `page_log.reset_dir_sync`, `page_log.reset`, `page_log.reset_sync`) | `Wal::reset` | Error | Yes → No | Crash-ordered (`docs/recovery-semantics.md`, GAP-2): sealed segments are deleted oldest first and the directory synced before the active file, which holds the checkpoint record, is truncated and synced. A failure or crash at any step leaves a chain that still reaches the checkpoint record, or an empty log. A stale chain left by an older engine is recognised at recovery and the reset finished |
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
| A failure up to and including the rename of `ha-node.json`, `consensus-state.json`, the pending catalog, the model registry or the memory-index files (`<file>.write`, `.sync`, `.rename`) | The old file is intact and memory is unchanged (consensus memory is put back), so nothing is uncertain. The operation fails and is not acknowledged: no vote is granted, no fence or promotion reported. A primary that cannot persist its fence has already closed its quorum and refuses writes. |
| `ha-state.json` (file-shipping HA) | No open handle owns it. The promotion or apply fails and is not acknowledged; the in-progress record keeps an unfinished apply from being promoted without `force`. |
| `planner-stats.json` (`planner_stats.*`), `index-maintenance.json` (`index_maintenance.*`) | Derived data: statistics only steer plans and ANALYZE rebuilds them; build status and sizes are rewritten by the next build, rebuild or verification. |
| `replication-state.json` | Descriptive; the applied position is durable in the transaction log. |

### 7.3 Remaining sites (follow-up; behaviour unchanged here)

The generic frame logs (`sync.log`, event segments, record-segment deltas,
the open-time audit publication) were listed here; section 7.4 covers them.

| Site | Code | Caller told | Later acks possible? | Recovery |
|---|---|---|---|---|
| Commit-log archive | `LogArchive::append` / `sync` / `finish_segment`, `crates/bicdb-core/src/log_archive.rs:209`, `:324`, `:333` | Error; poisoned only if the rollback fails | Yes. `archive_commits` retries `sync()`, so a retried `fsync` can falsely succeed. | `open` truncates the torn tail |
| Page-WAL archive | `archive_wal_segments`, `db/spatial_paged.rs:1755`; directory sync ignored at `:1825` | Copy, sync and rename errors propagated | `mark_wal_archived_through` runs even when the directory sync failed, so local segments can be released before the archive is durable | — |
| FTS segment manifest | `crates/bicdb-core/src/fts_segment.rs:1867` | Directory sync ignored | — | Manifest re-read at open |
| Page-store relayout / migration | `crates/bicdb-page/src/manager.rs:2823` | Directory sync ignored | Offline tool | — |

### 7.4 Generic frame logs: tail repair, poisoning and mid-file damage

Every file written by `storage::append_frames*` (the "generic frame
appender", `crates/bicdb-core/src/storage.rs:231`) and read back by the
recovery readers `read_frames`, `stream_frames` and `read_frames_from`.

**Inventory**

| User | Writer | Reader at open | Acknowledged when | Before this change | Now |
|---|---|---|---|---|---|
| `sync.log`, the CDC outbox | `SyncLog::append_ops` (`sync.rs:113`), called before the commit decision (`db/spatial_paged.rs:2444`); `SyncLog::mark_synced` (`sync.rs:156`) | `read_frames` in `SyncLog::open` | An op: its commit returns `Ok`. An ack: `mark_synced` returns `Ok`. | A failed or short append left a torn prefix and the log stayed usable. The next commit's ops landed after it, and the reopen truncated at the torn frame, dropping them (measured: 6 acknowledged ops lost, and 3 complete frames of the *aborted* commit reported as pending ops). A failed `mark_synced` likewise lost the next acknowledgment. | Tail cut back; the commit aborts with an ordinary error; nothing of it stays in the outbox. On a failed cut-back: poisoned, fenced (`frame_log.repair`), and the reopen cuts to the recorded acknowledged length. |
| `events/events.seg`, the event log (user events, broker, geofence and spatial events, the record-audit outbox) | `EventStream::append_event` (`event.rs:540`) | `stream_frames` in `EventStream::open_with_encryption` | The append returns `Ok`. | Same failure: every later event lost at the reopen (measured: 5 of 5). | Same repair and poisoning. |
| Record-audit publication behind a commit | `Transaction::finalize_committed_record_audit_events` (`db.rs`): event-log appends, then a `RecordAuditPublished` marker in the transaction log | Transaction-log recovery republishes unmarked outboxes | The commit's decision was already durable. | The commit returned an ordinary error and the handle stayed usable. A checkpoint on that handle then cut the transaction log past the unpublished outbox, and the committed row's audit event was lost for good (measured). | Fences (`record_audit.publish`). The commit is durable and visible, so it is reported as committed, with the failure logged, as for a failed publication marker (section 4). The fenced handle refuses the checkpoint, and the reopen republishes the outbox. The standby journal import already fences (`standby.import`). |
| `segments/<collection>.seg`, record segments (embedded storage) | Checkpoint delta append, `checkpoint_write_segments` (`db/commit_snapshot.rs:2419`) | `read_frames` / `read_frames_from` (snapshot tail) at open | The checkpoint succeeds, then cuts the transaction log. | A failed delta append left a torn prefix; the retried checkpoint appended after it and cut the transaction log; the reopen truncated at the torn frame (measured: acknowledged rows lost). | Same repair and poisoning; a failed cut-back fences. |
| `transactions.log`, open-time audit publication markers | `append_frames` at open (`db.rs:10393`) | `stream_frames` in `recover_transaction_log` (`db.rs:19134`) | Markers only; republishing is idempotent. | No repair; a failure fails the open, and the next open truncates the torn tail. | Same repair; a failed cut-back leaves the marker file, which the next open applies. |
| Atomic rewrites: `rewrite_frame_file`, `rewrite_frame_file_iter`, `rewrite_record_segment_streamed` (`db.rs:18225-18306`), `write_frame_atomic` | Append into a fresh temporary file, then rename | — | After the rename. | A failed append abandoned the temporary file; the live file was untouched. | Unchanged in effect (the temporary file is cut back or abandoned). |
| Binary snapshot `records.snap` | Snapshot publication | `read_frames` in `snapshot.rs:415` | — (a cache of the segments) | — | Mid-file damage now reads as "snapshot unusable", and the open falls back to the segments. |
| `transactions.log`, live readers for replication | — | `replication_retention_status` (`db.rs:11199`) and `replication_commits_from_retained_wal` (`db.rs:11421`, behind `export_replication_frames_since`) | — | **Used the recovery readers on the live log.** A commit mid-write looked like a torn tail, was truncated under the writer, and the writer then acknowledged it. | Read-only readers (`stream_frames_live`, `read_frames_readonly_from`) that stop before an incomplete frame and never truncate. |

Not users of the generic appender, and unchanged: the commit-log archive
(`log_archive.rs`, its own format and repair, section 7.3) and the cluster
backup journal (`distribution_backup_run.rs`).

**Failed append (writer side).** `append_frames_with_sync_mode` now goes
through the transaction log's `write_raw_with_repair`. Any failure (an error
before writing, a short write, or a failed sync after a complete write)
cuts the file back to its length before the batch, and syncs that cut when
the append was durable. Nothing in the batch was acknowledged, so nothing
acknowledged is removed, and the next append starts on a frame boundary.
The error is ordinary (retryable), not a fence: every earlier
acknowledged append was synced before it was acknowledged, so a failed
`fsync` cannot have dropped acknowledged data in these files, and the
failed bytes no longer exist. The database-level consequences are handled
by each caller: a `sync.log` failure aborts its commit before the decision;
an audit-publication failure fences (above); a segment failure fails the
checkpoint, which retries.

**Failed cut-back.** When the cut-back fails too, the file has bytes of
unknown status after its last acknowledged frame, possibly including
complete frames of the failed batch. Then:

1. The path is poisoned in this process: every later append to it is
   refused with `WriteFailed { site: "frame_log.repair" }` and writes
   nothing, so no acknowledged frame is ever placed behind the damage. The
   generic appender opens the file per call, so the poison set is
   process-wide, keyed by path; a recovery reader clears it.
2. `<file>.append-failed` records the length before the failed append
   (best effort: the disk is already failing). The next open's recovery
   reader cuts the file to that length before reading, so complete frames
   of the failed batch are not accepted. A `sync.log` append belongs to a
   commit that aborted, and its ops must not reach a CDC consumer. While the
   marker exists, the appender refuses that file in any process, which
   covers a log that the open does not read (a disabled `sync.log`).
3. `SyncLog`, `EventStream` and the checkpoint's segment write record the
   failure in the database's `DurabilityHealth`, so the database is fenced
   and the reopen runs verification (section 6).

**Recovery (reader side).** A recovery walk ends at the first frame that is
incomplete, has an unknown header, the wrong kind, an oversized length or
a bad checksum. Before truncating there, the reader now scans the rest of
the file for a complete, checksum-valid frame (`first_valid_frame_after`,
`storage.rs:1123`; only on this rare path):

- None found: a torn tail. Appends are sequential, and a failed append cuts
  itself back or poisons the file, so a crash or failure leaves only a torn
  suffix. It is truncated and the truncation synced (`settle_recovered_tail`,
  `storage.rs:1090`).
- One found: damage in the middle (media corruption, or a file written by a
  version without the repair). Truncating would silently drop that frame
  and every later one. The reader returns `Corruption` ("a valid frame
  follows at offset N"), and the file is left unchanged. This closes GAP-3
  of `docs/recovery-semantics.md` for these logs and for the transaction
  log, which uses the same reader; the page log (`store.wal`) is not
  affected.

Frame headers start with a 4-byte magic and a version byte that serialized
JSON payloads cannot contain (control bytes are escaped), and a match must
also pass the CRC, so the scan does not mistake payload bytes for a frame.

**Operator recovery for mid-file damage.** `bicdb_core::salvage_frame_log`
(`storage.rs:1191`), with the database closed, keeps every frame before the
damage, copies the damage and everything after it to
`<file>.damaged-<offset>` (synced before the log is cut), and truncates the
log there. It is the explicit decision to give up those frames, for when no
backup can restore them.

**Residual assumptions.**

- *Out-of-order writeback on power loss.* A torn suffix is the only damage a
  crash leaves when the file system persists a file's appended data in
  order. Some file systems may persist a later page of an unsynced append
  before an earlier one. Power loss in the middle of the final, unsynced
  append could then leave a hole followed by valid frames of that same
  append. Recovery reports this as mid-file damage and refuses the open
  rather than guessing. Nothing in that append was acknowledged, so
  `salvage_frame_log` loses nothing acknowledged in that case, but it needs
  an operator. With `fsync` off, the unsynced window is larger.
- *The marker is best effort.* When it cannot be written, the reopen
  accepts whatever complete frames of the failed batch reached the disk.
- *`sync.log` is written before the commit decision*
  (`db/spatial_paged.rs:2444`, the existing TODO). A crash, or a commit
  that aborts (a failed decision write, or `08007` resolved as absent),
  after its ops were appended leaves those ops pending for a commit that
  did not happen. This change does not alter that ordering; it needs the
  outbox to move behind the decision, with recovery republishing from the
  transaction log as the audit outbox does.
- *Replication export reads the file, not the durable prefix.*
  `export_replication_frames_since` can return a complete frame whose
  `O_DSYNC` write has not returned yet. This change only stops that reader
  from truncating; bounding it by the written sequence is separate.
### 7.5 HA, consensus and sidecar state files

All of these are now written with one primitive, `storage::replace_durably`
(`crates/bicdb-core/src/storage.rs`): a new temporary file (unique name,
`O_NOFOLLOW`, mode 0600), `fsync` of the file, rename over the old name,
`fsync` of the directory. With `fsync` off only the write and the rename
happen. A rename is atomic: when it fails nothing was renamed. So a failure
up to and including the rename leaves the old file, and only the directory
`fsync` after a successful rename leaves the outcome unknown (the new file
is visible now and may be gone after a power loss). The `FailClosed` policy
of each caller says what that means:

- `AfterRename`: a failed directory `fsync` fences the database
  (`WriteFailed`, site `<file>.dir_sync`); earlier failures are returned and
  change nothing.
- `Always`: memory already holds the new contents, so any failure fences.
- `No`: the error is returned; the database stays healthy.

A fenced database refuses every further replacement under `AfterRename` or
`Always`, so no vote, term, epoch, fence or catalog change is made on it.

Inventory, ranked by consequence. "Before" is the write pattern before this
change.

| Rank | File | Protects | Before | Now | A lost or failed write means |
|---|---|---|---|---|---|
| 1 | `consensus-state.json` (`persist_consensus_state`, `BicDb::consensus_*`) | Consensus term, `voted_for`, log entries, commit index | Fixed temporary name (`fs::write`), file `fsync`, rename, **no directory `fsync`**; memory changed before the write and kept on error | Durable protocol, `AfterRename`; every change goes through `consensus_transition`, which puts memory back to the state before the change on any error; directory `fsync` at open | A vote or term lost after it was granted allows two votes in one term; a follower's acknowledged entries lost after the leader counted them can drop a committed entry |
| 2 | `ha-node.json` (`HaNode::with_file`) | Role (primary, standby, fenced), epoch, vote and election term, switchover offer (drain), epoch starts, standby slots and holds | Durable protocol (`write_atomic`), but a failed directory `fsync` was a plain error: memory stayed old while the new file was live, and later changes went on | Durable protocol, `AfterRename`; directory `fsync` at open | A lost fence or epoch unfences a superseded primary; a lost vote allows a second vote in the term; a lost drain lets the old primary accept writes after the target took over; lost slots release log a standby still needs |
| 3 | `collections.json`, `indexes.json` (`persist_catalog`, `persist_index_catalog`) | The collection and index catalogs | Durable protocol; memory changed before the write; every failure was a plain error | `Always` (sites `catalog.*`, `index_catalog.*`) | Memory runs ahead of the disk. A CREATE retried on the same handle found the collection in memory and returned success without persisting it; rows acknowledged into it could vanish at the next open |
| 4 | Standby bootstrap install (`finish_standby_bootstrap`) | Swap of the staged snapshot for the old directory | Two renames, directory `fsync` result ignored, then the old directory deleted | Directory `fsync` after both renames is the commit point (`WriteFailed`, site `bootstrap.dir_sync`, fails the open); `BOOTSTRAP_READY` stays inside the installed directory until it succeeds, and the next open completes the install | A power loss after the old directory was deleted but before the renames were durable could leave neither database |
| 5 | `ha-state.json` (`persist_ha_state`; file-shipping HA, `promote_standby`, `configure_standby_from`) | Promotion of a file-shipping standby; apply progress | Fixed temporary name, file `fsync`, rename, **no directory `fsync`**; a failed apply's error write ignored (`let _`) | Durable protocol, `No` (no open handle owns it: the operation fails and is not acknowledged). Before the first file is copied, an apply records durably that it is in progress | A lost promotion reverts the node to standby; an interrupted apply, whose older state said the last apply succeeded, could be promoted without `force` with files from two checkpoints |
| 6 | `pending-replication-catalog` sidecar (`stage_pending_replication_catalog`) | Redo of a catalog frame on a standby | Durable protocol, plain error | `AfterRename` (site `pending_catalog.dir_sync`) | The next open may or may not redo the frame while the import reported failure |
| 7 | `model-registry.json`, `memory-indexes.json`, `memory-index-jobs.json` | Model and memory-index definitions and jobs | Durable protocol, plain error | `AfterRename` | A definition reported failed could reappear, or one reported created could vanish |
| 8 | `planner-stats.json`, `index-maintenance.json` | Planner statistics, index build status and sizes | Durable protocol | `No` (section 7.2) | Plans and reported sizes until the next ANALYZE or rebuild |
| 9 | `replication-state.json` | Descriptive standby apply state | Durable protocol, no `fsync` on the success path | Unchanged, `No` (section 7.2) | Nothing: the applied position is durable in the transaction log, and open takes the larger of the two |
| 10 | Replication snapshot restore (`restore_snapshot_archive_atomic`, offline CLI) | A restored snapshot directory | Files synced; directories not; no directory `fsync` after the rename | Every staging directory synced before the rename, the parent after it | A power loss could leave a target with missing entries |

Also checked, already complete and not attached to a database handle: the
cluster metadata consensus state (`distribution_consensus.rs`) and the range
write catalog (`distribution_range_consensus.rs`) use `write_atomic` with the
directory `fsync`.

## 8. Site names and fault injection

The recorded `site` and the injection name are the same strings.

| Injection site | Where |
|---|---|
| `page_log.write`, `page_log.sync` | `Wal::append`, `Wal::sync_through` |
| `page_log.seal_sync`, `page_log.seal_dir_sync` | `Wal::seal_active_locked` (rename failures record `page_log.seal_rename`) |
| `page_log.reset_unlink` | `Wal::reset`, each sealed-segment deletion |
| `page_log.reset_dir_sync` | `Wal::reset`, the directory sync after the deletions |
| `page_log.reset`, `page_log.reset_sync` | `Wal::reset`, the active-file truncation and its sync |
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
| `ha_node.write`, `ha_node.sync`, `ha_node.rename`, `ha_node.dir_sync` | `ha-node.json` (`HaNode::with_file`); only `dir_sync` is fatal |
| `consensus_state.write`, `.sync`, `.rename`, `.dir_sync` | `consensus-state.json`; only `dir_sync` is fatal |
| `catalog.*`, `index_catalog.*` | `collections.json`, `indexes.json`; every step is fatal |
| `pending_catalog.*`, `model_registry.*`, `memory_index_catalog.*`, `memory_index_jobs.*` | Sidecars; only `dir_sync` is fatal |
| `planner_stats.*`, `index_maintenance.*`, `ha_state.*` | Not fatal |
| `bootstrap.rename`, `bootstrap.dir_sync` | `finish_standby_bootstrap`; both fail the open (`WriteFailed`) and the next open resumes |

| `frame_log.write` | `storage::append_frames*`, before the write (scoped by the file's path) |
| `frame_log.short_write` | the same, after writing about half the batch, ending inside a frame |
| `frame_log.sync` | the same, after the whole batch was written, when the append is durable |
| `frame_log.repair` | the cut-back after a failed append (recorded as `frame_log.repair` when it fails) |

Other recorded sites: `transaction_log.auxiliary`, `paged.commit`,
`paged.abort`, `commit.materialize`, `standby.import`, `open.verify`,
`record_audit.publish`.

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

Crash points (`BICDB_TEST_CRASH_AT=<point>:<n>`) follow every step of a
durable replacement: `<site>.after_write`, `<site>.after_sync`,
`<site>.after_rename`, `<site>.after_dir_sync`; also `bootstrap.after_rename`,
`bootstrap.after_dir_sync` and `ha_ship.after_file` (after each file of a
file-shipping apply). `BICDB_TEST_DIR_VOLATILE=1` emulates power loss of
directory entries: at the crash, every rename of a durable replacement that
no directory `fsync` covered yet is undone, and a replaced file whose data
was never synced is truncated (`test_faults::register_volatile_rename`,
`directory_synced`).

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
| `storage::tests::a_torn_or_damaged_last_frame_at_every_byte_offset_is_cut_to_the_last_good_frame` | A last frame cut at every byte offset inside it, and a complete last frame with any one byte damaged, for each of the four recovery readers (buffered, mmap, streaming, snapshot tail): exactly the last frame is cut, every earlier frame kept, appends resume on the boundary |
| `storage::tests::damage_followed_by_valid_frames_is_corruption_and_the_file_is_left_unchanged` | Every byte of a middle frame, a zeroed run, and a torn frame followed by complete ones, for each reader: `Corruption`, file byte-for-byte unchanged; `salvage_frame_log` keeps the prefix and preserves the rest |
| `storage::tests::a_failed_short_or_unsynced_append_is_cut_back_and_later_appends_survive` | `frame_log.write`, `frame_log.short_write`, `frame_log.sync`, with and without `fsync` |
| `storage::tests::a_failed_repair_poisons_the_log_until_recovery_cuts_the_failed_bytes` | Poison refuses appends and writes nothing; each recovery reader applies the marker, so complete frames of the failed batch are not accepted |
| `storage::tests::live_readers_never_truncate_an_in_flight_frame` | The live readers stop before an incomplete frame and never change the file |
| `frame_log_torn_tail::a_failed_sync_log_append_aborts_its_commit_and_later_commits_survive_reopen` | Embedded and paged, three failure kinds: the commit aborts with an ordinary error, the database stays healthy, and the CDC outbox equals exactly the acknowledged commits' ops before and after two reopens |
| `frame_log_torn_tail::a_failed_sync_log_repair_fences_and_recovery_never_reports_the_aborted_ops` | A failed cut-back fences (`frame_log.repair`); the reopen verifies and reports no op of the aborted commit |
| `frame_log_torn_tail::a_failed_sync_acknowledgment_keeps_the_ops_pending_and_the_log_clean` | `mark_synced` failures |
| `frame_log_torn_tail::a_failed_event_append_is_cut_back_and_later_events_survive_reopen`, `a_failed_event_log_repair_fences_and_recovery_keeps_only_acknowledged_events` | The event log, both storage modes |
| `frame_log_torn_tail::audit_events_behind_a_commit_survive_a_failed_event_append` | Audit outbox: the commit is reported committed, the database fenced (`record_audit.publish`), the checkpoint refused on the fenced handle, the reopen republishes; after more commits, a checkpoint and two reopens every committed row has exactly one audit event |
| `frame_log_torn_tail::a_failed_segment_append_during_checkpoint_loses_no_acknowledged_row` | Record-segment delta append during a checkpoint, then a checkpoint that cuts the transaction log, then two reopens |
| `frame_log_torn_tail::damage_in_the_middle_of_a_frame_log_refuses_the_open_until_salvaged` | `sync.log` and the event log, both modes: the open refuses, the file is unchanged, salvage, the open succeeds |
| `frame_log_torn_tail::replication_readers_never_truncate_the_live_transaction_log` | `replication_retention_status` and `export_replication_frames_since` with a frame mid-write |
| `commit_durability_crash::transaction_log_only_path_survives_every_crash_point_with_eviction` (`materialize.torn_markers`) | The crash hook now holds the transaction log's writer lock across its torn write and the crash. It used to release the lock in between, so a concurrent commit could append complete frames behind the torn ones, a state no crash can produce; recovery, which used to drop those frames silently, now refuses it. Passed 3 of 3 repeats after the change |
| `core::intentional_frame_corruption_is_detected_and_not_silently_repaired` | Updated: offset-0 damage in a log with later frames refuses the open (it used to truncate the whole log); salvage, then the open succeeds |
| `storage::tests::durable_replacement_fails_closed_per_step_and_policy` | Each of the four steps under each policy: file contents after the failure, fencing, site name, no temporary file left, a fenced health refuses the next replacement |
| `ha_state_durability::a_vote_is_never_granted_ahead_of_the_disk_and_never_twice_per_term` | `ha_node.*` at each step, embedded and paged: no grant, nothing in memory, fencing only on `dir_sync`, a fenced node refuses votes and candidacies, and after the reopen a second candidate in the term is granted exactly when the first vote is not on disk |
| `ha_state_durability::a_primary_whose_fence_cannot_be_persisted_still_stops_writing` | A primary that learns of a newer epoch refuses writes whatever happens to its fence file; a reported fence survives the reopen |
| `ha_state_durability::switchover_drain_and_election_promotion_fail_closed` | Drain offer and election promotion at each step |
| `ha_state_durability::a_consensus_vote_is_durable_before_it_is_granted` | `consensus_state.*` at each step: memory put back, no grant, fencing on `dir_sync`, never two votes in a term across the reopen |
| `ha_state_durability::file_shipping_promotion_and_apply_fail_closed` | `ha_state.*`: a promotion is on disk only when reported; an apply whose final state write fails refuses promotion without `force` |
| `ha_state_durability::a_bootstrap_install_completes_only_after_its_directory_sync` | `bootstrap.rename` leaves the old directory and the staging; `bootstrap.dir_sync` fails the open and the next open completes the install |
| `ha_state_durability::a_catalog_write_failure_fences_and_acknowledged_rows_survive` | `catalog.*` at each step fences; the retried CREATE no longer succeeds; acknowledged rows survive the reopen |
| `ha_state_durability::a_planner_statistics_failure_is_not_fatal` | The deliberate non-fatal case |
| `ha_state_crash::*` | Out of process (`examples/ha_state_child.rs`), with `BICDB_TEST_DIR_VOLATILE=1`: a crash at each step of `ha-node.json` (vote, fence, drain, promotion), `consensus-state.json` (vote) and `collections.json` (CREATE COLLECTION and a row in it), and after the acknowledgment. A crash before the directory `fsync` loses the change, after it keeps it; every acknowledged vote, fence, drain, promotion, collection and commit is present after the reopen, and a second vote in the same term is refused. A file-shipping apply crashed mid-copy refuses promotion without `force` |
| `commit_durability_crash` fail-closed matrix, `catalog.write:4`, `catalog.dir_sync:5` | The catalog sites under the concurrent workload: fenced, no acknowledgment after the failure, acknowledged-commit reconciliation twice |

Mutation checks (each mutation applied alone; the named tests must fail):

| Mutation | Caught by |
|---|---|
| The page log's sync loop no longer checks the health state (waiters retry, a later sync publishes) | `a_failed_log_sync_is_sticky_and_no_later_sync_publishes_durability`, `a_failed_log_write_fences_the_log`, `concurrent_commits_waiting_on_a_failing_sync_all_fail` |
| The transaction log's `write_durable` and `flush_buffer` no longer check the health state | `concurrent_commits_around_a_failing_write_never_acknowledge_after_it` (writes after the failure: 31 instead of 25) |
| The cut no longer fences after a failed rename or sync | `a_failed_log_cut_fences_after_the_rename_and_not_before` |
| Generic appender: the cut-back after a failed append is a no-op | `a_failed_short_or_unsynced_append_is_cut_back_and_later_appends_survive` and 5 `frame_log_torn_tail` tests |
| Recovery readers: no mid-file detection (always truncate) | `damage_followed_by_valid_frames_is_corruption_and_the_file_is_left_unchanged`, `stream_frames_matches_read_frames_across_recovery_shapes`, `damage_in_the_middle_of_a_frame_log_refuses_the_open_until_salvaged`, `core::intentional_frame_corruption_is_detected_and_not_silently_repaired` |
| Both of the above (the old behaviour), with the cut-back length checks relaxed so the reconciliation runs | Acknowledged data lost at the reopen: record rows (`delta-0` …), 5 of 5 later events, 6 later CDC ops plus 3 ops of the aborted commit reported, a CDC acknowledgment |
| Replication readers use the recovery readers again | `replication_readers_never_truncate_the_live_transaction_log` |
| The append-failure marker is ignored | `a_failed_repair_poisons_the_log_until_recovery_cuts_the_failed_bytes`, `a_failed_sync_log_repair_fences_and_recovery_never_reports_the_aborted_ops`, `a_failed_event_log_repair_fences_and_recovery_keeps_only_acknowledged_events` |
| A failed event-log append during audit publication no longer fences (and the test's fence check relaxed) | `audit_events_behind_a_commit_survive_a_failed_event_append`: the checkpoint cut the outbox, and the committed row's audit event was lost |
| The durable replacement no longer `fsync`s the directory (the state of `consensus-state.json` and `ha-state.json` before this change) | Every `ha_state_crash` matrix: the vote, fence, drain, promotion and CREATE COLLECTION are lost after acknowledgment, and so is a row acknowledged into the new collection |
| `ha-node.json` replacements not fail-closed | `a_vote_is_never_granted_ahead_of_the_disk_and_never_twice_per_term`, `a_primary_whose_fence_cannot_be_persisted_still_stops_writing`, `switchover_drain_and_election_promotion_fail_closed` |
| `consensus-state.json` replacements not fail-closed | `a_consensus_vote_is_durable_before_it_is_granted` |
| Consensus memory not put back on a failed write | `a_consensus_vote_is_durable_before_it_is_granted` |
| The catalog fences only after the rename | `a_catalog_write_failure_fences_and_acknowledged_rows_survive` |
| No in-progress record before a file-shipping copy | `an_interrupted_ship_is_never_promoted_without_force` |

## 10. Known gaps (outside this change)

- **Partial page-log reset: fixed** (GAP-2, `docs/recovery-semantics.md`
  4.4). `Wal::reset` is crash-ordered: segments deleted, directory synced,
  active file truncated and synced last. Recovery finishes an interrupted
  reset, and refuses any other short chain.
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

The frame-log follow-up (section 7.4) adds two points for review:

4. **Mid-file damage refuses the open.** Damage in a frame log that has
   valid frames after it used to be truncated silently at open; it is now
   `Corruption`, and the open fails until the file is restored or salvaged.
   This trades availability for not losing acknowledged frames. It applies
   to the transaction log too, which shares the reader.
5. **A failed event-log append during audit publication fences.** It used
   to return an ordinary error on a usable handle, and a checkpoint on that
   handle could then cut the unpublished outbox out of the transaction log.
   It now follows the rule of section 4 for a failed publication marker:
   the commit, durable and visible, is reported as committed, the failure
   is logged (`durability.audit_publication_deferred`), and the database is
   fenced, so the next checkpoint is refused and the reopen republishes.

The HA and consensus state-file follow-up (section 7.5) adds:

6. **HA and consensus state.** A vote, a term, an epoch, a fence, a drain
   or a promotion is reported only after its file and directory are synced.
   Open now `fsync`s the directory holding `ha-node.json` and
   `consensus-state.json` (one directory `fsync` per open) so a state left
   by a process that stopped between its rename and its directory sync is
   durable before the node acts on it. The verification step of a reopen
   after a durability failure also rewrites both files through the full
   protocol.
7. **Catalog.** Any failure writing `collections.json` or `indexes.json`
   fences the database, including an out-of-space failure before the
   rename. Before, the handle went on with a catalog in memory that the
   disk did not have.
8. **File-shipping apply.** An apply now writes an in-progress record
   before copying. A standby whose apply was interrupted refuses promotion
   without `force` (it reports the replication lag and the unfinished
   apply).

Unresolved assumptions:

- A primary whose own fence write fails at the directory `fsync` is fenced
  in process (quorum closed, `WriteFailed`), but if the rename is lost to a
  power loss it reopens as a primary of its old epoch until a peer
  announces the newer one. Every peer and standby already follows the newer
  epoch, so a synchronous-quorum primary cannot confirm anything; an
  asynchronous primary in that state accepts local writes until it hears
  the new epoch, which is the same exposure as a primary that was
  partitioned before it ever learned of the new epoch.
- The bootstrap install's directory `fsync` and the load-time directory
  `fsync` are covered by fault injection, not by the power-loss emulation,
  which only tracks renames of files.
- The commit-log archive and the page-WAL archive (section 7.3) are
  unchanged by this follow-up.

Not implemented, because each changes the durability contract or adds
synchronous I/O to a hot path, and needs a decision first:

- Making the page-log reset durable (an `fsync` after the truncation and a
  directory `fsync` after removing sealed segments).
- Fail-closed handling of the commit-log archive's retried `sync()` and the
  page-WAL archive's ignored directory sync before
  `mark_wal_archived_through`. (The generic frame appender's tail repair
  and poisoning are in section 7.4.)
