# Unlocked page-log sync (gated, off by default)

Status: **under qualification. Do not enable in production.**

Gate: `PagedStoreOptions::with_unlocked_wal_sync(bool)`, plumbed from
`DbConfig::paged_wal_sync_unlocked` / `DbConfig::with_paged_wal_sync_unlocked`,
which defaults from `BICDB_PAGED_WAL_SYNC_UNLOCKED`. Only the exact value `1`
enables it. Default: off.

## 1. The change

`Wal::sync_through` (crates/bicdb-page/src/wal.rs) makes the page log durable
through an LSN, sharing one `fdatasync` across waiters (group commit).

- **Gate off (default, unchanged):** the sync takes the log's file mutex,
  reads the covered offset, runs `fdatasync` on the file, records the offset,
  and only then releases the mutex. Every page-log append needs that mutex,
  and page-store commits append under the store-wide `write_lock`. So one sync
  stalls every appender for its full duration.
- **Gate on:** the sync takes the mutex only to wait out the in-flight append,
  read the covered offset, and duplicate the descriptor (`try_clone`, a
  `dup` sharing the same open file description). It releases the mutex, runs
  `fdatasync` on the duplicate, and then publishes durability exactly as the
  locked path does.

With the gate off, the sync step is the original code. The only additions on
that path are test-only (`#[cfg(test)]`) failure injection and one atomic
increment when a segment seals or the log resets.

## 2. Why the promise is unchanged

`sync_through(lsn)` returns only once `durable_lsn >= lsn`. With the gate on:

1. The published `target` is read from `next_lsn` before the file mutex is
   taken. LSNs are assigned and the record written in one critical section
   under that mutex, so once the sync holds the mutex, every record at or
   below `target` is completely written to the file the mutex guards.
2. `fdatasync` flushes every byte written to the inode before it starts,
   whichever descriptor wrote it. The duplicate names that inode.
3. Records appended after the mutex is released have LSNs above `target` and
   are never published by this sync.
4. A **segment seal** in the window syncs the active file itself, renames it
   and opens a fresh one. The duplicate still names the renamed inode, which
   holds every record at or below `target`.
5. A **reset** (checkpoint truncation) in the window empties the file after a
   checkpoint made its records redundant, and itself sets
   `durable_lsn = next_lsn - 1`. The late publication is a `max`, so it never
   moves `durable_lsn` past what reset already set and never covers a
   post-reset record.
6. **Archive retention** seals before truncating; deleting an archived segment
   unlinks the inode the duplicate names, and syncing an unlinked inode
   succeeds harmlessly.
7. **Errors:** the duplicate shares the open file description, so Linux
   reports a writeback error on it exactly as on the original (one shared
   `f_wb_err`). A failed sync takes the same error branch as before: no
   publication, `sync_failures` incremented, waiters woken to retry.
8. If `try_clone` fails (descriptor exhaustion), the step syncs the original
   under the mutex, exactly like the default path. A failed duplicate never
   becomes a failed commit.

The test-only `synced_offset` (what the power-loss emulation keeps) is
published under the mutex after the sync, and only if the active file has not
been replaced or emptied since the capture (`active_generation`). Production
builds skip that second lock acquisition unless the emulation is on.

## 3. Qualification

All runs on s3ny (24 cores, ext4 on a consumer NVMe, fsync on), 2026-11-08.
Test hooks: `SyncTestHooks` (`#[cfg(test)]`: pause at a phase, fail the next
N syncs, fail the next N descriptor duplications) and two crash points that
exist only in the gated step, `wal.sync.after_clone` and
`wal.sync.before_publish`.

| Area | Test | Result |
|---|---|---|
| Gate | `the_gate_is_off_by_default_and_off_means_the_locked_step`, `the_paged_store_passes_the_gate_through_and_defaults_off` (page); `unlocked_wal_sync_is_strictly_opt_in` (core: default off, only `1` enables, 10 near misses rejected, builder both ways) | pass |
| (a) ordering | `an_unlocked_sync_lets_appends_through_and_claims_only_its_target` (an append completes while the sync is paused; `durable_lsn` ends exactly at the captured target) | pass |
| (a) ordering | `the_unlocked_sync_publishes_nothing_until_the_sync_has_returned` | pass |
| (a) ordering | `group_commit_waiters_return_only_once_their_own_lsn_is_durable` (waiters stay blocked while the leader is paused; exactly 2 syncs for 2 targets) | pass |
| (a) ordering | `concurrent_commits_keep_lsn_order_equal_to_file_order` (8 writers x 100 commits, 8 KiB segments sealing, monitor thread: `durable_lsn` monotonic and `< next_lsn`), gate off and on | pass |
| (a)/(b) | `a_power_cut_never_loses_an_acknowledged_commit` (6 writers, 4 KiB segments, 6 emulated power cuts mid-run), gate off and on | pass |
| (b) crash recovery | Crash matrix, gate on: `all-logs`, `all-logs+small-pool`, `transaction-log`, `transaction-log+small-pool`; 15 common boundaries (+4 materializer) + 6 in-window points (`after_clone`, `before_publish` at 3 hit counts) + interrupted replay; reconciler checks every acknowledged commit, atomicity, index = scan, unique keys, second-reopen idempotence | 4/4 matrices pass |
| (b) crash recovery | Same matrices gate off (regression) | 4/4 pass |
| (b) repeats | 50 rounds each: `all-logs+unlocked-sync` at `wal.sync.before_publish:60`, `wal.sync.after_clone:40`, `commit.after_txlog_durable:150`; `all-logs+small-pool+unlocked-sync` at `wal.sync.before_publish:300`, `pool.after_writeback:120`; `transaction-log+unlocked-sync` at `materialize.after_wal_sync:40` | 300/300 |
| (c) checkpoint | `a_seal_inside_the_window_publishes_only_records_the_old_file_holds` (seal while the duplicate is held: target in the sealed inode, nothing later published, old offset not applied to the new file) | pass |
| (c) checkpoint | `a_reset_inside_the_window_never_makes_later_records_durable` (reset before the sync, and after the sync but before publication) | pass |
| (c) checkpoint | `archive_retention_reset_inside_the_window_keeps_the_segment_and_its_records`; `an_archived_segment_deleted_inside_the_window_does_not_fail_the_sync` (duplicate names an unlinked inode) | pass |
| (d) fsync failure | `sync_failures_behave_exactly_as_on_the_locked_path`: same outcome struct for gate off and on (failed commit: no durability, 1 failure counted, abort suppressed, retry succeeds; persistent failure: all 4 concurrent committers get their own error, `durable_lsn` unchanged; recovery after clear) | pass, identical |
| (d) fsync failure | `one_failed_sync_fails_exactly_one_caller_on_both_paths` (20 rounds each) | pass |
| (d) fsync failure | `a_failure_inside_the_window_wakes_waiters_without_publishing` (1 failure: waiter retries and succeeds; 2 failures: both error, nothing published) | pass |
| (d) dup failure | `a_failed_descriptor_duplication_falls_back_to_the_locked_sync` | pass |
| (e) visibility | `rows_become_visible_only_after_the_unlocked_sync_publishes` (page store: paused after `fdatasync`, a fresh snapshot does not see the row and does not block; failed sync leaves the row invisible, gate off and on) | pass |
| (e) visibility | `commit_visibility`, `sync_transaction_visibility`, `mvcc_chain_recovery` (core, forced `server_paged`), gate off vs on | identical, pass |
| (f) replication | `live_replication` 31, `log_archive_pitr` 8, `backup_online` 3, `backup_online_wal_rotation` 1, `backup_paged` 4, `replication_journal` 2 (core); `embedded_ha` 6, `online_backups` 4 (pgwire) — forced `server_paged`, gate off vs on | identical, pass |
| Regression | `bicdb-page` full suite, gate default (326 lib + integration) | pass |
| Test strength | 5 mutants of the gated step (publish at return, no generation check, offset read after sync, offset published before sync, lock held across sync) | 5/5 killed |

Not run: `bicdb-cli` `backup_online_cli` (the CLI crate does not build on
s3ny: no OpenSSL headers, and it has no vendored-OpenSSL feature). The same
online-backup path is covered by the core and pgwire suites above.

Why visibility cannot move: `PagedStore::commit` marks the transaction
committed only after `wal.commit` returns, and `wal.commit` returns only after
`durable_lsn` covers the record. In the core commit path the applied
watermark (`mark_committed_seq`) and `wait_until_visible` run after the page
commit returns. The gate changes neither order; it only changes what happens
inside `sync_through` before it returns.

## 4. Measurements

`commit_durability_bench run <dir> all-logs <threads> 30` (production commit
path, release build, fsync on), gate off and on run back to back on the same
host, 3 trials, median shown. The host is a shared warm standby: load average
8-40 during the runs; 3 of 24 runs overlapped one other durability job.

| Writers | Gate | Commits/s median (range) | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| 1 | off | 312 (304-353) | 2.5 | 5.1 | 21.5 |
| 1 | on | 311 (245-330) | 2.6 | 5.4 | 22.9 |
| 4 | off | 337 (273-372) | 8.7 | 27.9 | 67.4 |
| 4 | on | 421 (391-439) | 7.6 | 18.2 | 41.0 |
| 16 | off | 527 (274-613) | 29.6 | 44.6 | 72.0 |
| 16 | on | 430 (387-566) | 28.7 | 80.5 | 96.4 |
| 64 | off | 392 (361-633) | 134.4 | 273.9 | 323.3 |
| 64 | on | 502 (453-599) | 103.6 | 174.2 | 328.5 |

- 1 writer: no change (expected; nothing contends for the lock).
- 4 writers: +25% throughput, p95 -35%. The trial ranges do not overlap.
- 16 writers: inconclusive. The median is lower with the gate on, but the
  ranges overlap almost completely (off spans 274-613).
- 64 writers: +28% throughput, p95 -36%, ranges overlap.
- Group commit is not the mechanism. In every run, page-log syncs were
  about equal to commits and `group_commit_savings` was 0: a page-store
  commit syncs while holding the store's `write_lock`, so commits cannot share
  a sync on either path. Whatever the gate gains comes from the other page-log
  appenders (the writeback barrier's page images, checkpoint steps) no longer
  queuing behind a commit's `fdatasync`. This was inferred from the counters
  and not measured directly.
- This run is smaller than the earlier prototype measurement (4 writers:
  368 to 522 commits/s). Both were taken on a loaded shared host.

## 5. Remaining risks

1. **Real power loss is not tested.** The emulation truncates the active file
   to the last published synced offset. It does not test what the kernel and
   drive keep. Before enabling, run a VM hard-reset test on the target
   hardware with the gate on.
2. **Concurrent `fdatasync` and `write` on one inode.** With the gate on, an
   append can run while `fdatasync` is in flight. POSIX and Linux guarantee
   only the bytes written before the call. Later bytes that are partly
   persisted show up as a torn tail, which recovery already truncates, and
   they are never acknowledged. Only ext4 on Linux was exercised.
3. **Platforms.** The argument relies on `fdatasync` being per inode and on
   the duplicate sharing the open file description, which is true for Linux
   `dup` and macOS. Windows (`DuplicateHandle` + `FlushFileBuffers`) was not
   tested.
4. **Already present on both paths, not changed here:** after a failed
   `fdatasync`, a later successful one advances `durable_lsn` over records
   whose writeback may have failed (the kernel can drop the dirty pages and
   clear the error). Neither path poisons the log after a sync failure. A
   seal's own sync can now overlap the gated sync, so which caller sees the
   error can differ, but neither path stops a later sync from publishing.
   This needs its own fix (fail closed after a page-log sync error).
5. **Already present on both paths, not changed here:** `reset()` stores
   `end_offset = 0` and `synced_offset = 0` after releasing the file lock. An
   append in that gap is undercounted (segment sizing, `size_bytes`, and the
   emulation's cut point). Real durability is not affected, since every sync
   covers the whole file.
6. **The benchmark is noisy.** s3ny is a shared standby, and trial spreads
   reach 2x at 16 and 64 writers. The 16-writer result in particular needs a
   quiet host.

## 6. Recommendation

Keep the gate **off**. Correctness qualification passed every area above with
no finding against the change. Two things are still missing before
production:
- the gain needs to be confirmed on a quiet host, since only the 4-writer
  result is outside the noise;
- risk 1 (real power loss) is untested.

Steps proposed to enable it:
1. Merge with the gate off. Default behaviour is the locked path.
2. Repeat the benchmark on a dedicated host with more trials, including 16
   writers.
3. Run a VM hard-reset power-loss test with the gate on.
4. Fix risk 4 separately; it applies to both paths.
5. Then enable it per deployment through `BICDB_PAGED_WAL_SYNC_UNLOCKED=1`
   before considering a default change.
