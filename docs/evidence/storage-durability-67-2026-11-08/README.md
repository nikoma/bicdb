# Post-merge evidence: #67 crash-ordered page-log reset and horizon-bounded redo (2026-11-08)

**Not production qualification.** Crashes were real process aborts at named
points or SIGKILL; power loss was **emulated** by truncating the page log's
unsynced tail at the crash point (`BICDB_TEST_WAL_VOLATILE_TAIL=1`), no real
power cut; I/O failures were injected EIO. One host, one revision, synthetic
workload. Production stays on its last qualified configuration.

Method, harness, environment, build and the cross-cutting findings are in
[`../storage-durability-harness-2026-11-08/README.md`](../storage-durability-harness-2026-11-08/README.md).

| | |
|---|---|
| Fix | #67, merge commit `64251c3e` (the tested revision itself) |
| Source revision tested | `main` `64251c3e2b37d0ffd6a4b30ad50ceaaedb6d7963` (includes #55, #74, #67) |
| Server binary sha256 | `6844a23a6793e8a63507b2885b106ac8bf2368524420074373dca8d9a02968a2` |
| Verifier binary sha256 | `ea1473a8f6247b8433ef2484bd0c6a177183fc69d4221537a2e13d43d7526a46` (main + unmerged PR #85, merge `d2a7fe1c`) |
| Environment | s3ny, Linux 6.8.0-147-generic, ext4 on NVMe `CT2000E100SSD8` (volatile write cache, FUA), server `--storage-sync durable` (fsync on) |
| Trials | 320 |

## Claim tested

`Wal::reset` is crash-ordered (sealed segments deleted, directory synced,
active file truncated and synced last); a reset failure fences; recovery
finishes an interrupted reset and refuses any other short chain; redo is
bounded by the checkpoint horizon, so a crash at any reset or checkpoint
step, or during recovery itself, recovers every acknowledged commit and
nothing else.

All boundaries use `server_paged` with the `stress` profile: 128 KiB
page-log segments and a 512 KiB page-log threshold, so the bounded
checkpoint, segment seals and resets run every few hundred milliseconds.

## Injected failure boundaries

| Boundary | Points / sites (n = hit number) |
|---|---|
| Crash during reset | `wal_reset.after_seal`, `.after_segment_delete`, `.after_segments_removed`, `.after_active_truncate`, n = 1..8, with and without power-loss emulation |
| Crash during checkpoint | `checkpoint.after_wal_reset`, `.after_publish`, `.after_page_flush`, `.after_wal_sync`, `checkpoint_step.after_finish`, `.after_meta_writeback`, `wal_seal.after_rename`, n = 1..8, with and without power-loss emulation |
| Crash during recovery | SIGKILL during the workload, then the restart is armed with one of `page_recovery.after_scan`, `.after_page_replay`, `.after_flush`, `open.after_recovery_checkpoint`, `open.after_txlog_floor`, the four `wal_reset.*` points, `checkpoint.after_publish`, `checkpoint.after_wal_reset`, n = 1..2; then a clean recovery |
| Reset I/O failure | EIO at `page_log.reset_unlink`, `page_log.reset_dir_sync`, `page_log.reset`, `page_log.reset_sync`, n = 1..8 in-workload and n = 1..3 during a graceful shutdown |

Points reached (fired) per site over all #67 trials:
`wal_reset.after_active_truncate` 29, `.after_segment_delete` 26,
`.after_segments_removed` 24, **`wal_reset.after_seal` 0**;
`checkpoint.after_publish` 18, `.after_wal_reset` 15, `.after_wal_sync` 9,
`.after_page_flush` 8, `checkpoint_step.after_meta_writeback` 14,
`.after_finish` 8, `wal_seal.after_rename` 9; during recovery
`page_recovery.after_page_replay` 9, `.after_flush` 5, `.after_scan` 3,
`open.after_recovery_checkpoint` 5, `open.after_txlog_floor` 1 (and the
reset/checkpoint points above when recovery ran them); EIO
`page_log.reset_sync` 15, `page_log.reset` 13, `page_log.reset_unlink` 13,
`page_log.reset_dir_sync` 9.

## Results

| Boundary | Mode / profile | Injected failure | Trials | Fired | Result | Acked | Failed (absent) | 08007 committed / not | In flight: present / absent | Violations | Verifier exit codes pre / pre2 / post |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `f67-crash-checkpoint-paged-powerloss` | paged / stress | abort at `checkpoint.after_wal_reset`, `checkpoint.after_publish`, `checkpoint.after_page_flush`, `checkpoint.after_wal_sync`, `checkpoint_step.after_finish`, `checkpoint_step.after_meta_writeback`, `wal_seal.after_rename` n=1..8 (power loss emulated) | 40 | 33 | 1 FAIL | 17591 | 0 | 0 / 0 | 409 / 20 | writer_index_differs x2 | 0:39 3:1 / 0:40 / 0:40 |
| `f67-crash-checkpoint-paged` | paged / stress | abort at `checkpoint.after_wal_reset`, `checkpoint.after_publish`, `checkpoint.after_page_flush`, `checkpoint.after_wal_sync`, `checkpoint_step.after_finish`, `checkpoint_step.after_meta_writeback`, `wal_seal.after_rename` n=1..8 | 40 | 34 | 1 reopen refused | 18471 | 0 | 0 / 0 | 405 / 25 | none | 0:40 / 0:39 2:1 / 0:39 |
| `f67-crash-reset-paged-powerloss` | paged / stress | abort at `wal_reset.after_seal`, `wal_reset.after_segment_delete`, `wal_reset.after_segments_removed`, `wal_reset.after_active_truncate` n=1..8 (power loss emulated) | 40 | 25 | all pass | 21560 | 0 | 0 / 0 | 421 / 26 | none | 0:40 / 0:40 / 0:40 |
| `f67-crash-reset-paged` | paged / stress | abort at `wal_reset.after_seal`, `wal_reset.after_segment_delete`, `wal_reset.after_segments_removed`, `wal_reset.after_active_truncate` n=1..8 | 40 | 29 | 1 FAIL | 23362 | 0 | 0 / 0 | 429 / 14 | k_index_differs x1 | 0:40 / 0:39 3:1 / 0:40 |
| `f67-recoverycrash-paged-powerloss` | paged / stress | abort during recovery at `page_recovery.after_scan`, `page_recovery.after_page_replay`, `page_recovery.after_flush`, `open.after_recovery_checkpoint`, `open.after_txlog_floor`, `wal_reset.after_seal`, `wal_reset.after_segment_delete`, `wal_reset.after_segments_removed`, `wal_reset.after_active_truncate`, `checkpoint.after_publish`, `checkpoint.after_wal_reset` n=1..2 (power loss emulated) | 40 | 22 | all pass | 26956 | 0 | 0 / 0 | 425 / 22 | none | 0:40 / 0:40 / 0:40 |
| `f67-recoverycrash-paged` | paged / stress | abort during recovery at `page_recovery.after_scan`, `page_recovery.after_page_replay`, `page_recovery.after_flush`, `open.after_recovery_checkpoint`, `open.after_txlog_floor`, `wal_reset.after_seal`, `wal_reset.after_segment_delete`, `wal_reset.after_segments_removed`, `wal_reset.after_active_truncate`, `checkpoint.after_publish`, `checkpoint.after_wal_reset` n=1..2 | 60 | 40 | 1 FAIL | 37470 | 0 | 0 / 0 | 636 / 37 | k_index_differs x2 | 0:60 / 0:59 3:1 / 0:60 |
| `f67-resetfail-paged` | paged / stress | EIO at `page_log.reset_unlink`, `page_log.reset_dir_sync`, `page_log.reset`, `page_log.reset_sync` n=1..8 | 40 | 34 | all pass | 10546 | 619 | 195 / 2 | 206 / 18 | none | 0:40 / 0:40 / 0:40 |
| `f67-resetfail-shutdown-paged` | paged / stress | EIO at `page_log.reset_unlink`, `page_log.reset_dir_sync`, `page_log.reset`, `page_log.reset_sync` n=1..3 (graceful shutdown) | 20 | 16 | all pass | 4560 | 290 | 92 / 2 | 107 / 5 | none | 0:20 / 0:20 / 0:20 |

Columns as in the #55 report. Totals over the #67 boundaries (320 trials,
233 fired; in 11 more the crash point fired during the server's own
startup checkpoint and in 10 more the reset EIO hit the open; the open
failed and the trial continued with a clean reopen; those 21 are counted as
not fired):

- **(a)** 0 acknowledged transactions missing at the recovery of a fired
  boundary (160,516 acknowledged commits over both cycles).
- **(b)** 0 of 909 non-08007 failures present.
- **(c)** Reset I/O failures fenced at the recorded site in all 50 fired
  trials; 291 commits reported 08007; `bicdb_commit_outcome` answered 287
  `committed` and 4 `not_committed`, 0 `unresolved`, all matching presence
  and stable at the second recovery.
- Crash during recovery: the armed point was reached in 62 of 100 trials;
  each time the next, clean recovery reconciled with 0 violations.
- **(e)/(f)** 4 trials failed; none at the recovery of the fired boundary:

| Trial | Boundary | Failure | At | Deterministic on replay |
|---|---|---|---|---|
| `f67-crash-reset-paged` rep 31 | `wal_reset.after_active_truncate:7` fired; recovery 1 clean | `evr_k` misses 3 committed rows; verifier `needs_external_cross_check` before, `clean` after | recovery 2, after plain SIGKILL | yes, 2 of 2 |
| `f67-crash-checkpoint-paged` rep 5 | `checkpoint.after_wal_sync:6` fired; recovery 1 clean | open **panics** at `crates/bicdb-page/src/btree.rs:430:1` (`TryFromSliceError` unwrap) instead of reporting corruption; verifier `inconsistent` before | recovery 2, after plain SIGKILL | yes, 2 of 2 |
| `f67-crash-checkpoint-paged-powerloss` rep 33 | point not reached (plain SIGKILL at the time limit) | `evr_writer` misses 1 committed row | recovery 1 | yes, 2 of 2 |
| `f67-recoverycrash-paged` rep 25 | point not reached; SIGKILL after the recovery | `evr_k` misses 2 committed rows; verifier `clean` before and after | recovery 1 | yes, 2 of 2 |

All four are classes F2/F4 of the cross-cutting findings: plain SIGKILL on
the `stress` profile.

## Observations

1. `wal_reset.after_seal` was never reached, neither in the 80 in-workload
   reset-crash trials nor in the 100 recovery-crash trials (where it was one
   of 11 points); the seal-before-reset step is not covered by this
   evidence.
2. Horizon-bounded redo is not observable from SQL. It is covered only
   indirectly: crashes at every reached reset and checkpoint step, with and
   without discarding the unsynced page-log tail, and crashes during
   recovery, followed by complete reconciliation.
3. A refused or crashed open still runs page recovery and a checkpoint
   before failing elsewhere (see the #74 report, observation 1).

## Remaining risks

- Power loss emulation discards only the page log's unsynced tail at the
  crash point. Directory entries (segment unlinks, renames) and the page
  file keep their page-cache state, which is exactly what a crash-ordered
  reset must survive under a real power cut; that was not produced. A real
  power-cut or `dm-log-writes` replay run is needed.
- `wal_reset.after_seal` and the default-profile reset path (64 MiB
  segments) were not reached.
- The plain-SIGKILL recovery defects (silent index omissions, refusals, one
  panic, one silent loss) all occurred on stores that use the reset path
  heavily (`stress` and `smallwal` profiles), including one refusal with a
  256 MiB pool (`smallwal`), where pool eviction does not explain it. They
  were not root-caused; a contribution of the reset or checkpoint path has
  not been excluded.
- The verifier missed the 2-entry index omission of `f67-recoverycrash-paged`
  rep 25 both before and after recovery.
