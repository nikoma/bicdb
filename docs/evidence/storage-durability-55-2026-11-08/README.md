# Post-merge evidence: #55 fail-closed write and fsync failures (2026-11-08)

**Not production qualification.** Emulated I/O failures (EIO injected by
`BICDB_TEST_FAIL_IO`) and crashes on one host, one revision, synthetic
workload. No real device error and no real power loss. Production stays on
its last qualified configuration.

Method, harness, environment, build and the cross-cutting findings are in
[`../storage-durability-harness-2026-11-08/README.md`](../storage-durability-harness-2026-11-08/README.md).

| | |
|---|---|
| Fix | #55, merge commit `e4647d88` |
| Source revision tested | `main` `64251c3e2b37d0ffd6a4b30ad50ceaaedb6d7963` (includes #55, #74, #67) |
| Server binary sha256 | `6844a23a6793e8a63507b2885b106ac8bf2368524420074373dca8d9a02968a2` |
| Verifier binary sha256 | `ea1473a8f6247b8433ef2484bd0c6a177183fc69d4221537a2e13d43d7526a46` (main + unmerged PR #85, merge `d2a7fe1c`) |
| Environment | s3ny, Linux 6.8.0-147-generic, ext4 on NVMe `CT2000E100SSD8` (volatile write cache, FUA), server `--storage-sync durable` (fsync on) |
| Trials | 350, plus the shared SIGKILL baselines and 10 negative controls (harness README) |

## Claim tested

After the first durability-critical write or `fsync` failure the database is
fenced: the affected commits report 08007 with an identity in DETAIL, later
statements report 58030, no commit is acknowledged afterwards, a reopen runs
recovery and the verification step and removes `durability-failed.json`, and
`bicdb_commit_outcome(incarnation, commit_seq)` then gives a definitive
answer that matches what is stored.

## Injected failure boundaries

Each trial picks one site and a random call number `n` from the range shown.
Sites: the transaction log (`transaction_log.write`, `.sync`, `.repair`,
`.cut_rename`, `.cut_sync`, `.cut_copy`), the page log (`page_log.write`,
`.sync`, `.seal_sync`, `.seal_dir_sync`), and the page file
(`page_file.sync`, `page_file.write`); one-shot and persistent (dead disk);
in-workload and during a graceful shutdown (the shutdown checkpoint and
transaction-log cut). Crash points `commit.after_txlog_durable`,
`commit.after_page_apply`, `commit.after_marker`, with and without the
page-log power-loss emulation. Page-log reset failures are reported under
#67.

## Results

| Boundary | Mode / profile | Injected failure | Trials | Fired | Result | Acked | Failed (absent) | 08007 committed / not | In flight: present / absent | Violations | Verifier exit codes pre / pre2 / post |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `f55-crash-commit-paged-powerloss` | paged / stress | abort at `commit.after_txlog_durable`, `commit.after_page_apply`, `commit.after_marker` n=10..400 (power loss emulated) | 20 | 16 | all pass | 8839 | 0 | 0 / 0 | 223 / 7 | none | 0:20 / 0:20 / 0:20 |
| `f55-crash-commit-paged` | paged / stress | abort at `commit.after_txlog_durable`, `commit.after_page_apply`, `commit.after_marker` n=10..400 | 20 | 19 | 1 FAIL | 10431 | 0 | 0 / 0 | 216 / 11 | writer_index_differs x2 | 0:20 / 0:19 2:1 / 0:19 non-0:1 |
| `f55-pagefile-sync-paged` | paged / stress | EIO at `page_file.sync` n=1..20 | 20 | 16 | all pass | 7029 | 308 | 69 / 7 | 109 / 5 | none | 0:20 / 0:20 / 0:20 |
| `f55-pagefile-write-paged` | paged / stress | EIO at `page_file.write` n=1..200 | 10 | 9 | all pass | 3882 | 176 | 39 / 1 | 44 / 5 | none | 0:10 / 0:8 3:2 / 0:10 |
| `f55-pagelog-seal-paged` | paged / stress | EIO at `page_log.seal_sync`, `page_log.seal_dir_sync` n=1..10 | 20 | 20 | all pass | 7469 | 392 | 81 / 7 | 102 / 5 | none | 0:20 / 0:20 / 0:20 |
| `f55-pagelog-sync-dead-paged` | paged / stress | EIO at `page_log.sync` n=20..400 (persistent (`+`)) | 10 | 10 | all pass | 3992 | 184 | 55 / 1 | 53 / 1 | none | 0:10 / 0:9 3:1 / 0:10 |
| `f55-pagelog-sync-paged` | paged / stress | EIO at `page_log.sync` n=20..400 | 20 | 19 | all pass | 7448 | 351 | 103 / 2 | 109 / 5 | none | 0:20 / 0:20 / 0:20 |
| `f55-pagelog-write-paged` | paged / stress | EIO at `page_log.write` n=20..2000 | 20 | 20 | all pass | 9193 | 376 | 98 / 6 | 102 / 6 | none | 0:20 / 0:20 / 0:20 |
| `f55-txlog-cut-embedded` | embedded / stress | EIO at `transaction_log.cut_rename`, `transaction_log.cut_sync` n=2..3 | 20 | 19 | all pass | 111190 | 456 | 0 / 0 | 19 / 62 | none | n/a / n/a / n/a |
| `f55-txlog-cut-paged` | paged / stress | EIO at `transaction_log.cut_rename`, `transaction_log.cut_sync` n=2..4 | 20 | 8 | 1 FAIL | 12603 | 192 | 0 / 0 | 159 / 10 | k_index_differs x1, writer_index_differs x1 | 0:20 / n/a / 0:20 |
| `f55-txlog-cut-shutdown-embedded` | embedded / stress | EIO at `transaction_log.cut_rename`, `transaction_log.cut_sync` n=2 (graceful shutdown) | 20 | 19 | all pass | 71636 | 456 | 0 / 0 | 19 / 59 | none | n/a / n/a / n/a |
| `f55-txlog-cut-shutdown-paged` | paged / stress | EIO at `transaction_log.cut_rename`, `transaction_log.cut_sync` n=2 (graceful shutdown) | 20 | 15 | all pass | 14329 | 360 | 0 / 0 | 98 / 12 | none | 0:20 / n/a / 0:20 |
| `f55-txlog-cutcopy-shutdown-paged` | paged / stress | EIO at `transaction_log.cut_copy` n=2 (graceful shutdown) | 10 | 0 | all pass | 8372 | 0 | 0 / 0 | 52 / 4 | none | 0:10 / n/a / 0:10 |
| `f55-txlog-sync-embedded` | embedded / stress | EIO at `transaction_log.sync` n=20..400 | 20 | 20 | all pass | 59122 | 399 | 0 / 81 | 20 / 52 | none | n/a / n/a / n/a |
| `f55-txlog-sync-paged` | paged / stress | EIO at `transaction_log.sync` n=20..400 | 20 | 20 | all pass | 10127 | 366 | 98 / 16 | 106 / 5 | none | 0:20 / n/a / 0:20 |
| `f55-txlog-sync-repairfail-embedded` | embedded / stress | EIO at `transaction_log.sync` n=20..400 (plus `transaction_log.repair:1`) | 10 | 10 | all pass | 17185 | 210 | 10 / 20 | 10 / 28 | none | n/a / n/a / n/a |
| `f55-txlog-sync-repairfail-paged` | paged / stress | EIO at `transaction_log.sync` n=20..400 (plus `transaction_log.repair:1`) | 20 | 20 | all pass | 7780 | 368 | 108 / 4 | 107 / 4 | none | 0:20 / n/a / 0:20 |
| `f55-txlog-write-dead-paged` | paged / stress | EIO at `transaction_log.write` n=20..400 (persistent (`+`)) | 10 | 10 | all pass | 3980 | 183 | 48 / 9 | 56 / 2 | none | 0:10 / n/a / 0:10 |
| `f55-txlog-write-embedded` | embedded / stress | EIO at `transaction_log.write` n=20..400 | 20 | 20 | all pass | 64244 | 419 | 0 / 61 | 19 / 51 | none | n/a / n/a / n/a |
| `f55-txlog-write-paged` | paged / stress | EIO at `transaction_log.write` n=20..400 | 20 | 19 | all pass | 8576 | 351 | 92 / 13 | 111 / 13 | none | 0:20 / n/a / 0:20 |

Columns: *Fired* = the fault reached its site (fence marker, `durability.write_failed`
event or SQLSTATE observed; for crash points the crash message). *Acked* =
`COMMIT` successes in the client ledgers, all cycles. *Failed (absent)* =
non-08007 failures, all required absent. *In flight* = `COMMIT` sent, no
answer (connection lost), atomic either way. Verifier exit codes: 0 clean,
2 inconsistent, 3 needs an external cross-check; `n/a` where not run
(embedded stores, or before the pre-recovery-2 run was added).

Totals over the #55 boundaries:

- Faults fired in 309 of 350 trials. In 5 further trials the fault hit
  during the server's open (small `n`), the open failed, and the trial
  continued with a clean reopen; they are counted as not fired.
- **(a)** 0 acknowledged transactions missing at the recovery of the
  injected fault (447,427 acknowledged commits over both cycles of these
  trials).
- **(b)** 0 of 5,547 non-08007 failures present (58030 after the fence, and
  ordinary errors).
- **(c)** 1,029 transactions received 08007. Every DETAIL carried
  `incarnation`, `commit_seq` and `transaction_id`; every `transaction_id`
  equalled the `txid_current()` the client recorded inside that
  transaction. After the reopen `bicdb_commit_outcome` answered 801
  `committed` and 228 `not_committed`, 0 `unresolved`; every answer matched
  presence, and every answer was unchanged at the second recovery after more
  commits (which reuse sequence numbers).
- **(d)** 0 rows that no transaction wrote, 0 partial transactions, 0
  resurrected deletes.
- **fence**: 0 acknowledgments after a client's first 08007/58030 within one
  server incarnation, including the persistent (dead-disk) variants.
- `durability-failed.json` was present after the fence in all 354 fenced
  trials (all fixes) and absent after the verified reopen in all 354.
- **(e)** 2 trials failed index-versus-heap agreement, both at the second
  recovery after a plain SIGKILL (not at the recovery of the injected
  fault). See "Failures" below.
- **(f)** Verifier clean after the final clean shutdown in 259 of 260 paged
  trials; the exception is `f55-crash-commit-paged` rep 15 (below).

## Failures in these boundaries

| Trial | Boundary fired | Failure | At | Deterministic on replay |
|---|---|---|---|---|
| `f55-crash-commit-paged` rep 15 | `commit.after_page_apply:205` (recovery 1 clean) | `evr_writer` misses 20 + 185 acknowledged rows the heap, primary key and `evr_k` have; verifier `inconsistent` before and after | recovery 2, after plain SIGKILL | yes, 2 of 2 |
| `f55-txlog-cut-paged` rep 3 | `transaction_log.cut_rename:2` (fenced; recovery 1 clean) | `evr_writer` and `evr_k` miss 2 rows (`w5-81:m`, `w5-81:0`) of an in-flight transaction that recovery kept; `:1` of it is indexed; verifier `clean` after | recovery 2, after plain SIGKILL | no crash copy (first failure, before the harness kept copies); the recovered store still shows it after further restarts |

Both belong to class F2 of the cross-cutting findings: they follow a plain
SIGKILL on the `stress` profile, not the #55 boundary.

## Observations

1. **`page_file.write` is documented as not fatal, but fenced.**
   `docs/durability-failure-handling.md` section 8 lists `page_file.write`
   (`PageStore::write_page_raw`) as "not fatal". In all 9 fired trials the
   database fenced with site `paged.commit` (08007, then 58030). This fails
   closed and lost nothing; the document or the behaviour should be
   reconciled.
2. **Recorded site names differ from injection names for transaction-log
   sync faults.** Section 8 says "The recorded `site` and the injection name
   are the same strings". Injecting `transaction_log.sync` recorded
   `transaction_log.write` (embedded, 30 of 30) or `transaction_log.write` /
   `transaction_log.auxiliary` (paged); injecting `transaction_log.write` in
   paged mode recorded `transaction_log.auxiliary` in 18 of 29. The cut sites
   record `transaction_log.cut`, as documented.
3. **Outcome of a failed transaction-log sync differs by mode.** With only
   `transaction_log.sync` failing, the embedded store resolved all 81
   uncertain commits `not_committed` (matching the section 4 row "tail repair
   succeeds: Absent"), while the paged store resolved 98 of 114 `committed`.
   With the repair failing too: embedded 10 committed / 20 not, paged 108 /
   4. Every answer matched what was stored, so this is not a correctness
   violation, but section 4 does not describe the paged behaviour (a group
   of concurrent commits shares one failing sync, so the counts include
   group-commit peers).
4. **`transaction_log.cut_copy` coverage is unproven.** The site is not
   fatal and leaves no external trace; 0 of 10 trials could confirm it fired.
   No claim is made for it.
5. In-workload transaction-log cuts are rare on a paged store (cut mostly at
   open and close): the in-workload cut fault fired in 8 of 20; the shutdown
   variant in 15 of 20.

## Remaining risks

- EIO was injected before or instead of the I/O. The kernel behaviour the fix
  exists for (a real failed `fsync` dropping dirty pages and clearing the
  error) was not produced; a real device or `dm-error`/`dm-flakey` run would
  be needed.
- Power loss was emulated only for the page log's unsynced tail at named
  crash points; the transaction log, page file and directory metadata kept
  their page-cache contents. No real power cut, no drive-cache loss.
- One host, one filesystem (ext4), one drive model, six clients, short
  cycles. Production runs the `default` profile, where most of these sites
  are reached far less often; the `stress` profile was needed to reach them.
- Not covered: failures during the open itself as a counted boundary (5
  trials hit it incidentally), HA/quorum and standby paths (#71, open),
  log-archive paths (#73, open), the event log, record-audit outbox,
  bulk-load and index-build commits (08007 with `commit_seq=none`), and
  encrypted stores.
- Plain-SIGKILL recovery of `server_paged` stores on the `stress` profile is
  not reliable at this revision (12 failures in 1,422 plain-SIGKILL
  recoveries, including silent loss and silent index omissions; harness
  README section 5). Any #55 guarantee is bounded by that.
- The verifier (PR #85) is unmerged and does not detect small
  secondary-index omissions after recovery.
