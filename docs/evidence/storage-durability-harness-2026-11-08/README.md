# Storage-durability evidence: shared method, environment and cross-cutting findings (2026-11-08)

This directory holds the method, harness, environment and raw data shared by
three per-fix evidence reports:

- [`../storage-durability-55-2026-11-08/`](../storage-durability-55-2026-11-08/README.md): #55, fail-closed write and fsync failures
- [`../storage-durability-74-2026-11-08/`](../storage-durability-74-2026-11-08/README.md): #74, frame-log torn-tail repair and mid-file damage refusal
- [`../storage-durability-67-2026-11-08/`](../storage-durability-67-2026-11-08/README.md): #67, crash-ordered page-log reset and horizon-bounded redo

**This is not production qualification.** It covers one source revision, one
host and synthetic workloads. Power loss and disk failure were emulated, not
real. Green unit tests were not used as evidence; nothing here uses the
repository's own durability tests or its crash child. Production stays on its
last qualified configuration; nothing deployed was touched.

## 1. Provenance

| Item | Value |
|---|---|
| Source revision under test | `main` at `64251c3e2b37d0ffd6a4b30ad50ceaaedb6d7963` ("Durable page-log reset ...", #67). Contains #55 (`e4647d88`) and #74 (`d0cf59b3`). Later merges (#57 at 16:35 UTC the same day, and all later commits) are **not** covered. |
| Server binary | `target/release/bicdb`, sha256 `6844a23a6793e8a63507b2885b106ac8bf2368524420074373dca8d9a02968a2` |
| Verifier binary | `bicdb` built from local merge commit `d2a7fe1cab42bd5dd0838b74df9af36bc7148c09` = `64251c3e` + PR #85 head `2a64e329fd203ca7bfbb46585f6e4475c3d49b37` (PR #85 is not merged; the merge was clean and was not pushed). sha256 `ea1473a8f6247b8433ef2484bd0c6a177183fc69d4221537a2e13d43d7526a46`. Used only for `verify-store --read-only`. |
| Crash child (built, not used) | `target/release/examples/durability_child`, sha256 `721df93f12a6ba39685651cd3d5a70486c71d5fa251f5e8eb799f36f65000763`. Built as asked; the harness does not use it, so the evidence does not depend on the repository's own test children. |
| Build commands | `cargo build --release --locked -p bicdb-cli --bin bicdb`, then `cargo build --release --locked -p bicdb-core --example durability_child` |
| Build flags | Workspace `release` profile (`lto = "thin"`, `debug = "line-tables-only"`, default `opt-level` 3); `CARGO_INCREMENTAL=0`, `CARGO_BUILD_JOBS=6`; no `RUSTFLAGS`; no extra features |
| Toolchain | rustc 1.96.0 (`ac68faa20c58cbccd01ee7208bf3b6e93a7d7f96`, 2026-05-25, LLVM 22.1.2), cargo 1.96.0 (`30a34c682`), self-contained toolchain under `/root/bicdb-durability-s3/` |
| Harness | Python 3.12.3, standard library only. Source in [`harness/`](harness/) |

## 2. Test environment

| Item | Value |
|---|---|
| Host | s3ny, a warm standby host for a production service (that service was not touched). Shared with other build and test jobs during the run (load average 11 to 18). |
| OS / kernel | Ubuntu 24.04.4 LTS, Linux 6.8.0-147-generic, x86_64, 12 cores / 24 threads, 91 GiB RAM |
| Filesystem | ext4 `rw,relatime` on an NVMe partition (`/dev/nvme2n1p2`) |
| Disk | NVMe model `CT2000E100SSD8`, firmware `VDCR4014`; volatile write cache present (`vwc 0x7`, queue `write_cache = write back`), FUA supported |
| fsync | On. The server ran with its default `--storage-sync durable`; nothing set the buffered mode. |
| Isolation | Every campaign ran as a transient systemd unit: `Nice=19`, `CPUWeight=10`, `IOWeight=10`, `MemoryMax=16G`, `MemorySwapMax=0`, `LimitCORE=0`. Disposable data directories under `/root/bicdb-durability-s3/evid/work/` on the same NVMe; deleted after each passing trial. |
| Wall clock | 2026-11-08, trials started 16:04:01Z to 17:43:07Z (plus pilots from 15:37Z) |

## 3. Harness (independent reconciler)

`harness/pgw.py` is a minimal pgwire (protocol v3) client written for
this evidence from the protocol specification (startup, simple query,
ErrorResponse fields). No BicDB code and no client library are involved.
`harness/trial.py` runs one trial:

1. `bicdb init` (with `BICDB_STORAGE_MODE=server_paged` for paged), check
   `format.json` records the intended mode, start `bicdb serve`, create:
   `evr(id text PRIMARY KEY, txn text, writer int4, k int8, payload text)`,
   `CREATE INDEX evr_writer ON evr(writer)`,
   `CREATE UNIQUE INDEX evr_k ON evr(k)`,
   `evhot(writer int4 PRIMARY KEY, seq int8, txn text)` with one row per
   writer. Graceful stop.
2. **Cycle 1.** Restart with the fault armed. Six concurrent clients, each
   one connection. Writer `w`'s transaction `i` (`tok = w<w>-<i>`):
   `BEGIN`; `SELECT txid_current()`; insert rows `tok:m`, `tok:0`, `tok:1`
   with unique `k` and a payload derived from the id (64 B to 2 KiB);
   `UPDATE evhot SET seq = i, txn = tok WHERE writer = w`; every fifth
   transaction `DELETE FROM evr WHERE id = 'w<w>-<i-3>:0'`; `COMMIT`.
3. **Client ledgers.** Each client appends JSON lines to its own file in a
   ledger directory that is a sibling of the database directory, `fsync`ed
   after every line: `attempt` (token, cycle, planned delete) before `BEGIN`;
   `commit` (with the `txid_current()` value) right before `COMMIT` is sent;
   `result` with `ok`, or `error` with SQLSTATE, message, DETAIL and HINT, or
   `lost` (connection dropped). After its first 08007/58030 a client makes at
   most three more attempts. The reconciler reads the ledgers back from disk,
   never from memory.
4. The cycle ends when the server exits by itself (crash point), 1 s after the
   first 08007/58030 (fence) followed by SIGKILL, or at the time limit (6 s by
   default) by SIGKILL, or (`--end term`) by a graceful SIGTERM shutdown with
   the fault still armed.
5. Optional tampering (torn tail, mid-file damage) or a crash during recovery,
   then `verify-store --read-only` on the crashed directory, a copy of the
   crashed directory, and a restart **without** faults.
6. **Reconcile 1** over pgwire (below). Graceful stop.
7. **Cycle 2.** More writes on the recovered store, SIGKILL at a random time,
   copy of the crashed directory, `verify-store`, restart, **reconcile 2**
   (also against reconcile 1), graceful stop, `verify-store` again.

Reconciliation checks, all from the ledgers and ordinary SQL:

| Check | Rule |
|---|---|
| (a) | Every `ok` transaction is present with exactly its row set: marker and `:1` rows present, `:0` present unless a committed later transaction of the same writer deleted it; every column equal to the value the client wrote; `evhot.seq`/`txn` equal the writer's newest committed transaction. |
| (b) | Every transaction that failed with any SQLSTATE other than 08007 is absent. A transaction whose `COMMIT` was never sent is absent. |
| (c) | Every 08007 carries `incarnation`/`commit_seq`/`transaction_id` in DETAIL; the `transaction_id` equals the `txid_current()` the client recorded; `SELECT bicdb_commit_outcome(incarnation, commit_seq)` after recovery answers `committed` or `not_committed` (never `unresolved`) and matches presence; the answer is the same at reconcile 2. |
| (d) | No row exists that no ledgered transaction wrote; no partial transaction (marker without `:1` or the reverse); a deleted row is not back. |
| (e) | Per writer, `WHERE writer = w` (plan `IndexScan evr_writer`) and `WHERE k >= .. AND k < ..` (`IndexRangeScan evr_k`) return exactly the rows of the full scan; 40 primary-key lookups of present ids and 40 of absent ids; 40 unique-index equality lookups; a duplicate-`k` insert must fail with 23505; `count(*)` equals the scan. Plans are captured with EXPLAIN in every trial. |
| (f) | `bicdb verify-store --read-only` (PR #85 binary) before recovery 1, before recovery 2 and after the final clean shutdown; every file's sha256 is compared before and after each verifier run. |
| fence | Within one server incarnation, no client receives `COMMIT` success after it received 08007 or 58030. |
| stability | Transactions present at reconcile 1 are present at reconcile 2 and the reverse for cycle-1 transactions. |

In-flight transactions (`COMMIT` sent, connection lost) may be present or
absent but must be atomic; they are counted, not judged.

**Negative controls** (`control-*`, 2 trials each): between recovery and
reconcile the harness damages the database through SQL (deletes an
acknowledged transaction's rows, inserts a row nobody wrote, rolls back a hot
row, corrupts a payload, re-inserts a deleted row). The reconciler reported
the expected violation in 10 of 10 control trials, so the checks can fail.

### Profiles

| Profile | Settings | Purpose |
|---|---|---|
| `default` | Server defaults: 256 MiB buffer pool, 256 MiB page-log threshold, 64 MiB segments, 4 GiB auto-compaction threshold | The configuration production runs |
| `stress` | `BICDB_PAGED_BUFFER_POOL_BYTES=2097152`, `BICDB_PAGED_WAL_MAX_BYTES=524288`, `BICDB_PAGED_WAL_SEGMENT_BYTES=131072`, `BICDB_AUTO_COMPACT_WAL_MB=1` | Makes eviction, checkpoints, segment seals, page-log resets and transaction-log cuts happen within seconds, so the fixed boundaries are reached |
| `smallpool` | Only the 2 MiB pool | Diagnostic |
| `smallwal` | Only the small page-log threshold, segments and compaction threshold | Diagnostic |

The fixed boundaries are reached only in `stress`; so most numbers below are
for a deliberately non-production configuration.

### What was emulated and what was real

| Event | How |
|---|---|
| Process crash | Real `SIGKILL` of the server process group at a random time, or `abort()` at a named `BICDB_TEST_CRASH_AT` point. Unsynced data stays in the page cache, as with a real process crash. |
| Power loss | **Emulated** by `BICDB_TEST_WAL_VOLATILE_TAIL=1`: at the crash point, the page log's unsynced tail is truncated to its last synced offset. No real power cut, no device cache loss; other files keep their unsynced bytes. |
| fsync / write failure | **Emulated** by `BICDB_TEST_FAIL_IO=<site>:<n>[+]`: the n-th call of the site returns EIO without doing the I/O (`+` keeps failing). No real device error; the kernel's real post-fsync-failure page-cache behaviour was not exercised. |
| Torn tail | **Emulated** after SIGKILL: a strict prefix of the last complete frame, or that whole frame with one payload byte flipped, appended to one frame log (never modifying synced bytes). |
| Mid-file damage | **Emulated** after SIGKILL: one byte flipped inside a frame followed by at least one complete frame. |
| Crash during recovery | `BICDB_TEST_CRASH_AT` armed on the restart after a crash. |

## 4. Totals

1,255 trials, 2,509 crash recoveries reconciled (1,939 `server_paged`, 570 `embedded_memory`). Every fault-injection
trial records whether its fault fired (crash point message, fence marker,
`durability.write_failed` event or SQLSTATE seen); only fired faults count as
coverage. Raw per-trial records (one JSON line per trial, all boundaries, gzip): [`raw/trials.jsonl.gz`](raw/trials.jsonl.gz);
per-boundary aggregates: [`summary-all-boundaries.json`](summary-all-boundaries.json).

## 5. Cross-cutting findings (not specific to one fix)

No failure in this campaign was attributable to a boundary injected for #55,
#74 or #67: across the 497 recoveries of a fired boundary
(reconcile 1 of a trial whose fault fired, `stress` profile) there were no
violations. **Every failure followed a plain SIGKILL** (or a crash point that
did not fire, which is the same thing) on a `server_paged` store:

| Profile (`server_paged`) | Plain-SIGKILL recoveries | Failed |
|---|---|---|
| `default` | 100 | 0 |
| `smallpool` | 80 | 0 |
| `smallwal` | 200 | 1 (refused) |
| `stress` | 1,042 | 11 |
| `embedded_memory`, all profiles | 570 (all recoveries) | 0 |

That is 12 failures. 11 of them have a saved crash state (the first,
`f55-txlog-cut-paged` rep 3, ran before the harness copied crashed
directories); each was replayed twice (recovery from a fresh copy of the
crashed directory): **every replayed outcome is deterministic**
([`raw/replay-saved-crash-states.txt`](raw/replay-saved-crash-states.txt)).
They fall into four classes:

| Class | Count | Example | Fails closed? |
|---|---|---|---|
| F1. Silent loss after a successful open | 1 | `f74-midfile-synclog-paged` rep 13 (the pristine copy of cycle 1, not the damaged one): recovery opens normally, `evr` returns 12 rows, **445 acknowledged transactions are missing**, 2 of 6 `evhot` rows are gone and the other 4 come back with no column values (the whole row as one JSON value), and the unique index no longer refuses a duplicate `k`. Verifier: `inconsistent`, 2,946 committed live versions unreachable. | **No** |
| F2. Secondary index silently disagrees with the heap | 6 | `f55-crash-commit-paged` rep 15: after recovery 205 acknowledged rows are in the heap and the primary-key index but missing from `evr_writer`; others miss 1 to 3 entries in `evr_writer` and/or `evr_k`. Persists across further clean restarts. | **No**: queries through the index silently miss committed rows |
| F3. Open refused: `page N is beyond the end of the file (M pages)` | 3 | `all-sigkill-paged-stress` rep 5; matches the known "steal without undo" gap (GAP-1). | Yes (refusal) |
| F4. Open refused with `page 362 has type 2, expected 6`, or a **panic** (`crates/bicdb-page/src/btree.rs:430:1`, `TryFromSliceError` unwrap) | 1 + 1 | `diag-sigkill-paged-smallwal` rep 25 (256 MiB pool, so not pool eviction); `f67-crash-checkpoint-paged` rep 5 | Yes, but the panic is an abort, not a reported corruption |

9 of the 12 occurred at the second recovery, after a store that had already been
recovered once took more writes and was killed again.

The documentation (`docs/durability-failure-handling.md` section 10) says
the GAP-1 recovery defect "fails closed (the open is refused; nothing is
silently discarded)". Classes F1 and F2 show that, at this revision, the same
or a related defect can also open successfully with acknowledged rows missing
or with secondary indexes incomplete. These classes are consistent with
recovery of pages written before their transaction committed (eviction or
checkpoint writeback); that was not root-caused here. Open PRs #81 (GAP-1
interim) and #82 (full design) target that area; this evidence should be
re-run on them.

### Verifier (PR #85) observations

- On the crashed state before a failing recovery it reported `inconsistent`
  or `needs_external_cross_check` in 9 of the 10 cases it was run on (one of
  them a manual run on the saved state of `f74-torn-txlog-paged` rep 3; the
  in-trial pre-recovery-2 run was added after that trial). It reported
  `clean` before `f67-recoverycrash-paged` rep 25, whose recovered store then
  missed 2 `evr_k` entries.
- After recovery and a clean shutdown it reported `clean` for 5 of the 6 F2
  stores (1 to 3 missing secondary-index entries); it reported `inconsistent`
  only for the 205-entry case. **It does not detect small secondary-index
  omissions on a recovered store**, so a clean verifier report is not
  evidence of index completeness.
- It left every file byte-for-byte unchanged in all 2,707 in-trial runs.

### Saved states

Kept on s3ny under `/root/bicdb-durability-s3/evid/keep/` (139 MiB): for each
failing trial the crashed directories (`crashed-1`, `crashed-2`), the ledgers,
the recovered database and the server log. Directory names are the trial
names, except `null-13` (`f74-midfile-synclog-paged` rep 13),
`gap1-reopen-refused-1` (`f74-torn-txlog-paged` rep 3) and
`idx-crash-commit-15` (`f55-crash-commit-paged` rep 15). `null-13/crashed-1` is the cycle-1
crash state reconstructed from the damaged copy by restoring the single
flipped byte (CRC-verified with `harness/unflip.py`).

## 6. Pilots and harness corrections

Pilots (`/root/bicdb-durability-s3/evid/pilot-results/`, not counted) found
three harness defects, fixed before the counted campaign: `bicdb serve` alone
creates `embedded_memory` stores (fixed with `bicdb init` under
`BICDB_STORAGE_MODE`, checked through `format.json`); the no-ack-after-fence
rule was applied across server restarts; one control used the wrong `k`.
During the counted campaign one trial ended with a harness exception
(`f74-midfile-synclog-paged` rep 13: an `evhot` row with NULL columns); that
trial is the F1 case above, reconstructed and replayed separately. The
reconciler was then made to report unreadable rows as violations.
