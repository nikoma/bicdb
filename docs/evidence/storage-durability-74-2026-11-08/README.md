# Post-merge evidence: #74 frame-log torn-tail repair and mid-file damage refusal (2026-11-08)

**Not production qualification.** Torn writes and media damage were emulated
by editing files of a SIGKILLed server, and append failures by injected EIO,
on one host and one revision with a synthetic workload. No real torn write
and no real power loss. Production stays on its last qualified
configuration.

Method, harness, environment, build and the cross-cutting findings are in
[`../storage-durability-harness-2026-11-08/README.md`](../storage-durability-harness-2026-11-08/README.md).

| | |
|---|---|
| Fix | #74, merge commit `d0cf59b3` |
| Source revision tested | `main` `64251c3e2b37d0ffd6a4b30ad50ceaaedb6d7963` (includes #55, #74, #67) |
| Server binary sha256 | `6844a23a6793e8a63507b2885b106ac8bf2368524420074373dca8d9a02968a2` |
| Verifier binary sha256 | `ea1473a8f6247b8433ef2484bd0c6a177183fc69d4221537a2e13d43d7526a46` (main + unmerged PR #85, merge `d2a7fe1c`) |
| Environment | s3ny, Linux 6.8.0-147-generic, ext4 on NVMe `CT2000E100SSD8` (volatile write cache, FUA), server `--storage-sync durable` (fsync on) |
| Trials | 300 |

## Claim tested

A failed, short or unsynced append to a generic frame log (`sync.log`, the
event log, record segments; the transaction log's own frames) is cut back:
the commit aborts with an ordinary error and nothing of it remains; a failed
cut-back fences (`frame_log.repair`). At open, a torn or damaged **last**
frame is cut to the last good frame and nothing acknowledged is lost;
damage **followed by valid frames** refuses the open and leaves the file
byte-for-byte unchanged.

## Injected failure boundaries

| Kind | How | Files |
|---|---|---|
| Append failure | `BICDB_TEST_FAIL_IO` `frame_log.write`, `frame_log.short_write` (half a batch written, ending inside a frame), `frame_log.sync`, one-shot, `n` = 5..300 | Whichever frame log makes the n-th append (in practice `sync.log` and record segments) |
| Failed cut-back | `frame_log.short_write:n` plus `frame_log.repair:1` | as above |
| Torn tail | After SIGKILL, append to one frame log either a strict prefix of its last complete frame (57 trials) or that whole frame with one payload byte flipped (63 trials). Synced bytes are never modified. | `transactions.log` 53, `sync.log` 36, `segments/*.seg` 31 |
| Mid-file damage | After SIGKILL, flip one byte inside a frame that has at least one complete frame after it. The pristine directory is copied first. The open must refuse; the damaged file's sha256 must not change; then the pristine copy is restored and reconciled. | `transactions.log` 36, `sync.log` 25, `segments/*.seg` 28 |

The frame walker in the harness is independent (`BICF` magic, version 1,
16-byte header, little-endian length, CRC-32 over version, kind, flags,
length and payload).

## Results

| Boundary | Mode / profile | Injected failure | Trials | Fired | Result | Acked | Failed (absent) | 08007 committed / not | In flight: present / absent | Violations | Verifier exit codes pre / pre2 / post |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `f74-framefail-embedded` | embedded / stress | EIO at `frame_log.write`, `frame_log.short_write`, `frame_log.sync` n=5..300 | 30 | 30 | all pass | 132670 | 30 | 0 / 0 | 58 / 161 | none | n/a / n/a / n/a |
| `f74-framefail-paged` | paged / stress | EIO at `frame_log.write`, `frame_log.short_write`, `frame_log.sync` n=5..300 | 30 | 28 | all pass | 24715 | 28 | 0 / 0 | 305 / 20 | none | 0:30 / n/a / 0:30 |
| `f74-framerepairfail-embedded` | embedded / stress | EIO at `frame_log.short_write` n=5..300 (plus `frame_log.repair:1`) | 15 | 15 | all pass | 39860 | 360 | 0 / 0 | 15 / 39 | none | n/a / n/a / n/a |
| `f74-framerepairfail-paged` | paged / stress | EIO at `frame_log.short_write` n=5..300 (plus `frame_log.repair:1`) | 15 | 15 | all pass | 8772 | 292 | 68 / 0 | 81 / 5 | none | 0:15 / n/a / 0:15 |
| `f74-midfile-any-embedded` | embedded / stress | one byte flipped mid-file in `any frame log` | 15 | 15 | all pass | 87733 | 0 | 0 / 0 | 30 / 83 | none | n/a / n/a / n/a |
| `f74-midfile-any-paged` | paged / stress | one byte flipped mid-file in `any frame log` | 15 | 15 | all pass | 12523 | 0 | 0 / 0 | 144 / 10 | none | 0:15 / 0:15 / 0:15 |
| `f74-midfile-segments-embedded` | embedded / stress | one byte flipped mid-file in `segments` | 15 | 15 | all pass | 67837 | 0 | 0 / 0 | 28 / 85 | none | n/a / n/a / n/a |
| `f74-midfile-synclog-paged` | paged / stress | one byte flipped mid-file in `sync.log` | 15 | 14 | 1 harness error | 9070 | 0 | 0 / 0 | 145 / 10 | none | 0:13 3:1 / 0:14 / 0:14 |
| `f74-midfile-txlog-embedded` | embedded / stress | one byte flipped mid-file in `transactions.log` | 15 | 15 | all pass | 63912 | 0 | 0 / 0 | 28 / 75 | none | n/a / n/a / n/a |
| `f74-midfile-txlog-paged` | paged / stress | one byte flipped mid-file in `transactions.log` | 15 | 15 | all pass | 9463 | 0 | 0 / 0 | 161 / 11 | none | 0:15 / 0:15 / 0:15 |
| `f74-torn-any-embedded` | embedded / stress | torn tail appended to `any frame log` | 20 | 20 | all pass | 85207 | 0 | 0 / 0 | 36 / 115 | none | n/a / n/a / n/a |
| `f74-torn-any-paged` | paged / stress | torn tail appended to `any frame log` | 20 | 20 | all pass | 16700 | 0 | 0 / 0 | 203 / 16 | none | 0:20 / 0:20 / 0:20 |
| `f74-torn-segments-embedded` | embedded / stress | torn tail appended to `segments` | 20 | 20 | all pass | 138093 | 0 | 0 / 0 | 39 / 111 | none | n/a / n/a / n/a |
| `f74-torn-synclog-paged` | paged / stress | torn tail appended to `sync.log` | 20 | 20 | 1 reopen refused | 16669 | 0 | 0 / 0 | 198 / 7 | none | 0:20 / 0:19 2:1 / 0:19 |
| `f74-torn-txlog-embedded` | embedded / stress | torn tail appended to `transactions.log` | 20 | 20 | all pass | 138981 | 0 | 0 / 0 | 34 / 112 | none | n/a / n/a / n/a |
| `f74-torn-txlog-paged` | paged / stress | torn tail appended to `transactions.log` | 20 | 20 | 1 reopen refused | 16667 | 0 | 0 / 0 | 199 / 10 | none | 0:20 / n/a / 0:19 |

Columns as in the #55 report. Totals over the #74 boundaries (300 trials,
297 fired):

- **Append failures** (58 fired of 60): the failing commit returned an
  ordinary error (`XX000`) and was absent; no fence, no marker; later
  commits were acknowledged and survived two recoveries. 0 violations.
- **Failed cut-back** (30 of 30): fenced with site `frame_log.repair`;
  paged stores also reported 08007 for 68 in-flight commits, all resolved
  `committed` and present; 0 violations.
- **Torn tail** (120 of 120 applied): the open succeeded in 120 of 120 and
  reconcile 1 was clean in 120 of 120 (every acknowledged transaction
  present, nothing extra). 2 trials failed later, at the second recovery
  after a plain SIGKILL (`page N is beyond the end of the file`, class F3 of
  the cross-cutting findings), not at the torn-tail recovery.
- **Mid-file damage** (89 applied and recorded): the open was **refused in
  89 of 89**, naming the file, the damage offset and the next valid frame;
  the damaged file was byte-for-byte unchanged in 89 of 89. After restoring
  the pristine copy, 88 of 89 reconciled clean. The 89th
  (`f74-midfile-synclog-paged` rep 13, recorded as a harness error) is the
  silent-loss case F1: the **undamaged** cycle-1 crash state itself recovers
  to a store missing 445 acknowledged transactions. It is unrelated to the
  injected damage, which was refused correctly.
- 868,872 acknowledged commits over both cycles; 0 acknowledged missing at
  the recovery of the injected boundary; 0 of 710 failed commits present.

## Observations

1. **A refused open is not side-effect free.** Before refusing a damaged
   frame log the open already:
   - appended to `incarnations.log` (53 of 89 refusals);
   - on paged stores, ran page recovery and a checkpoint: `paged/store.pages`
     changed in 41 of 44 refusals and sealed page-log segments were removed
     in most. With `transactions.log` damaged on a paged store,
     `store.pages` changed in 14 of 15 refusals, so the page store has
     already advanced from a log the open then refused.
   The damaged file itself is untouched, as claimed. If an operator then
   salvages the damaged log (`salvage_frame_log` drops everything after the
   damage), the page store may already contain effects of the dropped
   frames; that path was not tested.
2. The event log (`events/events.seg`) and the record-audit outbox were not
   exercised: this SQL workload writes no events, so the file never held a
   frame.
3. The verifier does not read frame logs; it reported `clean` on stores
   whose `transactions.log` or `sync.log` was damaged.

## Remaining risks

- Torn writes and damage were produced by editing files after a kill, not by
  a real power loss or media error; real torn sectors may tear at other
  boundaries (for example inside the 16-byte header across a 4 KiB page).
- `salvage_frame_log` and the operator procedure after a refusal were not
  exercised.
- Event-log and audit-outbox frame logs, encrypted and compressed frames
  (flags `zstd`/`encrypted`) were not covered.
- Plain-SIGKILL recovery of `server_paged` stores on the `stress` profile is
  not reliable at this revision (harness README section 5), and 3 of the 300
  trials here hit it.
