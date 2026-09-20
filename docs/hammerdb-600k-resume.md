# HammerDB 600k campaign — September 4, 2026

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

## Current continuation — September 5

The correctness and >=600k goal remains open. Historical uses of "valid"
below describe the gates available at the time, not a completed financial
or durability qualification. Later audits found a shared Payment procedure
defect and a full-scale recovery memory failure. Exact successful call counts
alone do not establish correct business state.

Continue from [compact recovery validation](recovery-compact-validation-2026-09-05.md),
[the Payment financial audit](payment-financial-audit-2026-09-05.md), and
[repository consolidation](correctness-consolidation-2026-09-05.md).
The latest 569,136 NOPM is a corrected-Payment, buffered-tmpfs diagnostic.
Its full financial audit now passes after the recovery repair, but its
storage configuration still does not qualify the performance goal.

## Historical campaign record

Target: at least 600,000 database-verified NOPM without dropping correctness,
durable-path work, checkpoints, or the workload mix. Benchmark host:
`benchmark@192.0.2.11`; immutable 16-warehouse seed `/dev/shm/bicdb-tpcc-seed`.
The tmpfs runs exercise durable code, not power-loss durability.

These are short screening trials (32 VUs, one minute ramp plus one minute
measurement), not the full repeated acceptance campaign. District-sum NOPM
covers both minutes; HammerDB reports its timed minute separately.

## Recent screening evidence

Remote artifacts are under `/home/benchmark/bench-out/<campaign>/trials/<trial>/result.json`.
Each campaign below is `codex-<trial>`.

| Trial | District-sum NOPM | HammerDB NOPM | Interpretation |
| --- | ---: | ---: | --- |
| alloc-cuts-r3 | 467,543.5 | 482,986 | Earlier clean-of-profiling reference |
| arch-cuts-r1 | 461,490 | 475,442 | Snapshot cache, disabled-profile cut, inline MVCC: no established gain |
| pgo-baseline-r1 | 482,475.5 | 483,463 | PGO of `51fa7414`; promising single run, unconfirmed |
| no-max-map-r1 | 448,403.5 | 452,334 | `13c09c89`; MVCC map removal reduced memory but screened slower |
| shard-trace-r1 | 455,539.5 | 458,701 | `0c3c763a`; releasing diagnostic shard locks individually is not a demonstrated throughput win |
| pgo-baseline-r2 | 489,886 | 493,694 | First post-fix valid screening; 33 successful VUs, no failed queries, zero watermark gap |
| shard-trace-r2 | 452,028 | 456,519 | Valid immediate parent of UPDATE projection candidate |
| update-project-r1 | 470,969 | 476,856 | Valid, +4.19% vs immediate parent; abort mix changed, not yet a keeper |
| alloc-cuts-r4 | 464,819.5 | 472,696 | Valid repeat of the pre-architecture reference |
| update-slim-r1 | 478,071 | 483,231 | Valid; `4a060d46`; physically pruned downstream UPDATE layout |
| update-batch-r1 | 467,590.5 | 472,772 | Valid; `74aff85c`; lower NOPM but more total commits and fewer handled aborts |
| join-cuts-r1 | 477,027.5 | 481,244 | Valid; `ffdbde7f`; 1,437,841 write commits, 564,007 serialization failures |
| bound-case-r1 | 485,128.5 | 489,571 | Valid; `1f994333`; 416,987 committed Payments / 967,968 calls |
| bound-borrow-r1 | 493,599.5 | 494,979 | Valid; `bae9da9b`; 365,435 committed Payments / 986,972 calls; not an unqualified gain |
| join-move-r1 | 495,583.5 | 499,658 | Valid; `6891acba`; 384,094 committed Payments / 991,780 calls |
| typed-num-r1 | 488,480 | 491,461 | Valid; `05b7d18b`; 429,044 committed Payments / 979,268 calls |

The batch candidate completed 1,478,011 write commits versus 1,398,294 for
the slim candidate (+5.70%), with 488,076 versus 611,570 serialization failures
(-20.19%). It is not a NOPM winner. This is an example of why neither a higher
headline NOPM nor a lower abort count establishes the overall improvement.

Saved subsequent binaries (not acceptance results): `ffdbde7f` join cuts,
SHA256 `12d2df1d1dea58bee3dedb06757fe4077eadf29cf4817dd0fd51b032e4384a89`;
`1f994333` lazy type-validated bound CASE,
SHA256 `d3dedc8b50688229d57effc1c499016a8d46255bab4b803c094f4ccd17e3767f`.

## PostgreSQL 18.6 reference

User-requested same-host reference, using `bench/tpcc/postgres_reference.sh`.
Official Docker image digest:
`sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280`.
The actual server reports PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2).
Fresh HammerDB 16-warehouse seed; same HammerDB image, stored-procedure driver,
32 active VUs, one-minute ramp plus one-minute measurement, tmpfs data/WAL.
The random data is independently generated, not byte-identical to BicDB's seed.
Settings: fsync, synchronous_commit and full_page_writes on; shared_buffers 8GB,
max_wal_size 4GB, checkpoint_timeout 5min, jit off, track_functions all.
Autovacuum remains enabled. Full settings/image/configuration evidence is saved.

`/home/benchmark/bench-out/codex-pg186-r1`: 587,655 district-sum NOPM over both minutes,
596,282 HammerDB timed NOPM, 1,373,906 reported TPM. All 33 VUs succeeded,
zero failed VUs and exit code zero. District sum 480,160 -> 1,655,470:
1,175,310 committed NewOrders. Database commits increased by 2,706,413, including
read-only transactions and a few observer queries; top-level rollbacks were zero.
That counter is NOT directly comparable to BicDB's write-commit sequence.
Procedure stats record 1,177,770 Payment calls, but calls alone cannot prove
business work. The first Payment probe, SUM(c_payment_cnt), remained unchanged
and is therefore not useful for this workload; follow-up trials measure the
change in history row count instead. Do not interpret the unchanged counter as
zero successful Payments. Procedure source inspection and repeat runs remain due.

The PostgreSQL runner preserves all data/logs, binds only loopback, stops its
own containers, and never modifies an existing cluster. The stopped immutable
seed is `/dev/shm/codex-pg186.AUDahD`; first trial data is
archived at `/home/benchmark/bench-out/codex-pg186-r1/data` and second-trial data at
`/home/benchmark/bench-out/codex-pg186-r2/data` (moved off tmpfs after shutdown; the original
`data_path` files record the measurement-time paths). One failed initial seed startup is preserved separately; it did
not run the benchmark (parent-mount permissions were corrected).

`codex-pg186-r2`: 653,059 district-sum NOPM; 660,600 HammerDB NOPM and
1,519,403 reported TPM. District sum 480,160 -> 1,786,278, hence 1,306,118
committed NewOrders. History rows 480,000 -> 1,785,532, hence 1,305,532 committed
Payments: exactly the Payment procedure-call count. All 33 VUs succeeded, zero
failed VUs/exit status zero, zero database top-level rollbacks. This run is 11%
faster than r1 and is NOT a stable single-number PostgreSQL baseline yet.
The procedure source confirms Payment never increments c_payment_cnt in this
HammerDB version; history-row growth is the correct completed-work check.

`bae9da9b` borrowed-expression candidate is saved as `bicdb-bound-borrow`, SHA256
`31a30f7396edad7d255c4cc23d51fd97ed1dc9cc30c8cb88643c49d4b0c5eae8`.
`6891acba` transfers owned left rows and last-use right-row values through
prepared primary-key joins, instead of cloning all upstream values at every
join. It passes 414 SQL integration tests, 17 cell-row tests (including duplicate
right keys, residual predicates, outer joins and missing/NULL keys), and 268
library tests (three ignored). No throughput gain is assumed from these tests.

`05b7d18b` retains numeric coefficient/scale between arithmetic nodes (no
resident-record cache); overflow and unsupported operations still use the
existing evaluator. 414 SQL + 17 cell-row tests and focused decimal boundary /
overflow tests pass. Saved full binary SHA256:
`2efc07f9d43a9653f2cfb6ce1e49363800fb79f925a29d3823c7cc819f55dbe4`.
Its first screen raises completed NewOrder+Payment work by 2.24% versus join-move,
while NOPM alone falls 1.43%. Single screens do not establish a keeper.

The user explicitly emphasized reliable commits as an additional important
target and questioned whether failed calls might be MORE expensive than
successful ones. Counts do not resolve that question. `4025fac0` adds opt-in
`BICDB_ROUTINE_OUTCOME_TRACE=1` diagnostics: per-thread handled exceptions,
discarded buffered writes and rollback time; server timing spans execution
through commit/WAL durability but excludes outer admission/network. Outcome
labels describe handlers, not inferred business commits. Pair this with the
existing `BICDB_LOCK_FAIL_TRACE=1`; instrumented runs are diagnostic, not
acceptance throughput evidence. `8f423e07` adds default-OFF
`BICDB_RC_FIRST_LOCK_WAIT=1`, applying the existing first-row reservation wait
policy to ordinary READ COMMITTED updates. Once a row lock is held, the usual
deadlock policy and attempt budget remain in force. Tests include owner release,
retained second-lock deadlock prevention, RC UPDATE-FROM rechecks, concurrent
NewOrder/Payment order-ID uniqueness, lost-update stress and checkpoint recovery.

Procedure-body comparison: all five PostgreSQL and BicDB bodies match after
removing BicDB's CREATE PROCEDURE wrapper and normalizing whitespace/token case.
Sources are `codex-pg186-r2/procedures.csv` and
`codex-bound-borrow-r1/trials/bound-borrow-r1/raw/procedures.csv` on the VM.

The pre-r2 screenings had 33 successful VUs and zero server failed queries, but
the HammerDB container's interactive `quit` cleanup throws
`can't rename "::_unknown": command doesn't exist` **after**
`ALL VIRTUAL USERS COMPLETE`. The harness correctly marks its nonzero process
exit invalid. Retain those records unchanged as diagnostic evidence; do not
promote them to valid acceptance results. The batch template now uses `exit 0`
after `vudestroy`, avoiding interactive readline cleanup. Independent VU, server,
watermark, and harness-error checks remain mandatory. Eight summarizer tests pass.

The fresh frame-pointer profile uses source `0c3c763a`, remote binary
`/home/benchmark/bench-logs/bicdb-fresh-fp`, campaign `codex-fresh-profile-r1`.
`perf-steady.data` is the workload sample; the earlier `perf.data` overlaps startup.
This entire run is diagnostic and must not enter throughput comparisons.

Fresh profile: 85,993 task-clock samples, no lost samples. Stored UPDATE is
24.30% inclusive, commit 8.21%, StoredRecord cell reading 4.43%; allocator
`_mi_page_malloc_zero` 7.04% self and `mi_free` 3.57% self. UPDATE cloned entire
candidate rows solely to call a disabled profiler. Candidate work removes that
clone and caches a conservative UPDATE read-dependency plan, leaving unread
slots NULL without modifying untouched stored fields. Unknown expression shapes
retain full materialization. No throughput claim until its tests and A/B pass.

`9a66d95e` (binary SHA256
`cef80b34a84faab3318203979606001ca49744de727a3882bd4f97d40c410991`)
passed 15 cell-row tests and 266 library tests (three ignored). Its first screen
increased serialization failures from 479,851 to 630,654 while total commits
fell from 1,419,698 to 1,349,436; the NOPM increase must therefore be interpreted
alongside the work mix. The follow-up `4a060d46` carries a physically smaller
slot layout downstream and passes all 414 SQL integration tests. A subsequent
candidate batches prepared UPDATE-FROM target reads and adds a regression test
for missing, NULL and duplicate source keys (16 cell-row tests pass).

## Decision discipline

### September 4: commit-outcome diagnostic pair

Source `8f423e07`, stripped binary SHA256
`2faa55fa6d57a0ef1617d925b5cd4c275d776381d8bb0df09464886320bcc86c`.
Both runs enable routine-outcome and lock-failure tracing, `PROFILE_KIND=latency`;
they pass harness validity but are excluded from acceptance throughput.

| Trial | Verified NOPM | Committed Payments / calls | NewOrder + Payment commits |
| --- | ---: | ---: | ---: |
| outcome-base-d1 | 479,503 | 448,676 / 960,081 (46.73%) | 1,407,682 |
| outcome-first-d1 | 310,449 | 538,994 / 620,159 (86.91%) | 1,159,892 |

Campaigns are `codex-<trial>` on the benchmark VM. First-row waiting improves
Payment completion probability, but NOPM falls 35.3% and combined committed
business work falls 17.6%; it is NOT a performance win. Both used default
old-dies and the canonical RC attempt budget of 2 after the first lock.
Baseline failures are mostly early warehouse/district rejection (about 186 us
per failed Payment versus 1,007 us per clean return in the last cumulative
trace). With first-row waiting, the later district conflict is much more
expensive (about 2.5 ms with one buffered write discarded, near the end).
This confirms that failure cost depends strongly on where it occurs.

The uninstrumented older PGO repeat `pgo-baseline-r3` gives 485,696 NOPM,
370,503 committed Payments / 972,851 calls. Typed-num's prior screen completes
4.78% more combined NewOrders + Payments with similar NOPM; still only a screen,
not a balanced acceptance comparison.

Next candidate: default-off `BICDB_ROW_LOCK_POLICY=wait-graph` tracks live
wait edges, rejecting cycles instead of transaction age. Harness now accepts
`RC_UPDATE_LOCK_ATTEMPTS` (default still 2) so a longer downstream wait can be
measured explicitly. Guard cleanup, simultaneous cycles, duplicate concurrent
edges, actual row-lock cycles, acyclic second waits and timeouts need validation.
No performance claim yet. A separate PGO build froze source `8f423e07` before
these changes and uses the older `51fa7414` training profile; keep provenance
separate and measure whether the profile remains useful.

Wait-graph source `75181352`, stripped binary SHA256
`2e85b10a054ca4648f54cc0cb99791d1b2cf03dab758f7c7b78a948b359156dd`.
Five graph/row-cycle tests, nine concurrency tests, 21 checkpoint tests, the
RC recheck test and the strengthened concurrent Payment/NewOrder test pass.
The latter now checks district YTD against exactly the committed Payments.
Old-policy long-budget diagnostic `outcome-old-long-d1` (20,000 RC attempts,
first-row waiting enabled) reaches 291,672 NOPM and 533,444 Payments / 582,793
calls (91.53%); combined NewOrder+Payment commits 1,116,788. Not a win.

Recovery finding while testing a separate detached-rowid checkpoint candidate:
the deletion/reinsertion crash test ALSO fails on the original checkpoint code.
An unconditionally applied legacy segment-supersession heuristic skips a modern
committed INSERT after a replayed DELETE. Restricting that compatibility rule
to commit-sequence-zero legacy frames fixes the reproducer. Full core suite:
386 passed, three ignored; expanded checkpoint tests: 23 passed, including
reinsertion after segment materialization with and without WAL truncation.
Keep this correctness fix separate from checkpoint performance work. The
detached-rowid checkpoint candidate is temporarily reverted for baseline
comparison; its patch is retained in the tool-session store for restoration.

Current-source PGO build (source `8f423e07`, NOT graph/recovery changes) finished;
stripped SHA256 `54f1a9d6a60e794e2408e41f882dc514c15c5516b9898440b0108aee961f2d71`.
Local artifact `/tmp/bicdb-current-pgo`; old training profile yields many missing
function warnings. No benchmark result yet.

Subsequent results:

- `outcome-graph-d1`: 302,082.5 verified NOPM, 605,541 committed Payments out of
  exactly 605,541 calls, 604,165 NewOrders / 604,165 calls, zero serialization
  failures. Combined successful NewOrders+Payments 1,209,706. This improves
  completion and beats the old-policy long-wait control but NOT the fast-abort
  baseline on throughput. 16.40 average cores, 10,711,631 context switches.
- `current-pgo-r1`: valid uninstrumented screen, 501,420 verified NOPM, HDB
  495,031; 512,297 Payments / 1,005,514 calls, 1,002,840 committed NewOrders,
  530,208 handled serialization failures, 20.96 cores. Combined business commits
  1,515,137. Best short screen so far; not repeated acceptance and not 600k.

Checkpoint candidate restored after the independent recovery fix; source
`12f84968`, stripped binary SHA256
`da363170b8bb4b972c4675f906cc91ecb344ed1405231b0fc196a2f882d0faba`, local
`/tmp/bicdb-rowid-checkpoint`. Phase 0 detaches dirty hash tables instead of
allocating millions of string keys. Phase 1 clones record Arcs by RowId in
256-row shard-lock chunks; absent post-boundary locators rely on retained WAL
deletions. Dirty-count metadata is now a locator upper bound, not deduplicated
logical key count. Full core suite: 387 passed, three ignored, including the
delete/reinsert/abort/truncation matrix. Not benchmarked yet.

`42beb2cf` adds opt-in `BICDB_ROW_LOCK_KEY_NOTIFY=1`: lazy per-key condition
variables and waiter-counted registry cleanup. Default stays shard-wide. This
addresses waking unrelated waiters on every row release. Four targeted tests
cover shared signals, matching notification, unrelated same-shard notification,
and timeout cleanup; full core with key notifications + graph passed 389 tests
(before the final two targeted tests), and mixed SQL Payments/NewOrders passes.
Normal release build in progress, `/tmp/bicdb-key-notify-build.log`.

Lookback lead: ledger lines 426-433 document deferred additive update repair.
It completed Payments but was rejected for NOPM at older execution speeds;
the later 378k-era repeat was only 7% slower. Retest
`BICDB_UPDATE_REPAIR=1` on current PGO, measuring actual committed Payments.

### Later September 4: reliability work and fresh profile

- `outcome-graph-sleep-d1` (graph, RC notify OFF, first-wait OFF, RC attempts
  20,000): 325,222 NOPM; 650,013 Payments / 650,013 calls; zero serialization
  failures; 13,961,990 context switches. Diagnostic, not acceptance.
- `pgo-repair-r1` (source `8f423e07` PGO, repair ON): 411,139.5 NOPM;
  819,992 Payments / 820,112 calls; 822,279 committed NewOrders; 32,070 handled
  serialization failures. Combined committed business work 1,642,271, about
  8.4% above current-pgo-r1, but NOPM falls 18%. The existing repair feature
  subsequently failed additional correctness tests below: NOT a production
  keeper until fixed and revalidated.
- `rowid-graph-d1`, source `12f84968`: 311,016.5 NOPM, all 622,615 Payments and
  622,033 NewOrders complete, zero serialization failures. Checkpoint phase 0:
  7 ms and 6 ms versus earlier 1.7-2.3 s; phase 1: 8.715 s and 11.415 s.
  11,680,914 context switches, 16.14 cores. Diagnostic, about +3% NOPM versus
  outcome-graph-d1; no repeat yet.
- `key-notify-graph-d1`, source `42beb2cf`: 337,959 NOPM, all 675,772 Payments
  and 675,918 NewOrders complete, zero serialization failures. 7,157,420 context
  switches, 16.77 cores. About +8.7% NOPM and -39% context switches versus
  rowid-graph-d1. Diagnostic, not acceptance.
- `key-notify-fast-r1`, same source, key notify ON but default old-dies/RC2:
  502,485 NOPM, 454,048 Payments / 1,007,743 calls, 595,543 serialization failures.
  Valid short uninstrumented screen, not an unqualified improvement over the
  501k PGO screen because it completes fewer Payments.
- `pgo-repair-profile-d1`: diagnostic 390,537.5 NOPM, 781,875 Payments / 782,014
  calls. Sampled self CPU only (no call stacks), `perf-self.data` at campaign
  root: 64,208 task-clock samples, zero lost, 2.551 MB. Main-thread worker self
  costs: malloc 6.81%, free 3.61%, aligned alloc 1.66%, cells_into 3.78%, member
  scanning 1.31%, cell classification 1.26%, collection_generation 1.54%,
  BoundExpr::eval_value 1.34%. Do not treat these as inclusive call-stack shares.
- `pgo-repair-aw32-d1`: **INVALID**, one VU failed with an uncaught district
  row-lock conflict (district `["3","4"]`, owner 756085). 399,657 apparent NOPM
  is not usable evidence. AW32 diagnostic only; canonical remains AW24.
  `run_trial.sh` now accepts MAX_ACTIVE_WRITES (1..32, default24), recording
  actual arguments. VM topology is 16 physical cores / 32 SMT threads, one NUMA
  node; 20 mean logical cores does not imply 12 idle physical cores.

Correctness fixes after the lookback retest:

- `c67e2319`: deferred update repair formerly trusted mutable WHERE predicates.
  Reproducer pinned balance100, competing writer changed it to40, then a stale
  `balance >= 60` authorized a debit to -20. Repair now declines predicates on
  non-primary-key target columns/unsupported expression shapes, retaining normal
  RC lock/recheck. Also tests an unrelated mutable `enabled` predicate. 415 SQL
  integration tests pass with repair enabled.
- `20484d21`: repair changed a typed numeric's text but left its stored index key
  unchanged (110's key alongside text115). Core and SQL now share the canonical
  numeric key encoder; repair updates both. Tests verify the stored key and an
  index created after repair, plus differential numeric encoding and all 415 SQL
  tests / 391 active core tests. Older compiled artifacts do NOT contain this fix.
- `c24dc761`: lazy repair trace formatting, cached trace environment check in
  the commit path, and consume the owned repair record instead of cloning it.
- `776735e1`: shared numeric key encoder writes one result buffer rather than
  allocating significant-digit and magnitude buffers. Differential codec test passes.
- `446b9a76`: validated ordinary integer PKs render directly into the final
  string; other cases retain the old codec. Differential tests include signed
  SMALLINT/INT/BIGINT boundaries and overflows; 415 SQL tests pass.
- `66d5255d`: BICDB_ROW_LOCK_KEY_WAIT_US defaults250; larger key-specific sleeps
  have an absolute deadline preserving the original approximately250us*attempts
  total budget. Owner release still notifies immediately. Tests with5000us:
  392 core passed, three ignored; concurrent Payment/NewOrder invariant test passes.

Artifacts:
- `/tmp/bicdb-key-notify`, remote `/home/benchmark/bench-logs/bicdb-key-notify`, source
  `42beb2cf`, stripped SHA256 `a4476829b27f91f77eb63f0a96c7aed97780ab7f931a63c0c30fcaebcaec38a2`.
- `/tmp/bicdb-repair-cost`, source `c24dc761`, stripped SHA256
  `2a7dfb72d9b36912599e5f372e65b8378e202756f9fc6f038a20d7090d68f107`;
  no numeric-key fix, not deployed/benchmarked yet.
- `/tmp/bicdb-reliable-latest`, remote `/home/benchmark/bench-logs/bicdb-reliable-latest`,
  source `66d5255d`, stripped SHA256
  `dc3d7841a9ee8204e64aed77eebcbf035b7272048f5ca9ac1f7046ce04fe1b88`.
  Includes all three correctness fixes, both allocation cuts, key notifications
  and bounded wait-period knob. `reliable-latest-fast-r1` running at time of note.
- PGO build `/tmp/bicdb-reliable-pgo-build.log` froze source `c24dc761`, BEFORE
  numeric-key fix and later allocation cuts. Do not confuse with latest normal
  binary. Uses old51fa training profile, target-pgo-use, fatLTO/codegen-units1/v3.

Do not add speculative percentages from micro-optimizations. Re-test the PGO
reference and candidates in balanced order, and compare successful procedure
mix, handled aborts, CPU, memory, WAL, and checkpoint work. Faster NewOrder with
more abandoned Payment work is not automatically a useful engine improvement.
Final acceptance requires longer repeated valid runs and correctness/recovery
checks. Source provenance for copied binaries must use the local build commit
and binary hash, not the remote harness checkout's commit.

## Fixed terminal assignment discovery and controlled screens (September 4)

**Important confound in ALL earlier single-run comparisons:** HammerDB 6.0's
`pg_allwarehouse=false` driver selects each worker's home warehouse and
Stock-Level district ONCE, then keeps them for the whole run. Different
startup-time random assignments change hot-row contention. Earlier differences
are observed screens, not attributable gains; even the PostgreSQL comparison
was not controlled for this layout. Preserve all original artifacts.

`run-vu.tcl.tmpl` now logs `BICDB_TERMINAL_ASSIGNMENT position warehouse district`
once per worker. `TERMINAL_ASSIGNMENTS=/path/to/recorded.tsv` optionally replays
every worker's pair, leaving both original random draws in place. The default
remains random. The harness records replay input/hash and validates complete
capture and exact replay. Paired comparison rejects differing layouts when
capture is enabled. Eleven summarizer tests pass. PostgreSQL runner supports
the same replay input. A changed layout is NOT an engine optimization.

- `latest-pgo-layout-a-r1`: INVALID capture-hook scoping bug, no worker
  assignments captured and all active VUs failed. Fixed by defining the replay
  dictionary at the initialization site inside each worker's procedure.
- `latest-pgo-layout-a-r2`: first valid captured random layout, selected for
  replay without selecting on throughput. **509,126 NOPM**, NewOrders1,018,252 /
  calls1,018,510; Payments481,901 / calls1,017,223; serialization575,800;
  all33success, zero failed queries, watermark0. RSS peak43,322,257,408 bytes.
  Layout is unexpectedly concentrated: warehouse1 has10 workers,12 has8,6 has6;
  only9 of16 warehouses have home terminals. Reason for this concentration not
  established; do not assume independent/uniform thread initialization.
  Exact capture checked in as `bench/tpcc/terminal-layout-a.tsv`, SHA256
  `3eafccfdf53bea54b3c37061299e0764c500e03a63d3a96337b27622806fa6d5`.
  Remote `/home/benchmark/bench-logs/terminal-layout-a.tsv`.

Additional earlier uncontrolled screens, all short and not acceptance:

- `reliable-latest-fast-r1`, normal66d5255d:523,219.5 NOPM;
  NewOrders1,046,439; Payments454,864/1,045,993; serialization635,484;
  22.57 mean logical cores; all33success, failedqueries0, watermark0.
- `reliable-pgo-c24-fast-r1`, PGOc24dc761:539,432.5 NOPM (highest verified screen
  so far, NOT600k); NewOrders1,078,865; Payments518,796/1,077,611;
  serialization600,307;22.01cores;all33success,failedqueries0,watermark0.
  SHA256 `7e3b8ebac649285331b0e61399164e567d6bc8cb3d94e484eb6bec72dd2bcda0`.
  Missing later numeric-key fix, so do not promote this older artifact.
- `latest-graph-250-d1`, normal66d5255d, wait-graph/attempts20000/keynotify:
  293,179 NOPM, all586,358 NewOrders and586,604 Payments, serialization0;
  14.43cores,8,283,855 context switches. Diagnostic.
- `latest-graph-5000-d1`, same binary/flags except KEY_WAIT_US5000:
  362,570 NOPM, all725,140 NewOrders and726,859 Payments, serialization0;
  17.68cores,4,183,612 context switches. Observed+23.7%, but home-layout
  confound means this is NOT yet a demonstrated wait-period gain. Diagnostic.

Latest artifacts (both local `/tmp` and remote `/home/benchmark/bench-logs`):

- `bicdb-reliable-latest-pgo`, source66d5255d/f3f9c53f docs+harness HEAD,
  SHA256 `c6e714dc2581eed7b35de71ac98a1295767c5f62107d280ff7ed8fdf0debe046`.
  All correctness fixes and allocation cuts through66, long-key-wait knob;
  does NOT include envelope-prefix decoder. Old51fa PGO profile, fatLTO,
  codegen-units1, x86-64-v3; build `/tmp/bicdb-latest-pgo-build.log`21m14s.
- `bicdb-envelope-prefix`, normal sourceabb647e8,
  SHA256 `f6f0568d7612f9d4463f13fa2972aea1443b4e3f1cfbbbdff20e75c13758488b`.
  Common typed envelopes classified while locating their boundary, avoiding
  double brace/string walks. Other shapes retain generic/serde fallback.
  Twelve record tests,393 active core tests (three ignored),415 SQL tests pass.
  No persistent row cache, storage-format or public-field changes.

## Controlled-layout continuation: real repair/WAL work

All below use captured layout A, MAW24, VU32,1+1min, throughput (not diagnostic).
Every completed BicDB run below is valid, all33VUs successful, failedqueries0,
watermark0. Still NO600k BicDB result. Short screens are not acceptance.

| Trial | Binary source | DB NOPM | Committed Payments / calls | Notes |
| --- | --- | ---: | ---: | --- |
| latest-pgo-repair-layout-a-r1 | PGO66d5255d | 412,963 | 825,447 / 825,581 | repairON, keywait5000; serialization29,346; WAL14,469,928,106 bytes; RSS37,469,696,000 |
| latest-normal-layout-a-r1 | normal66d5255d | 513,635.5 | 472,300 / 1,029,972 | repairOFF; serialization599,647; RSS39,694,270,464 |
| envelope-layout-a-r1 | normalabb647e8 | 504,389 | 471,284 / 1,009,231 | repairOFF; no demonstrated decoder gain; RSS39,080,443,904 |
| word-scan-layout-a-r1 | normal620ff18b | 492,646 | 462,721 / 984,849 | repairOFF; no demonstrated word-scan gain; RSS35,774,910,464 |
| word-repair-layout-a-r1 | normal620ff18b | 381,078.5 | 760,212 / 760,317 | repairON,keywait5000; serialization26,856; WAL13,323,884,878; RSS36,492,922,880 |

`620ff18b` skips eight ordinary JSON string bytes per iteration (safe u64 loads,
exact scalar handling of special bytes); differential scan test covers all256
byte values at different offsets/lengths plusUnicode/escapes.394coretests pass.
Do NOT promote based on mechanism: controlled screens above did not improve.
Binary `/tmp/bicdb-word-scan`, remote `/home/benchmark/bench-logs/bicdb-word-scan`, SHA256
`13e4b1d4e21d5b3924f9ec6fd7ed19267601cfa43bc00dadbd20b82b27896167`.

PostgreSQL18.6 **same terminal layout A**: `codex-pg186-layout-a-r1`:
district480,160 ->1,834,133 =1,353,973 NewOrders /2min =**676,986.5 NOPM**;
HDB687,624. History480,000 ->1,832,826 =**1,352,826 Payments**, exactly equal
toPayment calls; NewOrder calls exactly equalNewOrders;all33success/exit0.
Same16warehouses/32VUs/settings as earlier PG reference; seed still independently
generated, not byte-identical to BicDB. Preserved stoppeddata
`/dev/shm/codex-pg186.rrHzXm`, artifacts `/home/benchmark/bench-out/codex-pg186-layout-a-r1`.
Thus a genuine engine gap remains even after matching terminal contention.

Concrete repair/WAL findings and candidate fixes:

- `1307d00b`: clone latest StoredRecord Arc under shard read guard, parse after
  releasing it. For stored writes, project only delta fields, share exactly the
  full-path numeric arithmetic and index-key encoder, splice repaired cells into
  latest raw metadata. Preserve conservative index-key validation, fallback on
  unsupported shapes. Keep actual latest pre-image and consume finalized repair
  plan while row lock remains held. Differential arithmetic test, moved-index
  rejection and pre-image/reopen tests (with/withoutcheckpoint) pass.396core and
  415SQL tests passed. Artifact `/tmp/bicdb-stored-repair` ONLY, SHA256
  `f181f5a046549e9c7a5aee22c522e6dec148091eb3d94aaf83cd572be4775bce`.
- **Downstream discovery:** any repair used to clear ALL prepared WAL payloads.
  The fallback `encode_tx_commit_frames` serialized `TxFrameRef::Write`, forcing
  every stored Customer/History/etc write into a full JSON tree too. Merely
  preserving a compact repaired row did not fix this fallback.
- `0bc6363d`: defer eager WAL preparation for unresolved repair transactions;
  prepare once after repair finalization from the final compact rows. Preserve
  legacy fallback on encoding failure. Test asserts the actual prepared payload
  is a `tx_patch`, not just that a patch could have been made.396coretests and
  all415SQL with repair/compactWAL enabled passed. Current candidate running as
  `final-wal-repair-layout-a-r1`, matched against word-repair-layout-a-r1.
  Binary local `/tmp/bicdb-repair-final-wal`, remote `/home/benchmark/bench-logs/bicdb-repair-final-wal`,
  SHA256 `74c20753c4a5985d91272b91bc4fdd1760016e84a1db11477142c64dd155aab1`.
- Separate uncommitted storage candidate at time of note avoids the unconditional
  payload.to_vec in disabled compression before copying again into the final WAL
  buffer. Direct plain-frame bytes match existing headers/CRC/payload byte for
  byte; compression/encryption retain old path. Tests running/passed in
  `/tmp/bicdb-wal-copy-test.log`, `/tmp/bicdb-wal-copy-core.log`.

PGO decoder build still pending: `/tmp/bicdb-envelope-pgo-build.log`, session51312,
sourceabb647e8/0af64218 harness HEAD. Does NOT include620ff18b or later repairs.
Save a unique stripped binary/hash on completion before anotherPGO build.
PGOscores above do not yet establish a repeatable advantage overnormal builds.

## Reliable-commit continuation (fixed layout A)

The final-WAL candidate completed a balanced B/A/A/B short screen:

| Trial | DB NOPM | Payments / calls | WAL bytes |
| --- | ---: | ---: | ---: |
| word-repair-layout-a-r1 | 381,078.5 | 760,212 / 760,317 | 13,323,884,878 |
| final-wal-repair-layout-a-r1 | 425,137 | 849,682 / 849,812 | 14,259,821,226 |
| final-wal-repair-layout-a-r2 | 433,520 | 866,654 / 866,749 | 14,550,338,132 |
| word-repair-layout-a-r2 | 399,484.5 | 797,447 / 797,568 | 13,949,851,360 |

Mean NOPM 390,281.5 ->429,328.5 (+10.00% observed). All valid, no failed
queries/VUs, watermark0. Total WAL +5.63% exceeds the campaign's strict5%
resource gate, despite lower bytes per committed transaction. NOT acceptance.
`wal-copy-repair-layout-a-r1` (3624b5dd) was valid at404,992.5 NOPM,
807,135/807,229 Payments, WAL13,540,674,953: no demonstrated additional gain.

Further changes and correctness checks:

- `3624b5dd`: plain WAL frames bypass the temporary payload copy; exact byte
  differential tests including compression thresholds.397 core tests passed.
- `defb2a1e`: opt-in `BICDB_WAL_SINGLE_BUFFER=1` serializes plain frames directly
  into a single framed batch, same headers/CRC/format. Default OFF; encryption
  retains legacy path. Also fixes a PREEXISTING prepared-WAL cache bug: after
  savepoint rollback and equal-count replacement writes, stale prepared bytes
  could describe discarded rows. Reproduced with new feature OFF, then fixed by
  invalidation on write-index rebuild/new writes. Reopen regression tests pass;
  399 core tests and415 SQL tests passed with repair/compact/direct framing ON.
- `440d571d`: consume already-fresh, locked repair plans and preserve their actual
  pre-image so these rows also use compact WAL. Actual tx_patch regression test;
  400 core tests and415 SQL tests passed. Not yet benchmarked.
- `d9a7f006`: opt-in `BICDB_ROW_LOCK_KEY_NOTIFY_ONE=1`, default OFF. Wake one
  key waiter, with cancellation/drop handoff to another waiter so a cancelled
  selected waiter cannot strand the queue. No ownership/fairness change.
  401 core tests and415 SQL tests passed with all new flags ON, both default
  policy and explicit wait-graph policy.

`single-buffer-on-layout-a-r1` (defb2a1e, old-dies policy) is INVALID: one VU
failed with an uncaught Payment district lock conflict. Error at~41s after
listen, before first checkpoint, so checkpoint-timeout explanation is not
supported. Repair's long attempt budget still permits immediate age rejection;
this is a plausible cause from code, not a traced failure reason. Do not blindly
disable cycle prevention: repair can already hold other row locks.

Existing wait-graph policy +repair, canonical RC attempts2, same binary/layout:
`single-off-graph-repair-layout-a-r1`, direct framing OFF, **463,965 NOPM**;
927,930 NewOrders /927,930 calls, 927,350 Payments /927,464 calls (99.9877%).
All33 VUs success, failedqueries0, watermark0, valid. Serialization33,105;
WAL15,542,704,005; RSS38,499,303,424;22.64 logical cores;4,174,387 context switches.
One short screen, not an established wait-graph gain. Direct-ON counterpart
`single-on-graph-repair-layout-a-r1` started next.

Artifact provenance (remote harness git metadata is NOT binary source):

- defb2a1e `/tmp/bicdb-single-buffer`, deployed `/home/benchmark/bench-logs/bicdb-single-buffer`,
  SHA256 `ed75f52297e68dd05cba0489e1ce71b77a76c8feab62720081a5ae74bffd8e60`.
- d9a7f006 `/tmp/bicdb-notify-one`, local only at this note,
  SHA256 `1c0b16deb2f5c8e564ef97b0ae68ea3905fc9f80db88d00b581a15717a77e308`.
- Old-profile PGO build3624b5dd completed22m37s, `/tmp/bicdb-repair-wal-copy-pgo`,
  SHA256 `fa231876b819255effe2af053b2f2e9a14fd8df468cbf50175bf7cacede735ad`.
- Old-profile decoder PGO completed22m45s, `/tmp/bicdb-envelope-pgo`,
  SHA256 `3b593c46436977a972750764f58b6ebc8d87f624319d76e4c3af336971f6b667`.

Fresh PGO instrumentation build started from frozen source d9a7f006, separate
`target-pgo-train`, fatLTO/codegen-units1/x86-64-v3,
`-Cprofile-generate=/home/benchmark/bicdb/pgo-data/current-train`.
Build log `/tmp/bicdb-current-pgo-train-build.log`, session34325. No instrumented
score may count as throughput acceptance. Existing profiles came from much older
code; current-code training is an experiment, not an assumed gain.

Direct-ON graph counterpart completed: `single-on-graph-repair-layout-a-r1`,
467,257 NOPM;934,514 NewOrders /934,514 calls;
934,505 Payments /934,610 calls; serialization33,376; all33success, queryerrors0,
watermark0, valid. WAL15,681,868,782; RSS40,073,728,000 (HWM40,374,157,312);
22.43 logical cores,4,201,905 context switches. Only+0.71% over one OFF screen,
not an established framing gain. Latest d9a7f006 binary now deployed;
`reliable-caller-profile-d1` is a separate EXCLUDED diagnostic with commit phase
traces and 49Hz/4096-byte DWARF caller sampling, directON/notifyOneOFF/repairON/
wait-graph, same layout A. No throughput claim from this diagnostic.

Read-only PostgreSQL REL_18_6 comparison: heapam.c waits on the modifying
transaction in its update path (XactLockTableWait); proc.c ProcLockWakeup grants
eligible locks before waking their waiters. BicDB's current CV wake merely asks
the waiter to compete again. This motivates testing notification/handoff costs,
but is NOT measured proof that this difference accounts for the engine gap.
Sources: https://github.com/postgres/postgres/blob/REL_18_6/src/backend/access/heap/heapam.c
and https://github.com/postgres/postgres/blob/REL_18_6/src/backend/storage/lmgr/proc.c.

The DWARF diagnostic completed valid (455,605 NOPM, excluded),911,448/911,553
Payments, all33success, queryerrors0. Final COMMIT_PHASES means: exec1116us,
walprep30us, admission0us, DBread0us, apply251us, durable94us. These are elapsed
times including waits, not CPU shares. INDEX_BUILD includes repair and its WAL
preparation; GRAPH includes index apply, so do not mislabel those trace buckets.
Sample35,396/zero lost,147MB: worker self malloc7.35%,free3.79%,aligned1.51%+0.78%,
cells_into2.56%,collection_generation1.38%,member-spans1.14%. **Caller unwinding
FAILED** (children barely exceed self and unknown frames1/3/7); do not attribute
allocation to callers from this sample. VM perf has libdw unwinding enabled.
Fresh frame-pointer build (same functional d9a7f006 source) started using existing
`target-fp`, `RUSTFLAGS='-C force-frame-pointers=yes'`, log
`/tmp/bicdb-reliable-fp-build.log`, session31939. Latest unprofiled wake-one OFF
control `notify-off-graph-layout-a-r1` running meanwhile.

Wake-one first matched pair (d9a7f006, graph+repair+singlebuffer, layout A):

| Trial | DB NOPM | Payments / calls | WAL bytes | RSS peak | Context switches |
| --- | ---: | ---: | ---: | ---: | ---: |
| notify-off-graph-layout-a-r1 | 462,143.5 | 925,935 /926,046 | 15,275,253,555 | 37,291,642,880 | 4,144,665 |
| notify-on-graph-layout-a-r1 | 457,535.5 | 916,308 /916,420 | 15,125,716,935 | 35,007,930,368 | 4,036,920 |

Both valid/all33success/queryerrors0/watermark0; serialization33,093 vs32,663.
Observed NOPM -1.0%, context switches -2.6%; no demonstrated notification win.
Keep notify-one OFF by default. Frame-pointer build completed3m56s; functional
source d9a7f006, `/tmp/bicdb-reliable-current-fp`, deployed same basename,
SHA256 `e7aed167e62c3a5f2956f723c6c5f19238b151d13febb095d36c69232b12cfd0`.
`reliable-fp-profile-d1` starts next, excluded, no phase tracing, intended 99Hz
frame-pointer caller sampling in steady state. Fresh PGO training build still
linking; no current-code PGO result yet.

## Fresh caller profile and VM interruption

Frame-pointer sample succeeded:72,618 cpu-clock samples, zero lost,20.398MB,
`/home/benchmark/bench-out/codex-reliable-fp-profile-d1/perf-calls.data`.
Inclusive worker CPU: stored UPDATE20.95%, joins8.28%, commit9.19%, routine
expression evaluation5.16%, cells_into3.60%, final WAL preparation2.58%.
These overlap and MUST NOT be added. Most malloc samples are under SQL execution
(6.81% of total CPU) versus commit(0.44%). Nearest BicDB caller aggregation,
stripping instruction offsets, identified try_execute_update_stored251 malloc
samples, SqlValue::clone220, eval_slot_row_value139, slot_rows_from_visible117,
project_slot_row_select_with_wildcard116, apply_row_order_by109. Those are sampled
CPU counts, NOT allocation counts. Diagnostic completed valid453,912 NOPM,
908,021/908,137 Payments, all33success/queryerrors0/watermark0; excluded.

Confirmed source waste: apply_row_order_by cloned whole SlotRows (and their
Strings) before evaluating keys. Candidate first evaluates keys while borrowing
unchanged input, then moves rows into empty keyed slots, preserving original rows
on key errors, stable ties and existing top-K ordering. Ownership regression test
failed before the change; error-preservation test passed. Tests after fix passed
in `/tmp/bicdb-order-ownership-after.log`, `/tmp/bicdb-order-topk.log`,
`/tmp/bicdb-order-sql.log`: two ownership/error tests, two top-K tests,415 SQL
integration tests. No measured throughput gain yet; no release artifact built
for this change before the VM restart decision.

Fresh PGO training binary (functional source d9a7f006) built24m36s, uploaded and
SHA-verified: `/tmp/bicdb-current-pgo-train`, remote same basename,
SHA256 `a3b9b23fe0ba87eac79d29d4030ce3d4416e05d6a6b58ec01660dcbec07ce63d`.
**Training launch pgo-current-train-d1 did NOT connect:** SSH timed out. No
runtime profile collected and no new optimized PGO binary exists.

At~23:52UTC Azure get-instance-view confirmed `bicdb-bench-1` in resource group
`bicdb-bench` is **PowerState/deallocated**, provisioning timestamp23:49:19UTC.
VM prioritySpot, evictionPolicyDeallocate. Cause not confirmed: activity-log
query returned no matching events. Do NOT claim eviction as proven. Public IP
still192.0.2.11. Restart approval requested asynchronously; no restart issued.
Data in /dev/shm must be treated as potentially lost across deallocation. Check
durable artifacts/seed backups after restart; do not assume same physical host
or reuse old paired throughput baselines if host changes. No600k result.

## September 5 restart authorized; continue without voluntary pauses

User explicitly approved restarting/restoring the benchmark VM and instructed
continued work through recoverable interruptions. Same-VM restart/recovery is now
in scope: do not stop again merely to request that same approval. This does not
authorize changing VM size, buying additional machines, weakening correctness,
or claiming success before600k. No goal-completion or pause action issued.

`az vm start -g bicdb-bench -n bicdb-bench-1` succeeded; running at02:12UTC,
same public IP and EPYC9V74/32logical/16physical CPU topology. Physical host
identity cannot be established from matching model alone. Disk artifacts intact;
/dev/shm empty. No seed backup found in scoped disk search, so rebuilding the
16-warehouse seed with baseline d9a7f006 binary `bicdb-notify-one`,8loaderVUs,
same build harness. Work `/home/benchmark/bench-logs/codex-seed-restart-20260905`, log same
path plus`.log`, session26698. Save a zstd disk archive after verification so the
next deallocation needs restoration rather than regeneration. All fresh pairs
must use this seed and this boot, not pre-restart scores as paired controls.

ORDER BY candidate normal release, source cf6e6d3f, built3m28s, deployed:
`/tmp/bicdb-order-move`, remote `/home/benchmark/bench-logs/bicdb-order-move`, SHA256
`a4a67e14341ed591bbd4f0afeb6366d685d84221e26561a298954b95eebeab4c`.
Still unbenchmarked. Baseline `/home/benchmark/bench-logs/bicdb-notify-one` d9a7f006.

Restart follow-up at02:35UTC: rebuilt seed verified by compact/reopen,16WH,
district_sum480160, bytes4614582222, WAL0. Content SHA256
`af486d95cea4e4df6b95dd44f81abd9678a73931788a0525051bd1b74ecfac69`.
Durable, integrity-tested archive now exists:
`/home/benchmark/bench-out/codex-restart-20260905/seed.tar.zst`, SHA256
`2360d91973281a8a8d0ee61c0910550d7dac89b9ce84221040deb75f6ecaedeb`.
Same directory preserves seed-manifest.json and boot-id. Restore this seed after
any future same-VM recovery; do not regenerate or compare across boots as pairs.

Fresh training `codex-pgo-restart-train-d1` completed valid, all33VUs, no query
errors/watermark gap; 141843 NewOrders and141868/141888 Payments. Instrumented
70921.5 NOPM is EXCLUDED, not a throughput result. Instrumented startup184s.
Profile runtime-5946.profraw (58884912bytes) copied locally and merged with the
matching1.96 LLVM into `/tmp/bicdb-restart-trained.profdata`. Includes startup
and shutdown; no counter reset. PGO-use build session14733 uses functional source
cf6e6d3f, fat LTO/codegen1/x86-64-v3. Training was d9a7f006: ORDER BY is the only
functional difference, and LLVM reports that function's profile hash mismatch.
Do not attribute PGO changes to an identical-source trained profile.

First fresh normal baseline `restart-order-base-r1`: valid442540 DB NOPM,
885080 NewOrders equal calls,884798/884908 Payments (99.9876%), all33VUs,
queryerrors0/watermark0. WAL14626156755bytes, RSS HWM35939749888bytes,
mean cores22.29. Same repair+wait-graph+single-buffer ON, notify-one OFF,
layoutA,1+1minute canonical screen. New boot/seed means old460-470k scores
are historical context, not matched controls. Candidate `restart-order-move-r1`
started next, session50863. No600k result.

ORDER BY screens: candidate restart-order-move-r1 valid460413.5NOPM,
Payments919415/919524, WAL15214645498, RSS HWM36257251328. Candidate r2
valid467593.5NOPM, Payments936235/936349, WAL15434022680, RSS41022910464.
Reverse-order baseline r2 started next (session66051). First pair+4.04% is
provisional; do not promote before completing balanced controls/resource gates.

Next candidate keeps stored UPDATE RETURNING in SlotRows (no-FROM previously
rebuilt qualified/unqualified string-key maps) and moves the consumed candidate
instead of cloning it. Regression verifies stored path, zero slot-to-map calls,
generic-equivalent full SqlResult including types/origin metadata, expression,
CHAR/NUMERIC/timestamp, wildcard/alias and empty-result behavior. It failed on
old map conversion, passed after fix. Added alias RETURNING * test caught doubled
alias/table lookup slots in initial candidate; fixed wildcard width before
benchmarking. All273 active SQL unit tests pass (3ignored), canonical repair+
graph+direct-WAL environment. Integration logs `/tmp/bicdb-returning-integration.log`.
Broad parallel --tests build exhausted LOCAL disk during linking (not a test
assertion); removed our6.4GB target-pgo-train cache, preserving training binary,
raw and merged profiles. Re-running relevant sql/cell_rows targets with-j2.
Optimized PGO build still links frozen cf6e6d3f, without this RETURNING change.
Targeted integration rerun completed:415 sql +17 cell_rows tests pass, no failures.

At02:54UTC: ORDER BY balanced screens completed B/A/A/B/B/A:
baseline442540,463439,465207; candidate460413.5,467593.5,470586.
Allvalid, NewOrder counters matchcalls, Payments~99.99%. Baseline mean457062,
candidate466197.67; paired percentage gains4.039%,0.896%,1.156%, mean2.031%,
paired-t95%CI[-2.302%,6.363%]. INCONCLUSIVE, not a performance keeper.
Baseline r2/r3 Payments928628/928758 and927718/927854;
WAL15308509562/15374874906; RSS38490337280/40500002816.
Candidate r3 Payments941411/941526; WAL15563975301; RSS40958873600.

Fresh PGO-use build finished15m39s; frozen functional source cf6e6d3f (not the
newer RETURNING work), matching profile from d9a7f006 with the documented one
function mismatch. Saved/deployed `/tmp/bicdb-restart-trained-pgo`, remote
`/home/benchmark/bench-logs/bicdb-restart-trained-pgo`, SHA256
`d30d3438a1eb2d4ce7a75973cec0d6cb41375c14acfd148f1f87e88a6f790ef8`.
Trial restart-trained-pgo-r1 launched session77144, same canonical flags/layout.
No throughput result yet. Plain RETURNING source6a73ede1 release build still
running session91371, limited-j2 causing slow thin-LTO; no benchmark yet.

Next candidate adds immutable assignment ordinals/changed-column Arc to the
existing StoredUpdateReadPlan, validated by original IR key and schema Arc.
Avoids repeated AST/schema validation, column-name allocations/lookup and rebuilt
changed-column lists; does NOT cache authorization/index/trigger/repair checks.
Regression originally proved8 builds/8calls, now stable after catalog warmup;
first catalog loading changed schema Arc at same key, so identity validation
correctly rebuilds once. ALTER TABLE invalidates; newly added unique index still
rejects conflicting warmed update23505. All274 active units (3ignored),415sql,
17cell_rows,2authorization_canary tests pass. Logs `/tmp/bicdb-update-shape-*`.
No measured throughput result for this candidate yet.

Read PostgreSQL18.6 executor sources: execExpr.c builds projection ExprState /
ExprEvalSteps; execTuples.c virtual slots reduce copies. Relevant comparison,
not proof of any percentage attribution:
https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/execExpr.c
https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/execTuples.c
Local helper `/tmp/bicdb-restart-run.sh` launches exactly current canonical
throughput flags from arguments trial,binary-basename,rep,position[,duration].
Call only after prior trial finishes; it does not validate or compare results.

At03:01UTC, fresh trained optimized build cf6e6d3f returned two valid canonical
screens: restart-trained-pgo-r1=533838NOPM, Payments1067322/1067458;
WAL17751989066bytes/RSS41469812736. r2=535076.5NOPM,
Payments1070011/1070159; WAL17772417240/RSS41790926848. BothNewOrderdelta
equalscalls, queryerrors0/watermark0, all33VUs. These include fatLTO/codegen1/v3
plus fresh PGO, not a controlled attribution to PGO alone. r1 is+13.44% versus
immediately preceding normal same-source470586; reverse normal control
restart-pgo-normal-r4 started session pending below. No600k, no accepted keeper:
absolute WAL growth exceeds5% gate even though useful committed work increased.
Local wrapper r1 exited2 AFTER remote trial completed because it was edited
while bash still read it. Remote result/harnessvalidity remained clean. Do not
edit running script files; updated helper passes bash-n and r2 exited0.

RETURNING normal artifact source6a73ede1 finished13m16s with-j2; saved/deployed
`bicdb-returning-slots`, SHA256
`6b25f6731d018f58a569acf7853e697015392546ba053941799813eb27ab0c15`.
Assignment-plan normal artifact sourcef86c9178 finished4m07s with-j8;
`bicdb-update-shape` upload/hash check session53984. No benchmark yet.
PGO-use build f86c9178 started session66966 using the SAME d9 training profile,
fatLTO/codegen1/v3. More changed functions will have profile mismatches; preserve
the cf6 trained artifact before any rebuild (already done). No new training yet.
Removed only our12GB target-fp build cache; preserved source and saved diagnostic
binary `/tmp/bicdb-reliable-current-fp` SHA e7aed167e62c3a5f2956f723c6c5f19238b151d13febb095d36c69232b12cfd0,
and remote frame-pointer perf recordings. Regenerable build cache, no DB data.
Helper optional args now duration,writers,kind (defaults1,24,throughput).
AW32+graph reliability diagnostic is planned, not yet run; keep separate from
canonical24-writer results. Do not silently count altered admission as goal.
Assignment normal sourcef86c9178 saved/deployed SHA256
`1b38ccf27f2918bde6af6bbef2cf84eb4682a2f2e9276bbd3aa54c79a4a13c82`.
Reverse normal cf6 control restart-pgo-normal-r4 valid464965NOPM,
Payments929812/929939, WAL15358414944, RSS38546649088, noqueryerrors/watermark0.
PGO screens are thus normal470586 / optimized533838 / optimized535076.5 /
normal464965. Both paired throughput gains remain positive; resource/latency/
long-run gates still outstanding. Next restart-returning-slots-r1 started.

Next SQL join candidate removes a temporary RightRowSource Vec and borrows
resident right SlotRows through predicate evaluation instead of cloning each
before merge clones it again. Parsed-record fallback remains owned. Also defers
selection_with_join_constraint AST cloning until indexed join declines; fast
indexed joins never used that combined AST. Ownership test proves borrowed row
allocation identity. All275 active SQLunits (3ignored),415sql,17cell_rows and
2authorization_canary integrations passed under repair+graph+directWAL flags.
Logs `/tmp/bicdb-join-borrow-lib.log`, `/tmp/bicdb-join-borrow-integration.log`.
Not benchmarked. f86 PGO build remains frozen and excludes this join change.

At03:15UTC, normal RETURNING screen restart-returning-slots-r1 valid464714NOPM,
Payments930904/931021; WAL15379688155, RSS40312655872. Previous normalcf6
control464965: essentially neutral, no end-to-end gain demonstrated.
Normal assignment-plan restart-update-shape-r1 valid464219NOPM,
Payments927998/928137; WAL15348234533, RSS36802965504. Also neutral so far.
Do not infer throughput wins from reduced allocation/planning work alone.

Join normal source52883a62 finished4m06s, saved/deployed `bicdb-join-borrow`,
SHA256 `3564e6bd9c964b40c40e3651fb0e6dbe49ec24846885eb46510add92648ec07a`.
PGO assignment sourcef86c9178 finished14m10s, saved/deployed
`bicdb-update-shape-pgo`, SHA256
`98a65ce44e746d8865842ba551d387629b3331d26efa126ba01ba1d633fda2b0`.
Neither benchmarked yet. AW32 diagnostic restart-pgo-aw32-d1 started next using
the proven cf6 trained binary, PROFILE_KINDlatency solely to exclude altered
admission from canonical throughput; no HammerDB time-profile instrumentation.

Investigating LLVM BOLT20.1.2, downloaded signed-repository Ubuntu bolt-20 and
libbolt-20-dev packages and extracted ONLY under `/tmp/bicdb-bolt.x40KZA` (no
system install). Tool `/tmp/bicdb-bolt.x40KZA/extracted/usr/lib/llvm-20/bin/llvm-bolt`.
Official source https://github.com/llvm/llvm-project/blob/main/bolt/README.md.
Preflight instrumentation of cf6 PGO failed cleanly: runtime requires relocation
records, and that binary has no .rela.text. NO instrumented runtime executed.
Log `/tmp/bicdb-bolt-instrument.log`. Started relocation-enabled PGO build of
current frozen source52883a62, session75811, same profile/fatLTO/codegen1/v3,
`cargo rustc --release -j8 -p bicdb-cli --bin bicdb -- -Clink-arg=-Wl,--emit-relocs`.
Log `/tmp/bicdb-join-pgo-relocs.log`. Compare any BOLT output against this EXACT
input binary, not an earlier source; no BOLT gain claimed. Preserve binaries
before rebuilding target-pgo-use. Local memory cap40,000,000KiB was used for
BOLT preflight, not for any benchmark.

AW32 diagnostic restart-pgo-aw32-d1 completed valid536745NOPM, Payments
1072633/1072749, noqueryerrors/watermark0; mean cores21.06, WAL17878524581,
RSS40738840576. Essentially neutral versus canonical24-writer PGO534-535k;
excluded from acceptance. No admission win; retain24 for canonical trials.
PGO assignment candidate restart-update-shape-pgo-r1 valid535631.5NOPM,
Payments1068448/1068581; WAL17761858837, RSS40754978816, cores21.02.
Also neutral versus earlier cf6 PGO; no added throughput gain demonstrated.
Next normal join restart-join-borrow-r1 running session84300. Plan a separately
excluded flat CPU profile of the proven optimized cf6 binary (no frame pointers
in that binary, so self-cost sampling only) after the join screen.
Server read-only inspection: concurrent shared-execution gate is48, while
MAX_ACTIVE_WRITES admission is taken at commit, not around the entire SQL call.
AW32 therefore was not increasing a32-VU execution cap. PGO normal r2 uses21.13
mean cores versus22.71 for normalcf6 control; higher throughput with less CPU,
but5.04million context switches versus4.12million. Need fresh optimized profile,
not an assumption that prior source-level allocations remain the bottleneck.
Proc-mix counters increment after successful top-level query return (including
internally handled exceptions), not at entry. Accepted trials have zero failed
queries, so Payment denominator still includes all successful top-level calls,
even those whose business work rolled back internally; history deltas provide
the independent successful-commit check.

## Publication checkpoint — 2026-09-05

User requested merging all current work to main and pushing GitHub. Before
publication, origin/main was an ancestor of this branch (60 local commits ahead,
no divergent remote commits). Latest functional source is52883a62; subsequent
journal/formatting commits do not represent additional benchmark gains.
Best canonical screen remains535631.5 database-verified NOPM, not600k.
Normal join screen restart-join-borrow-r1 returned467318.5NOPM,
Payments935433/935543, WAL15445550115bytes, RSS39362523136; valid with no query
errors and zero watermark. One short screen is not a proven throughput win.

Relocation-enabled optimized build52883a62 completed13m30s. Saved local
`/tmp/bicdb-join-pgo-relocs`, SHA256
`d1015aa53481b105f56c28acc623b035e27ff63bca19ab32255ab1406f81658e`.
BOLT instrumentation completed successfully against that exact binary; local
`/tmp/bicdb-join-bolt-instrumented`, SHA256
`2da64269fdfda5b12808edd3521c7ddca41aaeebff0c8f0978d965e39bd4de03`.
Neither artifact has been benchmarked; the instrumented binary has not been
executed. Review crypto-assembly FDE/patch warnings before proceeding. Build
caches and binary artifacts are not included in Git.

Excluded diagnostic restart-pgo-wait-profile-d1 used the proven cf6 optimized
binary. Flat CPU sampling and a scoped GDB all-thread stack snapshot were
collected, not yet analyzed. GDB detached normally from PID111067 after0.28s;
the benchmark process was not left stopped when the user interrupted for
publication. The diagnostic result file exists, but its score is not acceptance
evidence. Scoped debugger tools are under
`/home/benchmark/bench-tools/codex-gdb-20260905` on the benchmark VM, not system-installed.

Latest functional validation before publication:275 active SQL unit tests
(3ignored),415 SQL integrations,17 cell-row integrations and2 authorization
canaries passed. Publication checks also run the11 benchmark-summary tests,
shell syntax validation, formatting and whitespace checks. Formatting-only
cleanup does not change the previously tested functional implementation.

## After publication: canonical numeric identity casts

PR824 merged all61 commits through GitHub's required linear-history PR workflow;
published main6709089a has the same tree as local publication970169ac. Continued
work starts on perf/hammerdb-600k-next. GitHub source/binary SHAs must not be
confused with the pre-rebase artifact source SHAs recorded above.

Optimized cf6 flat CPU diagnostic:163k cpu-clock:u samples, zero lost;
allocation helper8.36%, free4.51%, aligned-allocation helpers also present;
StoredRecord::cells_into6.00% self, stored UPDATE2.17%, BoundExpr::eval_value1.96%.
These are self samples, not allocation counts or inclusive attribution. The
single GDB snapshot showed three row-key waits and one WAL mutex wait, with
most connected workers executing SQL; parked Tokio scheduler threads are not
blocked database calls. One snapshot does not establish time-weighted waiting.

Next candidate keeps an already-canonical numeric String during a numeric cast,
instead of parsing a coefficient and rendering identical text. Uses the existing
canonical-text recognizer plus both unconstrained numeric digit limits. Other
syntax, special values and overflow remain on the authoritative full-parser
path. Ownership regression failed before the change; after it all277 active SQL
unit tests(3ignored),415 SQL integrations,17 cell-row integrations and2
authorization canaries passed under repair+graph+single-buffer flags. Differential
test covers noncanonical spellings, negative zero, special values, invalid text
and both numeric range boundaries. Logs /tmp/bicdb-numeric-cast-{before,lib,
integration}.log. No end-to-end numeric-cast result yet; not a proven win.

BOLT instrumentation startup trial restart-bolt-train-d1 exhausted the default
readiness polling budget before workload execution; no result or profile was
produced, and no crash/OOM was reported. The excluded next training attempt will
use a larger explicit readiness budget and periodic profile dumps (normal signal
termination did not emit an exit profile). READY_ATTEMPTS leaves the production
default1200 polls unchanged and changes no timed workload. The exact unmodified
relocation-enabled control is running as restart-join-pgo-control-r1.

At04:03UTC, main PR824 CI is fully green. Numeric identity candidate normal
build22f0f931 (functional change dab5f24d) finished4m37s; saved/deployed
`bicdb-numeric-cast`, SHA256
`fe1aac56c7518dae88d56390d2c2b0f9d579c772b078a1d3d2de8a3e4f61faf7`.
Its relocation-enabled PGO build finished14m32s, same d9 profile and
fatLTO/codegen1/v3 settings, saved/deployed `bicdb-numeric-cast-pgo`, SHA256
`de45be24ca9feaf0efc75b7dbe4781de997321e2bc1154980bc54ad2c1f61e71`.
Neither has a throughput result yet.

BOLT training d2 completed after240.697s startup with READY_ATTEMPTS12000.
Periodic instrumented binary SHA256
`d8f52754a16fd25fd6a5d7f313630d728056fa1a3402baa5ef71fd8377241849`.
Training throughput85375NOPM is EXCLUDED, zero query errors, all33VUs successful.
Captured a stable periodic profile during its measured minute at03:52:48UTC:
`/home/benchmark/bench-out/codex-restart-bolt-train-d2/workload.fdata`,12700235bytes,
SHA256 `1e649192056812426b1a69e526473bfdd2044162de555c47cd3ba8276f317389`;
local `/tmp/bicdb-bolt-workload.fdata`. Periodic runtime resets counters after
each dump; the captured workload snapshot was preserved before later dumps.
Runtime reference: https://github.com/llvm/llvm-project/blob/llvmorg-20.1.2/bolt/runtime/instr.cpp.

Applied BOLT to EXACT original `bicdb-join-pgo-relocs` d1015aa5 input:
`--data=/tmp/bicdb-bolt-workload.fdata --reorder-blocks=ext-tsp
--reorder-functions=cdsort --split-functions --split-all-cold --thread-count=4`.
1590 functions carried profile;63 profiled functions could not be optimized.
Output `bicdb-join-bolt-optimized`, SHA256
`49dab35f77265f973e27b0f4291aeeee461213caf721f8eb9d009bb8e41e5f3c`.
CLI version smoke passed; log /tmp/bicdb-bolt-optimize.log.

Valid canonical short screens, zero failed queries/watermark0/all33VUs:
- restart-join-pgo-control-r1:542383.5NOPM, Payments1083351/1083477,
  WAL18033055853bytes, sampled peakRSS40019238912.
- restart-bolt-optimized-r1:551820NOPM, Payments1104458/1104583,
  WAL18387138695, sampled peakRSS40508076032.
- restart-bolt-optimized-r2:550664NOPM, Payments1103057/1103186,
  WAL18322796224, sampled peakRSS44293226496.
Reverse exact control restart-join-pgo-control-r2 is running. First pair+1.74%,
not a proven keeper; r2 RSS also needs its paired control. No600k achieved.

Separate worktree `/home/benchmark/bicdb-wal-sparse`, branch perf/hammerdb-wal-sparse,
commit d7139aea adds opt-in BICDB_WAL_COMPACT_FRAME=1. It keeps public Record
serialization/content hashes unchanged, but full WAL writes omit absent optional
record fields and live-only statement_snapshot (existing reader defaults0).
Recovery applies absolute committed writes without live conflict validation;
retained-WAL replication also does not consume that snapshot. The existing
decoder reads these sparse tx_write frames without a format/version change.
404 core unit tests(3ignored),277 SQL units(3ignored),415 SQL integrations,
17 cell-row and2 authorization canaries pass with compact-frame+repair+graph+
single-buffer enabled. Added field-preservation/hash, rejected-stub, and
commit/abort/savepoint/repeated-reopen regressions. Additional recovery
integration checks are running. No throughput or resource gain claimed yet.
Worktree isolation kept the numeric PGO build source frozen while implementing
this independent candidate. Both candidate branches have been pushed.

## Parallel Spot benchmark capacity; latest screens

User explicitly authorized starting existing `benchmark-secondary` as Spot and using
it for parallel testing. Azure start succeeded, priority Spot, eviction policy
Deallocate, D32ads_v5, IP192.0.2.12. SSH confirms EPYC9V74,32 logical/16 physical
cores and125GiB RAM. Canonical acceptance host remains bench-1(192.0.2.11);
do not pool unpaired cross-machine results or use benchmark-primary. Both VMs now running.
Preserved bench-2's existing checkout; created detached worktree
`/home/benchmark/bicdb-parallel-20260905` and copied the exact bench-1 harness there.
Restored the same immutable seed archive (SHA2360d91973281a8a8d0ee61c0910550d7dac89b9ce84221040deb75f6ecaedeb)
and layout-A(SHA3eafccfdf53bea54b3c37061299e0764c500e03a63d3a96337b27622806fa6d5).
First bench-2 control `parallel-join-pgo-control-r1` uses exact d1015aa5 binary;
no bench-2 score available yet.

Completed bench-1 short screens (1 minute ramp +1 minute measurement; DBNOPM
uses district delta across both minutes, NOT the HammerDB last-minute number):
- Reverse join-PGO control r2:534715NOPM, Payments1068809/1068939,
  WAL17787603509, sampled peakRSS39611367424. Together with542383.5 r1,
  working optimized-control mean538549.25. BOLT551820/550664 is NOT a keeper:
  two pairs insufficient for positive95%CI, and r2 RSS exceeds its control>5%.
- Numeric identity inside cast body: PGO524794.5, normal469321; both valid,
  zero failed queries/watermark gap, all33 VUs complete. No demonstrated gain.
  PGO warning discards up to477775785 central-cast counts after CFG change.
- Sparse WAL frame r1:470871.5NOPM, Payments940915/941027, zero failed queries,
  watermark gap0, all33 VUs. Graceful reopen district1421903 exactly matches
  post-run. Need same-binary flag-OFF control before attribution. Additional
  backup_online_wal_rotation, mvcc_chain_recovery, wal_acknowledgement_race
  integration tests all pass. Artifact a57a5b72bc3c78d001de1127f77810382ec28f114872554338ad3c306f8fe3b7.

Numeric wrapper8a5595fc preserves the original central cast function body and
exports a separate fast wrapper.277 SQL units pass(3ignored). PGO build finished
13m29s; saved/deployed `bicdb-numeric-wrapper-pgo`, SHA
9407d2687d8afa99cb0446a3f694159c582d297a75c913aeb0e97c1eb207aa6f.
Central cast mismatch is absent, but inlining changes OTHER caller CFGs,
including slot_row_from_cells(up to1408672399 counts discarded). Thus this does
NOT completely solve stale-profile bias. First canonical screen running.

Independent INTO ownership candidate d2ead257 moves returned SQL values into
routine slots instead of deep-cloning them.278 SQL units(3ignored),415 SQL,
17 cell-row and2 authorization tests pass. Saved normal artifact
`bicdb-into-owned`, SHA fdf02f1e23c40c240a96ef9bd8d3a38851db28e6feed126b18b160351bde5c46,
now staged on bench-2; not benchmarked yet. No600k result or goal completion.

## Two-host runs and integrated profile training

Both hosts use identical HammerDB image digest
sha256:2f572150794859a5d2559d44646febfed33f0592dcccfda4c22bb5ed6542b827.
Bench-2 first optimized control:549991NOPM, Payments1099084/1099203, valid,
zero failed queries/watermark gap. Do NOT interpret host difference as code gain.
Bench-1 numeric-wrapper PGO:535841NOPM, Payments1072842/1072992, valid with
zero failed queries/watermark gap; still no demonstrated numeric throughput win.

Sparse-frame same-binary balanced short screens are complete:

| Pair/arm | DB NOPM | Generated WAL bytes | Sampled peak RSS bytes |
| --- | ---: | ---: | ---: |
| 1 ON (first) | 470871.5 | 14201768216 | 35981012992 |
| 1 OFF (second) | 468183.5 | 15433755002 | 36190248960 |
| 2 OFF (first) | 466055 | 15385474116 | 40528551936 |
| 2 ON (second) | 470136.5 | 14189802368 | 37390610432 |

All valid, zero failed queries/watermark gap, all33 VUs successful; first ON
also passed graceful reopen. Repeated approximately8% WAL reduction with
throughput approximately+0.7%, NOT a>=2% throughput keeper. This is a promising
resource reduction for the integrated optimized build, not a600k claim.

Bench-2 ordinary-release control for INTO ownership:484220NOPM versus first
candidate483217.5(-0.2%); valid but no gain in first pair. Reverse pair queued.
These ordinary-build numbers do NOT replace the optimized~539k bench-1 baseline.

CTE-copy candidate ec4c929f passed276 SQL units(3ignored),415 SQL integrations,
17 cell-row and2 authorization checks. Normal binary `bicdb-cte-copy` SHA
f3a821ae47a5531b06c062c48ea192fdbe4eb91c0ca8cf416925e097fa269635.
Build exact published-main control in the same worktree before attribution:
publication formatting/module ordering differs from the old52883a62 artifact.
New BOLT no-split layout uses exact d1015aa5 input and the same workload profile;
`bicdb-join-bolt-nosplit` SHA86227835b6112fbe9d31047f5d32d54dbf2df1edafe89a253f3b88a097deed17.
Long(1+2 minute) balanced optimized control/no-split pairs now queued on bench-1.

Integrated branch source dfac5dbe combines numeric wrapper, INTO ownership,
CTE-copy avoidance and opt-in sparse WAL.279 SQL units(3ignored),415 SQL,
17 cell-row,2 authorization and404 core units(3ignored) all pass with sparse
WAL/repair/wait-graph/direct notification enabled. Fresh matching PGO training
build is running with fatLTO/codegen1/x86-64-v3 and relocations retained.
Keep source frozen until it finishes; instrumented throughput is excluded.
No new performance keeper and no600k result yet.

## Longer optimized-layout pairs; checkpoint-lock target

Long canonical runs(1 minute ramp +2 measured) on bench-1:

| Pair | Arm | Verified NOPM | WAL bytes | Sampled peak RSS bytes |
| --- | --- | ---: | ---: | ---: |
| 1 | original PGO control | 531051.6667 | 26544209852 | 54221783040 |
| 1 | BOLT no-split | 550383.6667 | 27562726347 | 54576222208 |
| 2 | BOLT no-split | 546769.6667 | 27378356216 | 52741951488 |
| 2 | original PGO control | 530549.6667 | 26590084560 | 53621075968 |
| 3 | original PGO control | 533311.6667 | 26688032917 | 51997249536 |

All valid, zero failed queries/watermark gap, all33 VUs complete. Third candidate
is running. First two pairs average+3.35%; n2 Student-t95%CI still crosses zero.
Resource deltas within5% in those pairs. Need further pairs, latency and recovery
before promotion. Short-screen and ordinary-build scores must remain separately
labeled: the optimized baseline has NOT fallen to the ordinary-build~470k.

Bench-2 completed ordinary-build screening:
- INTO candidate483217.5/481432 vs numeric controls484220/484324.5: no gain.
- CTE-copy481347.5 vs exact published-main control481397: neutral.
- Inline row scratch476277 vs exact-main480297: first pair approximately-0.84%,
  no promotion. Independent branch perf/hammerdb-stack-cells,3b962613;
  402 core units(3ignored),275 SQL units(3ignored),415 SQL integrations,
  17 cell-row and2 authorization tests pass. Artifact `bicdb-stack-cells`, SHA
  e6c49130f2fea53204ac5eb012058b56204017a652f7d65ed490aff0045cc859.
Exact-main normal control source6709089a, SHA
65d04041cd682bbf910a8942c3f82a48ba2b52598fdc2d5e76876cffc4d26953.

Long-run control logs identify a pipeline-wide pause: checkpoint exclusive-lock
truncate phase takes1581,1881,2323,2025,1996ms while replacing a multi-GB WAL.
`copy_transaction_log_tail` copies the suffix, atomically replaces the WAL, then
`tx_log.invalidate()` closes the old inode inside the database write lock.
Hypothesis: final old-inode reclamation is a substantial part of this pause.
Independent branch perf/hammerdb-wal-reclaim,11fa41e4 adds opt-in
BICDB_CHECKPOINT_DEFER_WAL_RECLAIM=1(Unix): hold a read-only old-WAL File until
AFTER the database guard and write admission are dropped. Existing copy/fsync/
rename/invalidation order unchanged. Failed handle acquisition merely falls back.
Log reports total time including reclamation, with existing parser-compatible
truncate/total fields.76 pgwire units and158 protocol integrations pass with flag
ON, including durable recovery/new-commit routing while the old inode is held.
Normal artifact `bicdb-wal-reclaim`, SHA
122409eec321a9c6e1a2446659dc7fa078002778a2f2a665f248e8f6f684aea7;
not benchmarked yet. Do NOT assume reclamation caused the whole pause.

Integrated PGO instrumentation build completed26m15s, functional sourcedfac5dbe
(later branch commits only journal/ignore rules), fatLTO/codegen1/x86-64-v3,
relocations retained. `bicdb-integrated-pgo-train`, SHA
a9145bddfe7bc3d21eab0bfd80fc6d768791bfcd664af401da220259dbf58b5e.
Training `parallel-integrated-train-d1` running on bench-2, explicit warmup/profile
classification excludes its throughput. LLVM's reset/dump interfaces are present;
bounded GDB capture will attempt reset at measurement start and dump90seconds
later, excluding seed-open/ramp/shutdown. Failure must be recorded, not silently
called a workload-only profile. No debugger on throughput trials.
