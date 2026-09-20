# Same-seed PostgreSQL 18.6 / BicDB comparison

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

Fresh comparison requested by the user on September 4, 2026 (runs September 5
UTC). All six clean-headroom runs finished; the corrected protocol and excluded
initial BicDB runs are recorded below. No engine speed winner is established.

## Final results

Database-verified NewOrder throughput using the historical configured 1+2-minute
denominator, NOT HammerDB's reported-only score:

| Repeat | PostgreSQL 18.6 NOPM | BicDB PGO+BOLT NOPM |
| --- | ---: | ---: |
| 1 | 563407.0000 | 559068.3333 |
| 2 | 606449.6667 | 560250.0000 |
| 3 | 444442.3333 | 551457.6667 |
| Mean | 538099.6667 | 556925.3333 |

Actual driver-elapsed verified NewOrders/minute averages: PostgreSQL507,554.7;
BicDB534,329.2. These are separate metrics, not a change to the600k goal's
canonical denominator. PostgreSQL's very wide variation prevents claiming a
reliable speed win: paired PG/BicDB mean ratio0.9654, n3 Student-t95% interval
[0.6100,1.3207], also subject to the first pair's extended protocol-correction gap.

All33 VUs complete in all six runs. Every NewOrder call matches a district-counter
increment. PostgreSQL Payments4,841,353/4,841,353 committed; BicDB
5,008,487/5,009,164 (99.9865%). BicDB has zero failed pgwire queries and zero
watermark gap, but still reports handled serialization/no-data exceptions.

Delivery business counters for repeats2 and3 combined: PostgreSQL2,464,430 orders
from315,631 calls (7.80795/call); BicDB2,097,367 from334,076 calls (6.27811/call).
PostgreSQL delivered about24.4% more orders per Delivery CALL. This is NOT a
transaction-success-rate or latency measurement. It establishes that similar
NOPM does not mean equivalent completed business work across the full mix.

Final audit verifies all32 actual logged terminal assignments in each run against
layoutA, identical HammerDB image IDs, identical PG settings/procedure snapshots
across repetitions, and identical generated Tcl between engines except port.
Same boot ID before/after. No benchmark engine/container remains running; both
seeds remain, and completed PG data archives are preserved on bench-2.

Machine-readable checked-in results: `hammerdb-same-seed-pg186-results.json`.
Full local evidence: `/tmp/bicdb-apples-evidence/final`; canonical remote evidence
root and archive checksums are documented below. Stock-Level plan/data-access
instability and lower delivered work per CALL are follow-up investigation targets,
not explanations proven by these throughput runs. BicDB has NOT reached600k.

## Protocol

- One VM: canonical bench-1, `192.0.2.11`, 32 logical CPUs, AMD EPYC 9V74,
  approximately 125 GiB RAM. One engine/workload at a time. Same boot ID:
  `652e019a-1818-48bc-8689-4c5c727d1dc0`.
- Same HammerDB image, 16 warehouses, 32 active workers plus monitor, stored
  procedures, no key/think time, time profiling disabled. Same driver template
  and captured terminal layout A. One minute ramp-up plus two minutes measured
  for EVERY run. Database-verified NOPM divides the entire district counter
  change by all three minutes, identically for both engines.
- Three repetitions each, preplanned order: PG1, BicDB1, BicDB2, PG2, PG3, BicDB3.
  No perf instrumentation, debugger, builds, or heavy transfers on the host
  during throughput windows. BicDB's usual lightweight statistics remain on;
  PostgreSQL uses `track_functions=all` to check procedure calls.
- Same logical starting data, not separately randomized seeds: create an isolated
  PostgreSQL HammerDB schema, truncate only its generated workload tables, then
  COPY all nine tables from a disposable clone of the existing BicDB seed.
  Explicit column lists preserve values despite storage ordering differences.
  Re-export PostgreSQL and compare SHA-256 of C-locale-sorted COPY text for every
  table, plus exact row counts. All **8,081,732 rows** match, including duplicates.
- All five PL/pgSQL bodies match after removing BicDB's stored CREATE wrapper
  and surrounding whitespace. All ten index definitions match after normalizing
  unquoted identifier case and the public schema qualifier. All 92 workload
  column names/base types/character lengths match. BicDB's information_schema
  omits numeric precision/scale, so that catalog check alone does not verify
  numeric typmods. Both schemas were created by the same HammerDB loader.
- Fresh clones of each stopped seed for each trial. Original BicDB seed is
  never opened for export or testing. Removed only the disposable export clone
  and one-table COPY scratch after verifying all hashes; seeds/evidence remain.

## Artifacts and settings

Remote evidence root: `/home/benchmark/bench-out/codex-apples-20260905`.
Seed-copy progress, per-table hashes/counts, procedure/schema/index snapshots,
container identities, settings and per-run raw counters are preserved there.

BicDB candidate is the existing PGO+BOLT no-split build, not the fresh integrated
PGO experiment and not an ordinary release build:

```
bicdb-join-bolt-nosplit
86227835b6112fbe9d31047f5d32d54dbf2df1edafe89a253f3b88a097deed17
```

Canonical tuned-durable settings: MAQ/MAR32, MAW24, sharded indexes64/orch16,
RC attempts2, repair enabled, wait-graph, key notification, key wait5000us,
single-buffer WAL, compact update WAL, compact frame OFF, sync outbox OFF,
checkpoint WAL trigger4096MB. `PERF_STAT=0` for this engine comparison.

PostgreSQL image is pinned to the previously verified 18.6 image:

```
postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280
```

`fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, `shared_buffers=8GB`,
`max_wal_size=4GB`, `checkpoint_timeout=5min`, `jit=off`, autovacuum enabled.
Engine-specific cache and checkpoint algorithms are not claimed identical.

HammerDB image ID:
`sha256:2f572150794859a5d2559d44646febfed33f0592dcccfda4c22bb5ed6542b827`.
Terminal layout SHA-256:
`3eafccfdf53bea54b3c37061299e0764c500e03a63d3a96337b27622806fa6d5`.
Driver template SHA-256 on the test host:
`9244792320b806e3baeec538b60ab0379041a378252369dd5c6ba820bb47dfbd`.

## Interpretation limits

This is a **tmpfs, synchronous-commit-path comparison**, not a physical-disk or
power-loss durability benchmark. Logical seed values match; physical layouts
naturally differ. Per-worker randomized transaction streams are not replayed
byte-for-byte, so repetition remains necessary. Committed NewOrders are checked
by district counters and Payments by history row counts, independently of CALL
completion. Other procedure completion does not automatically prove business
success when exceptions are handled internally.

The old 676,986.5 PostgreSQL reference used independently generated data, an
earlier boot and a shorter 1+1-minute run. It is retained, but is NOT a paired
control for this experiment. Do not discard a fresh lower PostgreSQL result or
selectively substitute the old high-water result.

The first BicDB launch failed its `rustc` PATH prerequisite before starting the
database or workload. Corrected the launch environment, preserved the failure
log, and continued the preplanned order without repeating PostgreSQL1.

## Resource-control correction before the final comparison

PostgreSQL1 completed at 563,407 verified NOPM. The first two BicDB trials were
558,203.3333 and 555,533.6667, both logically valid. However, PostgreSQL1's stopped
data directory still occupied 8.9 GiB of tmpfs throughout those BicDB trials.
Keeping completed datasets in RAM would progressively change later trials'
available memory. Those two BicDB results remain preserved under `bicdb/trials`
but are excluded from the clean-headroom comparison, regardless of their speed.

Stopped the sequencing shell only, let the second BicDB workload and cleanup
finish normally, and archived PostgreSQL1's completed data to bench-2. Archives
are compressed outside throughput windows, checked for identical SHA-256 after
transfer and tested with `zstd -t` before removing only the corresponding
campaign-owned tmpfs data directory. All remaining PG runs use this cleanup.

PostgreSQL1 began without leftover trial datasets and remains eligible. All three
BicDB repetitions are rerun under `bicdb-clean/trials/bicdb-clean-rN` with no
completed PostgreSQL dataset resident. The remaining order is BicDB-clean1,
BicDB-clean2, PG2, PG3, BicDB-clean3. Each new trial requires no running engine or
container and over 50,000,000 KiB free tmpfs before starting; pre-run memory and
filesystem snapshots are saved. The first PG/BicDB pair therefore has an extended
gap for the retained exploratory runs and this protocol correction.

## Business-work verification beyond NewOrder and Payment

Added a read-only observer for repeat2 and repeat3 of each engine. It waits for
HammerDB's `ALL VIRTUAL USERS COMPLETE` marker, then counts `new_order`; it never
queries during the timed workload. With the verified initial144,000 pending
orders, delivered orders =144,000 + committed NewOrders - final pending orders.
This measures orders delivered, NOT successful Delivery transactions: one CALL
can deliver between zero and ten orders, and empty/contended districts matter.
Do not describe orders-per-CALL divided by ten as a transaction success rate.

Repeat2: BicDB delivered1,059,861 orders from168,638 Delivery calls (6.2848/call).
PostgreSQL delivered1,406,250 from182,539 calls (7.7038/call). PostgreSQL did
32.7% more delivered-order business work in that same three-minute window,
while its NewOrder rate was606,449.6667 vs560,250 (+8.25%). This is why similar
NOPM alone must not be presented as equivalent successful work across the mix.
BicDB's reported handled exceptions are retained, not hidden by zero failed
pgwire queries. Repeat3 subsequently confirmed the orders-per-CALL difference;
combined results are in the final summary above.

Archived PostgreSQL completed datasets are recoverable on bench-2 under
`/home/benchmark/bench-out/codex-apples-20260905/archived-pg-data`:

| Trial | Archive | SHA-256 |
| --- | --- | --- |
| PG1 | codex-apples-pg-r1-data.tar.zst | bd3b195ff2717e07ce765dc00ab261257423429837b0567aab15c3022e6da8ed |
| PG2 | codex-apples-pg-r2-data.tar.zst | 03a52acff3e048da9d376119e474c39cb9f4a57dd3093ba4634db4f0005a27f1 |
| PG3 | codex-apples-pg-r3-data.tar.zst | 69f06755710303dfaa3c3257a5c7b71dd639450959bdd22050cbb177de4e148a |

## PostgreSQL variation: retained, not cherry-picked

PostgreSQL repeats were563,407;606,449.6667;444,442.3333 configured-window NOPM.
Settings snapshots for repeat2 and3 are identical, pre-run MemAvailable is
approximately121 million KiB in both, terminal layout is replayed, and neither
run has a logged ERROR/FATAL/deadlock. One NUMA node is exposed by this VM.

`pg_stat_user_functions` locates most of the timing variation in Stock-Level:

| PG repeat | Stock-Level calls | Total procedure time, ms | Mean ms/call |
| --- | ---: | ---: | ---: |
| 1 | 168079 | 1052827.251 | 6.2639 |
| 2 | 182255 | 531393.713 | 2.9157 |
| 3 | 133043 | 1970780.998 | 14.8131 |

NewOrder mean time is approximately0.85–0.88ms across those same runs; Payment
approximately1.00–1.13ms. Thus a uniform host slowdown does not explain the
observed profile well. A Stock-Level query-plan/data-access issue is a hypothesis,
NOT an established root cause: actual nested plans were not captured. Do not
discard repeat3 or announce an engine speed winner from this noisy PG mean.

Also record actual driver wall time, not only configured ramp+measurement:
PG1=187.1796s, PG2=197.3322s, PG3=187.0532s. BicDB-clean1=187.6400s and
BicDB-clean2=187.5744s. The separate elapsed metric is committed NewOrders *60 /
actual driver seconds; it includes startup/tail overhead inside that interval.
This is distinct from historical canonical NOPM and must not silently replace
that denominator when discussing the600k goal.
