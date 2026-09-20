# Payment completion is not financial correctness — 2026-09-05

> Publication note: infrastructure identifiers and machine-local paths are
> anonymized examples. See [source distribution notes](source-distribution.md).

**Follow-up:** compact WAL recovery now opens the retained corrected-Payment
dataset. Its full audit passes for all 480,000 customers, including balances
and payment counters, and district/warehouse YTD totals. This does not repair
the older stock-procedure dataset or establish a durable throughput result.
See [the recovery and audit evidence](recovery-compact-validation-2026-09-05.md)
for exact results, order/Delivery checks and the fresh persistent-filesystem
acknowledgement crash test. The chronology below retains the original failures.

The VM2 longer-wait diagnostic completed every Payment and New-Order call,
but fails a stronger financial-state reconciliation. Do not describe it as
100% business-correct or use its NOPM as a correctness-qualified result.

## Full customer audit

Read-only post-recovery audit of the quiescent `wait20000-d1` dataset:

| Check | Result |
| --- | ---: |
| Customers examined | 480,000 |
| History rows, including seed | 2,155,934 |
| Balance discrepancies | 80,842 |
| Customer YTD-payment counter discrepancies | 285,046 |
| Customer payment-count discrepancies | 285,046 |
| District / warehouse YTD discrepancies | 0 / 0 |

Expected customer balance is negative total customer payment history plus
current delivered-line amounts minus seed delivered-line amounts. The exact
hash-matched seed has all balances -10.00 and one 10.00 history entry per
customer. It already contains 168,040,553.24 in delivered-line amounts, which
must be subtracted rather than treated as new credits. Current and seed
delivery amounts are joined to the owning customer through orders. District
and warehouse YTD totals reconcile to history's payment-location fields.

The captured procedure never updates `c_ytd_payment` or `c_payment_cnt`;
those two findings are workload omissions, not evidence that the engine
discarded requested column updates. Balance discrepancies still require
causal attribution; do not call all 80,842 lost database commits.

## A shared procedure defect reproduced on PostgreSQL 18.6

The captured Payment body iterates its surname cursor only `name_count / 2`
times using integer division. One matching customer means zero fetches. It
then assigns the caller-provided initial balance minus the payment amount,
instead of first loading that customer's actual balance.

On an isolated clone of the original PostgreSQL seed, warehouse 1 / district
1 / customer 221 has the unique surname `ABLEABLEBAR`. Its balance is -10.00.
Calling the original procedure by surname with amount 5.00 and the driver's
initial balance 0.00 changes it to **-5.00**, not **-15.00**. The probe ran in
an explicit transaction and was rolled back; no retained benchmark state or
original seed was modified. This reproduces a business error on PostgreSQL
without concurrency. It does not establish the cause of every discrepancy.

The benchmark driver indeed initializes the balance INOUT argument to 0.0:
`/tmp/bicdb-correctness-hammer-driver.log`, line 322. The same cursor bound and
stale-variable assignment also appear in the inspected
[upstream PostgreSQL workload source](https://github.com/TPC-Council/HammerDB/blob/master/src/postgresql/pgoltp.tcl#L840).
Independently, assigning an earlier read balance is not an atomic decrement
under read-committed concurrency; engine-to-engine runtime tests must separate
that case from the singleton cursor defect.

## Evidence and next boundary

- Audit: `/tmp/bicdb-financial-reconcile-vm2-20260905.jsonl`, SHA-256
  `0cacb2b19b96866721cc1b1c4e9f1460feebe786b50a67a1fafe05ce75ca164a`.
- Seed customer delivery totals: `/tmp/bicdb-financial-seed-delivered.csv`,
  SHA-256 `5328884e834cf0f2e179012b771335af275739a3a2e343f7df8b6a231d18de5f`.
- Seed history/YTD checks: `/tmp/bicdb-financial-seed-invariants.csv`.
- Rolled-back PostgreSQL probe:
  `/tmp/bicdb-payment-singleton-pg186-20260905.json`.

An experimental corrected Payment body rounds the median cursor position up,
uses an atomic row-relative balance decrement, and maintains both payment
counters. It must be tested identically on both engines and labelled as a
corrected-workload diagnostic, not silently substituted for stock HammerDB.
No corrected-workload throughput or financial pass is claimed here.

## Paired singleton probe

The isolated probe subsequently reproduced the same stock error on BicDB's
canonical pre-repair binary. Both engines return balance -5.00, YTD payment
10.00 and payment count 1 after the 5.00 payment, while history totals 15.00.
With the same corrected procedure, both instead return balance -15.00,
YTD payment 15.00 and payment count 2, matching history. All four calls ran
inside explicit transactions; rollback restored the prior customer and
history state in every case. This is a serial singleton test, not yet a
concurrent workload qualification.

- Probe: `/tmp/bicdb-payment-parity-probe.py`.
- Results: `/tmp/bicdb-payment-parity-probe-20260905-r2.jsonl`, SHA-256
  `1b782a28f012278fda3f13e05c21ccc5829fe000b6a5651830a07535a86b7bab`.
- Identical executed corrected DDL SHA-256:
  `5bcc1136d18cb13588bd8bb00371a9dc1bb67a9f9bfa5df5fe1f780b5a69d23f`.
  The explanatory leading SQL comment was removed on both engines because
  the older BicDB binary rejects that prefix before CREATE OR REPLACE PROCEDURE.

The first full corrected-workload attempt, VM2 `corrected-v1-d1`, is invalid:
installing the replacement as the administrator changed execution access and
the benchmark role received permission-denied errors. It was stopped and all
artifacts/data retained. The next trial installs as the existing routine owner,
`tpcc`. This setup issue must not be counted as workload performance evidence.

## Full corrected-workload diagnostics

Both trials use the same corrected SQL file (including its explanatory comment),
SHA-256 `fd6b0ee841d011d6e0d188a95f0d13149b842bb071269077622d9435c33d5a86`.
The file is applied through psql, which does not transmit that leading comment
as part of the procedure command. These are **not stock-workload throughput
keepers or an apples-to-apples performance pair**: PostgreSQL runs on VM1,
BicDB on VM2, and BicDB uses the buffered CPU-ceiling lane on tmpfs.

PostgreSQL 18.6, `/home/benchmark/bench-out/codex-payment-pg-corrected-20260905`:

- 16 warehouses, fixed layout A, 32 active users, 1+2 minutes; all VUs complete.
- Final history rows 2,325,638; final district sum 2,325,400.
- Post-run financial audit: all 480,000 customers, zero balance, YTD-payment,
  payment-count, district-YTD or warehouse-YTD discrepancies. 49.706 seconds.
- Financial result `/tmp/bicdb-financial-reconcile-pg-corrected-v1-20260905.jsonl`,
  SHA-256 `754cc7886a86ca53cbcc7e7162bb9fc635db50faf51f8f169d83e7cf88335b96`.
- Reported HammerDB score 621,129; whole-run district-delta/three-minute score
  615,080. Neither is a stable paired reference from this single diagnostic.

BicDB VM2 `codex-payment-corrected-20260905/corrected-v1-d2`:

- Binary SHA-256 `4938714482eb40b52ba5e45e0f94b26401dfb701e869d02d799624ce2e4f04d5`;
  its actual source provenance is in the snapshot-count repair document. The
  remote harness checkout SHA in metadata is **not** the binary source SHA.
- 1,707,408 New-Order commits / calls; 1,708,002 Payment commits / calls.
- Zero failed queries, handled serialization/deadlock exceptions or watermark
  gap. All 33 VUs complete. 21,375 handled no-data exceptions remain separate.
- Whole-run district score 569,136; HammerDB score 578,931; actual elapsed
  187.556 seconds. Buffered tmpfs and stale PGO: not a qualified speed claim.
- Generated WAL 29,302,358,104 bytes; sampled RSS peak 52,157,800,448 bytes.
- Financial qualification pending recovery audit; exact call counts are not
  sufficient proof.

The first recovery attempt was OOM-killed at 09:07:20 UTC, PID 139053, with
88,081,356 KiB anonymous RSS while tmpfs held about 41 GiB including retained
datasets. This is an availability failure, not proof of missing commits. The
closed corrected dataset was relocated with every file hash verified to
`/mnt/codex-retained-audits/bicdb-codex-payment-corrected-20260905-corrected-v1-d2`.
The earlier closed `wait20000-d1` dataset is also retained under that parent.
Azure resource-disk storage is temporary, **not host-durable**. Their per-file
manifests are `relocated-data.sha256` in the respective campaign directories.
Original logs remain unchanged; the second recovery uses a separate log.

Code inspection identifies a concrete architectural target:
`recover_transaction_log` first reads all WAL frames and accumulates decoded
writes for all transactions. Replay then retains those writes while constructing
collection versions, with garbage collection explicitly deferred. Bounded
recovery must preserve commit-sequence ordering, patches, savepoints, aborts,
materialized-commit handling, broker/audit recovery and legacy compatibility;
simply discarding old frames would be incorrect. No recovery-memory fix is
claimed in this diagnostic.

The disk-backed retry also exceeded physical RAM. A temporary 64 GiB swap file
was enabled solely to attempt the audit; even then the process used roughly
125 GiB resident plus 60 GiB swap without becoming ready. That retry was
intentionally stopped to prevent another host OOM, and swap was disabled.
Neither recovery attempt completed the BicDB financial audit. The 20-second
recovery CPU sample (`recovery-disk.perf.data`, 980 samples, zero lost) points
to `decode_tx_frame`, JSON deserialization and map insertion; the decoder
expanded raw metadata into `Record`/Value trees instead of `StoredRecord`.
The candidate fix is isolated in `fix/recovery-compact-writes`; its four focused
tests pass, but broad verification and full-scale validation are still pending.

The financial audit above checks balances and Payment counters, not every
TPC-C field. Further procedure inspection also finds no `c_delivery_cnt`
maintenance in Delivery and no `s_ytd`, `s_order_cnt` or `s_remote_cnt`
maintenance in New-Order. These shared workload omissions require separate
accounting; the PostgreSQL financial pass is not a claim of full TPC-C
specification conformance.
