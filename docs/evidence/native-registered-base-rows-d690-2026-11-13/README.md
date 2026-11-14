# Registered native base and pending-row WAL evidence — November 13, 2026

Production qualification remains incomplete. This packet contains **two terminal
runs only**, both at runtime candidate **1.0.494-beta**,
`d6908095e0d347e563ef68801f49e52dba35452d`. It does not test main 1.0.498-beta
or a later documentation release. No release binary, deployment, independent
physical scale-out or production qualification was produced.

| Run | Core / compile job | Actual result | Scope |
| --- | --- | --- | --- |
| [Base4: 43486854857](https://github.com/nikoma/bicdb/actions/runs/43486854857) | 132097541321 / 132097483301 | 4 passed, 0 failed; 31.95 s | Native base publication, missing-final refusal, same-xid cancellation recovery, historical floor after image invalidation |
| [Rows5: 43487075852](https://github.com/nikoma/bicdb/actions/runs/43487075852) | 132098295025 / 132098251107 | 5 passed, 0 failed; 35.33 s | Complete rows, missing audit, pre/post-WAL cancellation, durable-WAL apply failure; three pending WAL-lease controls |

Both runs passed workspace all-target compilation and the separate workflow
consumer without Core defaults. Those are compile checks, not broad runtime
regressions. Builds used one Cargo job, incremental compilation disabled and four
Rust test threads. Five native node instances communicate through actual pinned
loopback TLS on each shared GitHub runner. They mostly share one test process;
the apply-failure control uses an isolated child process. They are not five
independent machines. **Elapsed test time is neither RTO nor p50/p95/p99 latency.**

## What passed

The Base4 positive cases published the actual selected range's native base and
immutable archive together at page xid21, bound to the verified final-image
pointer at xid20 and R1. Repeated requests returned the same receipt. Cancellation
after native WAL retained xid21 before postchecks; a fresh request reconciled
that same publication. Missing final receipt refused publication. After a real
image-generation change, historical integrity still reported R1 while the
current image contract refused. The range catalogue remained Installing; no
catalogue floor was adopted and no writer or Serving admission was granted.

Rows5 exercised all five intended seams. Three completed copies each reconciled
one exact acknowledged source record, 177 serialized bytes, stored/audited
history and target visibility B0→B1. Their other target range remained empty and
its unknown native point stayed unchanged. Missing stored-history audit refused
row WAL. The genuine inner `BICDB_TEST_FAIL_COMMIT_APPLY` cut retained the
original logical Tx23, native intent xid12 and known logical sequence1 as
`WalDurablePendingApply`; progress stayed zero and fresh retry remained fenced.
This internal copy operation did not return a successful application commit.

The new same-open WAL observations are separate from cold recovery:

| Actual pending state | Retention observation | Result |
| --- | --- | --- |
| Pre-WAL cancellation | One actual lease, same pending Transaction/intent; zero leases after committed progress | Passed |
| Applied-known cancellation | One lease preserved the exact 568 logical-WAL bytes through an explicit ServerPaged checkpoint; same-Tx progress released the lease, and a later explicit checkpoint cut the log to zero bytes | Passed |
| Durable WAL, failed apply | One lease remained owned after Transaction consumption; retry stayed health-fenced and no progress/new Transaction appeared | Passed; no checkpoint attempted on the unhealthy database |

The pre-cut WAL SHA is recorded by the actual test and cross-checked among its
reports. The original pre-cut raw bytes are not separately archived here.
Automatic `auto_checkpoint_if_due` is guarded to EmbeddedMemory in this source;
this packet does not claim an automatic ServerPaged log cut was reproduced.

## Ledger accounting

The nine fixtures recorded **18 successful source application API calls**, zero
failed/uncertain source API calls, and zero separately recorded caller retries.
These are `ClusterEventClient.append` outcomes, not raw-wire attempt counters;
internal wire retry counts were not measured. All 27 actual remote OLD Close
calls succeeded, and every fixture retained three OLD protected records with
identical pre/post fields, native xids and checksums through fresh collection.

Keep internal target outcomes separate: two row-copy cancellations reconciled,
one row-copy CommitOutcomeUnknown remained retained/fenced, one missing-audit
request refused, one native-base cancellation reconciled at the same xid, and
one missing-final request refused. Rows exercised three fresh target retries:
two completed and one remained fenced. Seven fixtures completed and checked the
selected-range record on the target. The other source range's acknowledged
record was not claimed copied; this is not an 18-commit failover RPO measurement.

## Portable verification and custody

Run without GitHub access or an engine build:

```sh
python3 docs/evidence/native-registered-base-rows-d690-2026-11-13/verify.py --self-test
```

An exported directory also works: invoke its `verify.py` directly. Optional
`--repo /path/to/bicdb` additionally verifies all source-file hashes at their exact
commits. The verifier checks the file inventory, full decompressed raw-log
hashes, exact run/source/job identities, both projection inventories, all four
native-base outcomes and all three WAL-lease reports. Its two adversarial
controls reject a changed native archive and a wrong final pointer. It never
opens a database, recreates a native grant or interprets a status DTO as authority.

`artifacts/*-public.zip` contain only explicitly whitelisted logical reports,
source/toolchain identity and test logs. Projection manifests retain every
omitted file's original path, length and SHA-256; omitted database/TLS/custody
contents and custody-manifest files are absent. Full original ZIP digests remain
pinned in those manifests. Private originals were kept separately under private
permissions and are not committed. The original DB files were copied with live
owners held stable, followed by cleanup: these are not stopped, atomic or reopened
snapshots. Hashes of omitted files do not validate cold replay.

`manifest.json` pins this packet's payloads and four complete compressed job logs.
`source-index.json` distinguishes the measured d690 source from the optional
read-only predecessor reports. Legacy generic terminal flags remain conservative
metadata-baseline labels; detailed component reports identify the actual rows,
final-image and native-floor work that ran.

## Boundaries and next tests

[issues.json](issues.json) classifies passed controls and untested gates with
small deterministic next experiments. No catalogue adoption, joint NEW/decision/
Open completion, Ready/Serving/Strong routing, cold continuation, application
transparency, independent index/FTS/queue/audit parity, large-data migration,
controller convergence or capacity gain is established. Independent progress for
another range during stalled fsync is also untested. No physical RPO/RTO or
p50/p95/p99 result is claimed.

The optional cold and distributed-SQL reports in `findings/` are **read-only
source/design findings**, not additional terminal runs. Their exact commit and
file hashes are retained. The cold report proposes restrictive durable-debt
admission and an exact resolver; neither was implemented/tested by that report.
Its cut-site inventory is not proof that every listed path executes for a given
configuration: automatic EmbeddedMemory housekeeping, explicit ServerPaged
checkpoint and guarded paged-open reset are different paths. Source inspection
found no proof that missing imported-projection WAL flags synthesize fresh
business events on recovery; that claim is not made. The SQL review establishes
no transparent gateway, cross-range relational executor or durable production
SQL coordinator.

The original Azure qualification remains failed. Earlier `7ac` failed cohort and
`10d` passing cohorts retain their original sources/results and are not rewritten
or bundled into these two d690 runs.
