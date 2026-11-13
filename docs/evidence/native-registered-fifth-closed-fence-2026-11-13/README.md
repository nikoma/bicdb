# Registered fifth closed fence: first ten-case cohort failed 9/1

Exact executed source **`da56e489a34c1776e97a4ea089c1f250215cf8dc`**, package
**1.0.491-beta**, DATA protocol **29**; [run43351805110](https://github.com/nikoma/bicdb/actions/runs/43351805110).
Core job **131644446840**: **9 passed, 1 failed, 0 ignored, 980 filtered,
17.87s**. The original required genuine-fifth test and eight controls pass.
The foreign-DB control fails its expected-error assertion. The whole cohort and
workflow remain **FAILED**. All-target/isolated-consumer job **131644404794**
and Formatting **131644404499** pass. `results.json` binds all ten full test
names/results to the complete raw log, rather than inferring names from counts.

Every fixture first obtains two genuine mTLS handoff fence replies at the actual
Registered fifth, n4. Range1/2 are epoch2 in committed Cgen5, initiated by n1/n2.
The target retains its same actual open, birth, protected metadata Store and
range Store slot, with no writer policy. Four original source writer guards
and two full external source ACK payloads remain. Across ten **separate**
fixtures this is twenty fence replies and twenty source ACKs, not twenty
business writes in one cluster. The original required fixture's two replies
have original caller/role allowances **4874/1372ms** and **4855/1353ms**.
They persist refusal/freeze facts; they grant no application write/read or Ready
authority. Target native points remain **UNKNOWN/None**, not native R0, and
Core B0 is not a native logical prefix. No rows are copied.

The eight passing controls cover generic ingress/cold-context refusal, captured
caller cancellation, original empty-hook ABA, Store-slot restoration, receiver
stop, actual fsync followed by caller cancellation, actual write/IO ambiguity
and exact-live-operation refsync, and a real durable invalidation. These are
source-current component checks, not execution of an older binary or cold
recovery. `derived.json.gz` retains all ten exact terminal reports from the raw
log, including all source ACKs, logical fence replies and before/after states.

The failing Foreign case already acquired and checked a genuine valid target
capability. The actual foreign INITIAL service then refused earlier, at the
protected native birth/bootstrap/local-member gate:

```text
cluster error: native range image: native recovery birth/bootstrap/local member binding mismatch
```

The old expected-error assertion accepted only different later identity wording.
It panicked at `closed_fence_controls.rs:150`; the failed case did **not** reach
its final control-report append or post-control target-state assertions.
Its actual pre-control intent, valid-capability prerequisite, exact refusal,
primary failure and cleanup are retained. There is no foreign afterstate proof
in this run. Test-only correction **`550c23d0458ab7a4e10af94d1cc625a7eb69bb89`**
matches this one exact Cluster detail and adds actual foreign DB before/after
inventories plus Arc/birth non-alias checks. It is **not executed by this run**;
no result for that successor is inferred here.

## Raw and custody boundaries

Full raw Core SHA256:
`1466c4cbbfadfa2ee9c2994690e3c418464db22fea5a5e463a73e26e82300a44`
(4,284,708 bytes). Full compile and formatting logs and their byte/source/job
indices are included. EOF/BrokenPipe diagnostics are retained, not rewritten
as a zero-error claim or assigned a cause.

Private artifact **14004955628**: **2,390,349 bytes**, SHA256
`57732a282f49f5e0204d81c06521ac4cdaed96abf7bd07c39d6e5d9f08899664`,
1,042 ZIP entries, 39,605,562 uncompressed bytes. Ten original custody manifests
verify **960/960 files, 29,152,656 bytes**, with no missing files. At capture all
five metadata/service/DB/native-journal guards were held in each fixture;
engine owners were alive and listeners were not drained. These are guarded
observed-state copies, not atomic power-loss snapshots, native-page decoding,
engine reopen, WAL replay or post-crash reconciliation. Cleanup reports no
errors and preserves the original Foreign failure.

The restricted ZIP contains native DB and mesh-signing files. **Neither that
ZIP nor its DB/signing/TLS payload is published in this packet.** The portable
receipt retains aggregate custody hashes/counts, safe logical diagnostics and
raw job logs. Optional private verification reads ZIP bytes/hashes without
extracting or opening any database. These verifiers establish retained-byte
integrity and internal result consistency, not independent execution authenticity.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-registered-fifth-closed-fence-2026-11-13/verify.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-registered-fifth-closed-fence-2026-11-13/semantic_tamper.py
# Restricted optional check: add --private-zip PATH to verify.py.
```

This closes only the original required **two-fence** component prerequisite at
this exact source. Assignment is explicit Core consensus orchestration, not
automatic public relocation. No OLD closed DATA quorum, remote image stream,
materialized target, NEW DATA quorum, target ACK/Strong, automatic convergence,
8GiB capacity, cold DATA recovery or independent-VM qualification is established.
The original Azure FAILED verdict and earlier a98 required **0/1** remain intact.
