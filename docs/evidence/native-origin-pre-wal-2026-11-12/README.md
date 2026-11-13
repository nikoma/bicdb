# Native origin pre-WAL fault: executed recovery baseline

Classification: **FAILED required autonomous recovery**. Exact unmerged source
`721c329de4196aeef96604642c439fe7d8ddb0bf`, candidate `1.0.489-beta`,
[run 43325316433](https://github.com/nikoma/bicdb/actions/runs/43325316433).
Core job `131556248799`: **1 passed, 1 failed, 957 filtered, 26.04 seconds**.
The arm-owner/drop/retained-observer control passes. The actual fault test reaches
its unchanged terminal requirement and fails; expected RED is not a pass criterion.
Separate job `131556196584` passes all-target and isolated no-default-consumer
compilation. No test was restarted, waived or relabeled.

This is three genuine INITIAL NativeHost owners/RF3 and actual TLS in one Core
test process. Initial metadata election uses real vote/barrier APIs. It is not
three CLI processes, independent hosts, automatic four-to-five DATA placement,
a VM fault, cold recovery or a source of Azure causality.

## Actual cut, after transaction and normal APP cleanup

The same public EventClient first receives one complete-object ACK and exact
Strong value. Actual native history covers old command index1 on range3,
epoch1/leader n0; original visibility is typed local B1, not a cast from R1.
The old command is `01a1d203-eced-76a3-81d6-651e47ef9420`, SHA-256
`e80627c3af881154fa8dafe85ad6d5c141f192d7ae3f213671a02e677c310052`.

The one-shot receiver fault runs after actual native admission and mutation-scope
validation, before the origin's application WAL sequence is allocated. Its arm
is scoped to the actual DB open/range/event and cannot be kept armed by a cloned
observer. It changes no production build. First-origin facts are immutable:

| Actual observation | Retained result |
| --- | --- |
| Core result | `CommitOutcomeUnknown`, sequence **None**, actual transaction11 |
| Actual ticket/command | `01a1d203-ef53-7341-85a5-e6bac0e8fcfc`, index2, range3/epoch1/leader n0 |
| Command SHA-256 | `7fee4349b1a0e064547f80ca2af069a222169a23c2cea553eddff08b24dabcad` |
| Cleanup boundary | Actual release callback returned OK; normal APP dispatch branch exited after consumed transaction and DB guard destruction; no panic/integrity error |
| Origin after that exit | Raw CQ2; actual native R1/Pending2, no materialized pending index; native lineage not invalidated |
| n1 and n2 | Actual native R2 with equal H0/H and separately typed local B2; each immutable native history covers both actual commands; both complete old/new records match |

These facts precede all post-fault Strong calls. They are runtime assertion and
observation evidence, not a reconstructed native proof from raw CQ, row equality,
a synthetic abort, a visibility-counter seed, or a timeout. Later SDK attempts
cannot overwrite the first origin result. A legitimate future retry ACK would
be separately recorded and checked against actual native history/full value.
This execution receives no such ACK.

## Complete observed client attempts and API outcomes

The journal has **1,176 contiguous records**: 13 logical intents and 13 replies,
1,134 actual RPC phase records, actual command/native facts, two health rounds,
supported metadata/registry maintenance and the final stop-attempt report.
Every workload EventClient RPC is observed from before its first call. The
original authenticated setup Bootstrap call is recorded as data but explicitly
has `bootstrap_raw_frame_covered=false`. Supervisor/maintenance peer traffic is
not a complete global wire ledger.

There are 281 observed transport attempts: 15 APP, 65 typed Strong and 201
metadata-snapshot discovery requests. All 201 discovery replies are successful
full snapshots at generation1. Complete workload replies comprise one APP ACK,
one first-origin Unknown, 13 recovery-incomplete APP errors, one pre-fault
Strong value and 59 post-fault typed `DataQuorum` unavailable replies. Five
trailing Strong attempts retain partial/error paths instead of a complete
response. Prepared frames, positive byte counts, full errors, native source
births, request IDs, actual Strong business/challenge IDs, route and generation
remain in the original journal. A prepared frame alone is not a dispatched RPC.

The faulted SDK call issues four actual APP requests (IDs3/7/11/15). The first
returns the real Unknown; later attempts refuse incomplete leader recovery.
Its final API result is `no event cluster member is reachable`. That aggregate
message is not interpreted as all nodes being offline or as rollback.

The eight subsequent Strong API calls return no successful observation, and
neither of the two next-write API calls ACKs. Each next-write call sends five
actual APP requests and receives recovery-incomplete replies. All logical
outcomes and full input values/hashes are linked to their actual attempts in
`events.json`; immutable framed bytes and complete phase/error chains are in
`original-artifact.zip`. The diagnostic operation UUID is explicitly not a
wire business-ID or command certificate.

The existing configured RPC/attempt timeout remains 2 seconds; this is not a
claim of a hard whole-call/OS-IO bound. Health-round receipt times are
**7.649538381s and 15.333160068s** under an explicit 20-second success limit.
The fixture stops sampling at 15.333465020s because less than eight seconds
remain for another four-call round. It does not establish continuous observation
through second20. No round obtains old/new exact Strong plus next ACK/Strong.
Any successful false absence/wrong value after an ACK would fail immediately;
this execution reports neither a successful post-fault value nor absence.

## Custody and limits

The original six-file artifact is unchanged: 211,301 bytes, SHA-256
`8528db4cb8a55a32eca222a1c695f120370edc8b996db6b3c50b8905e68e66a8`.
It contains only the journal, terminal JSON, source revision, compiler identity,
selected-test output and test output; no native DB/WAL/catalogue or TLS key files.
Complete raw Core/compile logs and their indices retain exact source/job hashes.
The journal records the actual test executable SHA-256
`7eaf74e3c85616fefb40e36a2cb20dcf8575b5d92e22a2075e3686378cc39693`,
135,730,224 bytes. That executable is not archived or independently build-attested.

All explicit stop attempts return without errors. `/tmp/.tmp2G3uBX` is only the
runner's retained original-root path, **not portable native DB custody**.
No engine was reopened, replayed, repaired or fsynced by evidence verification.
The native cut was inspected after actual APP cleanup and before health calls;
there is no independently replayed final post-stop native state. No permanent
loss or rollback of the unknown operation is established. The earlier 56f CLI
unknown remains separate; this deliberate injection does not diagnose its cause.

Run `PYTHONDONTWRITEBYTECODE=1 python3 verify.py --repo /root/bicdb --tamper-tests`
here. Verification checks original bytes, all logical/RPC correlation, first
Unknown/cleanup/native observations, exact failed gate and six semantic tamper
refusals. Optional repo checks match eight selected files at the exact source;
they are not a full runtime reconstruction, a new execution or a native replay.
The actual [baseline source](https://github.com/nikoma/bicdb/blob/721c329de4196aeef96604642c439fe7d8ddb0bf/crates/bicdb-core/src/distribution_native_host/provisional_listener/tests/initial_data_tests/origin_pre_wal_fault_tests.rs)
and [receiver probe](https://github.com/nikoma/bicdb/blob/721c329de4196aeef96604642c439fe7d8ddb0bf/crates/bicdb-core/src/db/range_image_native/certified_prefix/origin_fault.rs)
remain pinned to the unmerged candidate. Main source publication, binary
publication, deployment and physical qualification are separate states.
