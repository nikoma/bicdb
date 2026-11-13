# INITIAL routing discovery: actual held-service positive and owner refusals

**Three component controls passed** at exact private source
`26cdaa87c446ca35542ec816b29f069d57f47242`, run `43284689289`, Core job
`131414888059`: **3 passed, 0 failed, 0 ignored, 933 filtered**, 5.93 seconds.
All-target plus isolated no-default consumer job `131414845527` also passed.
This is an implemented/tested private component fix, not a public source
merge, binary/release/deployment, production/physical qualification or a
completed automatic 4-to-5 operator.

## Exact executed controls

All three are in
`distribution_native_host::provisional_listener::tests::initial_data_tests::routing_discovery_tests`:

- `native_initial_data_healthy_metadata_discovery_while_all_data_services_held`
- `native_initial_data_routing_refuses_wrong_actual_service_and_stopped_owner`
- `native_initial_data_routing_refuses_replaced_store_with_old_actual_owner_retained`

The positive uses three actual from-birth INITIAL factories and their real
mTLS listeners/metadata quorums. The existing fixture's actual n0 metadata
election is a declared precondition, not automatic-election or independent-
process evidence. All actual writer policies/readiness precede the first ACK.
A separate thread holds all three actual DATA service latches; the public RPC
caller/observer holds no engine latch. BOTH real fresh HostMQ acquisitions
succeed while these latches remain held, with distinct original business and
context IDs, metadata n0/term1/read-index1 and unchanged committed C1.

The SAME public client now refreshes successfully. Its three exact
BootstrapSnapshot requests (IDs 3/n0, 4/n1, 5/n2) each receive the **full actual
committed topology**, with real response request/source/birth identities.
The observed dispatch records retain generation-1 addresses; the response
contains routing DATA, not a metadata quorum/read permission. There is no
public-client FetchTopology fallback. A separate actual FetchTopology RPC
WHILE HELD still receives the exact immutable INITIAL unsupported-operation
refusal; that control is in the audit/source assertions, not separately
raw-frame-journaled. Listener/control/blank-receiver allowlists are unchanged.

After release, the same client obtains its second business ACK on n1-led
range1 for JSON null (request6), then Strong **present null** (request7,
R1/local B2). It Strong-reads the exact original n0-led range3 value/history
(request8, R1/local B1, snapshot2). The original ACK was request1, followed by
its first Strong read request2. R and local B remain distinct: this is two
business ACKs in two different ranges, each at R1. All three actual owner
statuses remain current, the actual post-release snapshot equals the original,
and explicit owner stops precede strict positive final acceptance.

The second test uses two genuine actual admissions: the wrong service refuses
while both service locks are held. It then actually stops/drains the original
owner; retained opaque inputs cannot restore routing while service/DB locks
are held. The third replaces the actual service Store with a different-root
actual Store, retaining the OLD genuine Store and its still-live original
writer registration. The real TLS request, while service+DB locks are held,
must return the exact current-slot-ended/changed refusal rather than timeout.
Restoring that exact old Store cannot revive its revoked slot binding. Both
negative tests assert unchanged actual native R/H/local B/commit sequence;
they create no application ACK/Strong/serving grant. Their proof is the
source-bound executed assertions; no separate per-negative DB archive exists.

## Mechanism and limits

The controller retains the actual open guard already validated at creation.
The sole real INITIAL admission constructor captures the existing revocable
current service Store-slot guard. The INITIAL-only Bootstrap producer checks
actual service pointer/slot/Store ID/life/writer/open before and after its
bounded OWN metadata lock, validates the actual attached MStore, then clones
and validates its committed snapshot. It reacquires no DATA service/DB latch
and mints no MQ, NativeReady, Strong or mutation authority. Generic/control/
registered bootstrap remains unchanged. A later operation still needs its
own actual current route/writer/role/quorum checks; a hint can become stale.

The run also logged **27 EOF and 7 BrokenPipe** server diagnostics. All remain
in the raw log and observation index; this is not a zero-error claim. Their
individual causes/durable ordering are not inferred from buffered success
output. The actual native roots/page WAL were not archived, and no independent
WAL replay, ACK reconciliation or physical incident diagnosis was performed.

## Portable receipt and verification

`raw/core-job131414888059.log.gz` is the complete actual job log, raw SHA-256
`22f68ce30e613ed6a18b28f47aca47d6437e9b2fce1f4d02ff71dbf628a6bb67`.
The complete all-target/consumer job log is separately retained, raw SHA-256
`2d024f92d4eeb135a18c862312cae2faef43c3a06601a2de79a2c82f24c10231`.
Exact artifact `13973124819` has SHA-256
`67dd7d3b211a3dd311a435e40c835add6a571516b8b0dd799479ded52320346c`;
all six original payloads (595,288 bytes) are preserved under `artifact/`
(five direct copies, original `tests.log` losslessly stored as `tests.log.gz`).
The verifier compares every decompressed payload byte to the exact original ZIP. Full public-client request/response frames, IDs, routes, timestamps
and complete available errors remain in RPC JSONL. Actual successful HostMQ
constructors/context data are audited; their internal network exchanges are
not independently wire-journaled. No private TLS keys or DB roots are included.

`source.patch.gz` and `source-files.json` independently reconstruct/hash
1,768 tracked files in the declared crates/Cargo/.cargo/scripts/workflow roots
from public read-foundation base `b9394418bdf9d87864c2c12d6fe81bb7d4039d53`.
It needs no other evidence folder, does not mutate the checkout, and does not
reconstruct unrelated historical documentation/the original Git commit.
Original raw patch bytes are losslessly preserved in gzip. Historical receipts
are not modified and later completion-scope changes are not covered by this run.

```sh
python3 verify.py --repo /root/bicdb
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_verify.py
```

Verification checks package hashes, exact ZIP/extracted bytes, source/job and
successful compile-step identities, all three test results, held MQs, full
three-member snapshots, independent fallback refusal and exact APP/Strong
values with R/local B. Seven semantic negative controls reject source/member/
context substitution, wrong response identity, stale snapshot, missing explicit
allowlist refusal and present-null-as-absence. These verify evidence; they are
not new engine executions and are not added to the three-test cohort.

The original expected RED remains separate at source
`05979463ef312fc1128a42903f908de0189c2a63`, run `43278532377`: 0/1 at final
acceptance after real held MQs, busy Bootstrap plus refused fallback, and
post-release ACK/Strong. Its portable receipt is separate docs-only
`00f413d6086e75e83b82028643f972107cc7d3c4`. The preceding 286 fixture-binding
failure also remains separate. Neither those results nor this three-test pass
establish the cause of the older `3ee` CLI/Azure failure. No automatic scale-out,
remote image/joint/controller, broad Core/application or physical gate closes
by borrowing these component results.
