# Native Strong read across an actual receiver gap and same-voter transfer

This is an unversioned, source-only regression proposal based on
`881087806b3a5a9792b3e0ba46ba1ba1f2dd0617`. It has not been compiled or executed.
It does not establish an original Azure cause, linearizability, sustained
availability, automatic placement, cold recovery or a physical RTO.

The existing controls separately exercise a real CQ/pending refusal and a
retained stale SDK gateway across same-voter transfer. The latter starts with
the original ordinary Apply instructions delivered to all voters. This new
control requires both situations in one actual cluster and retained history.

## Actual cut and required acceptance

The new test is
`native_event_endpoint_same_stale_gateway_cq_gap_transfer_recovers_original_acked_payload`
under the existing `native_strong_point_tests/endpoint_tests` module.

1. The unchanged genuine fsynced born-v2 `EnforcedNetwork` fixture installs
   actual host policies and stock mTLS listeners/transports. Its initial
   metadata election and later same-voter leader choice remain declared
   fixture decisions; this is not an automatic election/operator test.
2. A new cfg(test)-only arm belongs to the original actual `n2` database/open,
   one canonical event record and range. It may refuse one real authenticated
   ordinary Apply only after all original admission/authority/config/base
   checks. The service has already required its actual durable CQ. The cut is
   before `begin_transaction`, so this selected receiver performs no row WAL
   or native pending write. No recovery Apply is intercepted.
3. The SAME stock SDK gateway appends a full nested payload larger than 16KiB
   and must receive an actual APP ACK. Immediately afterward the test requires
   the fault's actual node/open/command identity, actual `n2` raw CQ containing
   the SAME command, native index0, no native pending record and absent physical
   row. The actual origin command must contain the exact expected full payload.
   The old leader must serve that ACKed payload through genuine Strong. The
   missing receiver state is then checked again; a background repair or
   automatic retry that already closed the gap fails this prerequisite.
4. The existing actual metadata quorum producer commits the same-voter epoch
   change to leader `n2`. Actual fresh host enrollment and dispatcher publication
   occur; publication is not asserted to be Strong readiness. Old-route wire
   Get must Redirect. Published `n2` must return typed native data-quorum or
   snapshot Unavailable, rather than merely lacking a dispatcher. A Get through
   the SAME retained stock SDK gateway must return an error in the gap, never
   successful absence. Before recovery, actual CQ/body/native0/absent-row state
   must still hold.
5. The existing actual fresh recovery-role/fence-quorum producer fences every
   original voter. Authenticated fenced-history RPCs discover the SAME retained
   original command on all three members. The external payload/ACK is compared
   against this history, not used to manufacture a command, certificate, floor,
   Ready result or permission. Existing historical certify/apply RPCs perform
   recovery. Actual `n2` native prefix and full original row must then match.
6. Existing actual Prepare/Complete RPCs run on each voter. Existing trusted
   LOCAL `activate_native_recovery` runs on those same runtime owners and scope,
   followed by ordinary coordinator recovery and dispatcher publication. The
   local activation API is not described as an incoming authenticated endpoint.
   A final Get through the SAME gateway must return the exact original full
   payload.

Each observable API return/cut is emitted before its corresponding assertion.
The fixed fixture journal also records intents, full original command identity,
value hashes, actual native/CQ/row observations and primary failures. Errors are
bounded to 4096 characters with a truncation field. The outer panic boundary
records and resumes the identical panic; it does not turn failed/unreached
prerequisites into passing characterization. Existing setup helpers retain
their own unwrap/assertions; a failure inside one is reported as an unreached
stage and original panic, not a completed downstream cut. This is in-process
test evidence, not an independent external ACK ledger or power-loss proof.

## Timing and ownership boundaries

The SDK client expression is copied exactly from the existing
`endpoint_tests/stale_gateway_tests.rs:11`:
`f.event_client(initial.clone(), Duration::from_secs(20))`. Actual gap/final
elapsed time and API outcome are recorded. It is a 20s endpoint component
configuration, not the native hot fixture's 2s, OWN 1.5s, phase 5s windows or a
qualified recovery-time objective. No existing control duration/assertion is
changed. Every SDK operation retains its own original absolute retry deadline;
this test does not restart an in-flight operation's timer. No APP is replayed.

The arm has a separate 30s diagnostic registration lifetime, is one-shot,
same-open/range/record selected, and is disarmed by its sole RAII owner. It does
not retain a DB owner, State, writer, mutation permit, role or certificate;
the stored observation is copied DATA. Its registry lock is released before
matching/observation. It never waits or holds an engine lock across a test
callback. A nonbusy refusal avoids the ordinary transient-busy retry path.
All production admission and native completion code remains unchanged when
`cfg(test)` is disabled.

## Four-state report

| State | Current evidence |
|---|---|
| Passed | Standalone formatting/diff/source-preservation checks only; recorded in the source-freeze receipt |
| Failed | No test execution occurred, so no runtime failure or pass is assigned |
| Inconclusive | Actual runtime reachability/timing and success of all retained-owner recovery producers await execution; a real refusal must remain a failed/unreached prerequisite |
| Not tested | This combined control, physical process/volume/cold failures, automatic placement/election, repeated movement and sustained concurrent-append availability |

The six historical c05 focused PASS results remain at their original source
and raw log identities. Their four containing source files are unchanged here
except the existing endpoint file gains this new module declaration. None is
rerun or reclassified by this proposal. The separate concurrent-append/snapshot
test is not added: no new callback is installed under a borrowed read/journal
scope to manufacture a mutation or bypass its gate. Its lawful selected actual
mutation/recovery schedule remains a separate control requirement.
