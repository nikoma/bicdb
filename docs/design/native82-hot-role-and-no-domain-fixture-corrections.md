# Native 82-case qualification: bounded fixture corrections

The retained qualification at `88ffe19f40ffd4ee83fb63a062795380b385bb33`
(runtime `5a85900f571c389ff97e278a0b9555048bdeadec`, source `1.0.502-beta`)
remains **failed: 70 passed, 12 failed, 0 ignored, 941.30 seconds**. All 82
compiler-listed cases executed. These source/test corrections have not been
compiled or run and do not change that historical result.

The original run is [43544810818](https://github.com/nikoma/bicdb/actions/runs/43544810818),
Core job `132293554417`. Its retained decompressed job log is 22,693,709 bytes,
SHA-256 `241738e911657e0ed0e6d975f5ddf14ff10076f40e222699f859bf945a1181d0`.
The gzip bytes have separate SHA-256
`70a713e0c0fb0adacf401f67e5b0e3d6f3a7f12982fc88ff904d357b81f360e5`.

## Eleven common handoff-witness refusals

All eleven hot-policy, post-movement DATA and local-term controls failed at
`remote_source_tests.rs:283`, the `witness_recovery_role` result in `actual_role`.
The exact shared error was:

```text
cluster node n0 rejected RPC cluster_data_operation_failed: cluster error: metadata consensus: metadata recovery-role identity, purpose or committed configuration changed
```

The retained terminal records place these failures in
`actual_remote_original_a_through_first_policy_component`. Each contains its
two original source APP acknowledgments. Their SDK observer explicitly says
`native_phase_rpcs_in_this_observer: false`; it is not a capture of the failing
native role request or proof of APP/Strong behavior after movement.

The frozen fixture, retained topology, actual remote-OLD collector ledger and
successful source-frame records establish the concrete actor error. The copied
node0 MStore JSON records `role=leader`, `leader_id=n0` in all eleven cases:

| Identity | Original fixture |
| --- | --- |
| Selected range | `r1` |
| Range leader | `n1` |
| Metadata RPC rejector/leader endpoint | `n0` |
| OLD voters | `n1`, `n2`, `n3` |
| Retiring replica | Usually `n1`; follower variant `n2` |
| Selected donor | `n2`; follower variant `n3` |
| Handoff initiator expression | `clients[donor].local_metadata_read_participant()` |

`remote_old_seed` deliberately selects a donor different from the range leader
and retiree. `hot_policy_tests::exercise` then incorrectly reused that donor as
the mutation initiator. The production `CommittedHandoff` predicate requires
the initiator to be the range leader **or** metadata leader. Neither selected
donor satisfies that predicate in these retained configurations.

This confirms a fixture violation explaining the immediate admission refusal.
The donor is explicitly recorded by the actual collector. Its use as initiator
is derived from the exact fixture expression and actual retained topology;
it is not represented as newly captured native RPC evidence or proof that every
later phase would succeed after correction. The intended policy, replacement,
decision, local-term and post-movement APP/Strong cuts were not reached through
this failing admission path.

The correction selects the actual range-leader transport participant already
used to establish the initial plan. Donor OWN, source publication, actual remote
OLD collection and all application/fault assertions remain intact. A bounded
actor-selection report is written before target image admission. No production
role predicate, credentials, lease, deadline, retry policy or SDK changes.

Relevant sources under `crates/bicdb-core/src`:

- `distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/hot_policy_tests.rs`
- `distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests.rs`: `remote_old_seed`, `actual_role`
- `distribution_metadata_read_provider.rs`: `witness_recovery_role`
- `distribution_consensus.rs`: `MetadataRecoveryRoleRequest` committed-handoff initiator validation

## One NoDomain error-boundary assertion

`native_registered_fifth_remote_step_no_domain_actual_mstore_term_change_refuses`
reached its actual term mutator and then failed at
`target_no_domain_tests.rs:113`. Its exact observed refusal was:

```text
cluster error: metadata consensus: actual metadata authority version changed/dirty; retained native outcome is not a current grant
```

The original code expected only the later owned-member-state error. Actual
`NativeOwnedMetadataMemberObservation::require_current` checks its original
metadata version before the older term/configuration comparison. The copied target MStore records term 2 while node0 remains at term 1. This is
a local-term mutation observation, not evidence of a new elected leader or cold
recovery. The actual term mutation invalidates that original version, so the earlier refusal is
consistent with the intended fence. The original later before/after state
assertions were unreached and are not retroactively described as passing.

The correction accepts only the two exact `BicDbError::Cluster` strings for
these specific boundaries. Generic Cluster, timeout, cancellation or unavailable
errors do not satisfy the control. It retains the actual term mutator and
strengthens the observed resulting-term check. It records mutation intent,
mutation result, original version, exact validation result, parent/OWN window
and read-only native before/after observations before the error assertion.
Diagnostics perform no engine open and do not grant admission. All existing
native/raw/row/UID preservation assertions remain.

Relevant sources under `crates/bicdb-core/src`:

- `distribution_native_host/provisional_listener/tests/learner_enrollment_tests/promotion_tests/registered_image_baseline/remote_source_tests/target_no_domain_tests.rs`
- `distribution_native_host/provisional_listener/registration/registered_replica/no_domain.rs`: `require_metadata_current`
- `distribution_consensus/native_metadata_store/owned_member.rs`: `require_current`
- `distribution_consensus/publication_version.rs`: original-version refusal

## Next checks and limits

Qualify the corrected original twelve cases on an exact recorded successor,
then retain every newly reached phase and any new failure before changing it.
The source correction preserves the original test names and all 82 original
bodies outside these two narrow helper/expectation changes. The actual compiler
list must still be retained and verified; no tests should be dropped to obtain
a green result.

A successful successor would establish only its exercised component boundaries.
It would not qualify current-main HA, remote phase-protocol completion,
independent-machine four-to-five-node convergence, cold recovery, the 8 GiB
campaign, sustained throughput gain or general distributed SQL. The failed
Azure experiment and its unchanged production gate remain independent.
