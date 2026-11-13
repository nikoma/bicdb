# Observer inner spans: eedf prerequisite failure

Exact source `eedf23d58e236cf62d61366349ad4de3bd52373b`, version `1.0.489-beta`; run43319438217, Core job131535993682, filter `native_current_joint_activation_scope_actual_`. The workflow is terminal **failure**. Its all-target and isolated no-default consumer checks passed (job131535934325); Core is **0 passed, 4 failed, 0 ignored, 953 filtered; 81.00 s**. The test-only [nested instrumentation](native-ready-observer-inner-spans.md) is compiled but **never reached**: the complete raw log contains zero `NATIVE_PROBE_TRACE` events, and the profile contains zero contexts and zero spans. There are no per-challenge inner costs to report.

| Actual case | Exact Strong prerequisite error |
| --- | --- |
| Missing decision archive | DataQuorum unavailable; n1/n2 probes time out |
| Writer replacement | DataQuorum unavailable; n1/n2 probes time out |
| Original caller cancellation and exact fresh recovery | Canceled stage; query timeout |
| Foreign actual DB/Store | Canceled stage; query timeout |

All four fail in `native_strong_point_tests.rs:475`, the shared `observed` outcome assertion, called by the actual target n3 StrongR2 at `complete_joint_tests.rs:949`. This occurs **before** dispatch to any activation-scope control. The existing flow has already completed all-three joint release, committed target ACK2 and fsynced its two-record ACK ledger, asserted all three new voters' actual R2/history and both acknowledged rows, retried the historical R1 release while preserving R2, and rejected the stale old member's write. None of the four reaches CurrentJoint preparation, Complete, activation-scoped validation, its requested fault, or the new nested timing spans. No source68 receiver-validation attribution transfers to this earlier failure.

The original top-level fixture parent is uncancelable. The Strong request has its existing 5000 ms budget and real first metadata observation; the `Canceled` stage labels above are actual outcome labels accompanied by query-timeout errors, not evidence that a test deliberately canceled its top-level parent. The present log does not establish whether receiver validation, local/remote lock contention, allocation or another source accounts for those Strong timeouts. No worker change, timeout increase, retry, production-check removal, or payload/data-loss explanation follows from this result.

## Exact retained evidence

Private directory: `/tmp/bicdb-native-foundations-ci`. No engine was opened or replayed during capture or verification.

| File or artifact | SHA-256 |
| --- | --- |
| Full raw job log `eedf-observer-inner-cuts-job131535993682.log.gz` (decompressed hash) | `ac9b8e8d8ca220b817e89a0d5f23b2b40b9bf5c5fce5ba5c57e52e158b6da791` |
| All-target/consumer raw job131535934325 (decompressed hash) | `7c8a7c7f09484157ea6ff7c723b2c9dda92d96569fead44ddd7a74c26a01454a` |
| Empty actual profile `eedf-observer-inner-profile.json` | `9f9143c194fe58c00fef93b1f1d85a379986a4b5a2717b67cdebe9ea6a17240f` |
| Artifact13989698674 ZIP | `27ec0d1a8a6d212f04e6e611ec6af098ffc228b25639ec21977cf62ee0c08665` |
| Four-copy verification receipt `eedf-observer-inner-cuts-artifact13989698674-receipt.json` | `b74cdcc17f211739ce1fb60cd25de9b766a10f156da25ab9744ced5d6cea21d1` |
| Terminal receipt `eedf-observer-inner-final-summary.json` | `bbe41064f0c516053797ad758532650838484d7fa12b4a1d1b37772789511d71` |

Every ZIP digest/source revision and every manifest file was verified. The four manifests each contain 75/75 files and 4,677,766 bytes: **300 verified files, 18,711,064 bytes total**. Each real ACK ledger contains two records. Current raw recovery-fence maps are empty for n1/n2 and retain n3's original image-session `Active` phase, not a CurrentJoint NativePrepared state. These raw phase labels do not grant serving or a successful read. The copies are observed-stable while actual engine owners remain alive, not atomic power-loss snapshots or independent native WAL replay.

Manifest SHA-256 values, in actual failure order:

- Missing archive: `ad8569abb2da459c6365dd467a5350d332a195cd477c4e16e6d72a6702e6da63`.
- Writer replacement: `bd4deb897f443e37ab43315dee4c7be3d4baaf260b8bf5d75fdc35e056eac8b5`.
- Original caller: `1ed2a3f1a27b0670b6d8a3c48e8363431389cb5deee344da1f6329184f4b3b51`.
- Foreign DB/Store: `61cc24e335f43ac02401ebb4fd08fe5fe2142fcd58c04da81fa6f5fb8c9a522e`.

This receipt covers only the four-control run. The sibling eedf original-positive/idempotence and target-anchor/archive runs have separate owners and receipts. Prior exact68 controls remain 3 pass / 1 fail in their original source-specific record; this run qualifies none of those fault controls and provides no observer optimization evidence.
