# Closed C1 image: executed 8e component result

Exact full-prototype source `8e537222796648c1c652a8a73c87ffece9b53551`,
`1.0.490-beta`. [Machine-readable receipt](native-closed-c1-image-8e-result.json)
retains selectors, all function names, source, raw hashes and artifact inventories.

| Cohort | Actual result | Compile and isolated consumer |
| --- | --- | --- |
| [Image 43333163664](https://github.com/nikoma/bicdb/actions/runs/43333163664), Core `131582678444` | **5 passed, 0 failed**, 960 filtered, 7.88 s | Job `131582633227` passed |
| [Role 43332875879](https://github.com/nikoma/bicdb/actions/runs/43332875879), Core `131581679752` | **0 passed, 1 failed**, 964 filtered, 0.02 s | Job `131581649020` passed |
| Earlier image dispatch `43332872622` | Canceled by same-ref workflow concurrency **before tests** | Not tested |

The corrected image dispatch used immutable ref `qual/native-c1-image-8e5372227`.
Both executed cohorts retained `--test-threads=4`, `RUST_TEST_THREADS=4` and
`CARGO_BUILD_JOBS=1`; no deadlines or workers changed.

The image positive used real native OLD quorum/capture and committed Pending
metadata while retaining the original C1 writer policies. It completed actual
controlled clear/copy, asserted the full original ACK row, **higher target data
B**, unchanged namespace IDs/policies and a still-frozen target. Numeric B values
were not printed; the executed source assertion is `after > before`. The log
emitted `copiedOriginalACK=true originalPoliciesUnchanged=true targetFrozen=true
noServingOrNewACK=true`.

The four passing refusal controls independently exercised actual writer
replacement while old objects stayed held; original BEGIN caller cancellation
after authorization/pre-WAL; separately acquired step-role parent cancellation
at that cut while BEGIN/caller remained live; and actual OLD observation owner
drop. Reached hooks and unchanged row/data-B/namespace assertions were mandatory
where applicable. Real source committed-slot inclusion and conservative target
nonempty-slot refusal also ran; no slot/proof DTO manufactured authority.

The role companion is still a failed test: its real local-mint cancellation loop
completed, then the component transport used initiator n2 where the real
acquisition API required authenticated recipient n1. Its transport cancellation
assertions were not reached. [The retained failed receipt](native-recovery-role-cancellation-8e-first-result.md)
describes the subsequent test-only caller correction; it does not change this
source's failed result.

Private raw/index files remain under `/tmp/bicdb-native-foundations-ci/`, with
prefixes `8e-c1-image-` and `8e-c1-role-`. Image artifact `13996127246` has ZIP
SHA256 `0440643acf84a5c40170000af118327fe2d6615e6a270374588f61533ace7dc6`
(41,015 bytes); role artifact `13996776041` has SHA256
`014591de8ff30bbd518b5c6ada844436c214898b2a20703e0f3e0a9c772c9c05`
(40,592 bytes). Both contain four source/toolchain/test-log files, all CRCs and
source identities checked. **No database roots were retained**, and no engine
open or replay was performed during capture.

This qualifies the original-C1-writer image component only. Genuine Registered
receivers without an INITIAL writer, compatible nonempty NoData domains,
all-owner cold/expired-operation continuation, remote image/session kernels,
multi-range fifth-node population, final NEW writer/serving admission and target
next ACK/full Strong remain open. Prior failed no-reinstall image68 and Strong7b
cohorts stay failed. These focused results do not qualify the whole prototype,
operator flow, physical HA, or final fault self-healing.
