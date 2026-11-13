# Role cancellation: first focused 8e result

**Failed.** Exact source `8e537222796648c1c652a8a73c87ffece9b53551`,
`1.0.490-beta`, run [43332875879](https://github.com/nikoma/bicdb/actions/runs/43332875879),
Core job `131581679752`: **0 passed, 1 failed, 964 filtered**, 0.02 s.
The all-targets and isolated no-default consumer job `131581649020` passed.

The test loops through local mint first, then component transport. The first
iteration completed the real local-provider mint and original-parent
cancellation assertions. The second failed at provider line 839, **before** its
transport observation/cancellation assertions: `metadata consensus: recovery-role
recipient is not its authenticated member`.

The request's recipient was n1 and initiator n2, but that component transport
called the real acquisition producer with n2 as its authenticated caller.
`validate_recovery_role_caller` correctly requires the recipient n1. The narrow
test correction supplies actual participant n1 to this acquisition only; n2
remains the unchanged initiator in the request. All original cancellation,
deadline and expiry assertions remain. This is a fixture identity error, not
evidence that the cancellation guard failed. The whole original run stays failed;
its one successful loop iteration is not a passing test or transport result.

Private retained raw/index directory: `/tmp/bicdb-native-foundations-ci/`.

| Retained item | Exact identity |
| --- | --- |
| Core raw | `8e-c1-role-job131581679752.log.gz`, raw SHA256 `38d20ccccecb1818b04aabb7aa015b58b505432bf5911950fa9c44af21df5436` |
| Compile raw | `8e-c1-role-compile-job131581649020.log.gz`, raw SHA256 `d21e7814ffdc385dbacf5b82837a5a93725385f0f8fcdcc48abcafbaaaf5d87b` |
| Artifact | `13996776041`, ZIP SHA256 `014591de8ff30bbd518b5c6ada844436c214898b2a20703e0f3e0a9c772c9c05`, 40,592 bytes |
| Artifact contents | Four source/toolchain/test log files, all ZIP CRCs verified; no database roots |

No engine open, replay, automatic recovery, image-row result or physical HA claim
follows from this cohort. Separate image run `43333163664` remains independently
tracked. Earlier same-ref dispatch `43332872622` was canceled by workflow
concurrency before tests; it is **not tested**, not a runtime failure.
