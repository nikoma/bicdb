# Role cancellation: corrected 8ff result

Exact source `8ffae127a08ef4365a6f003195950d00a562f618`, `1.0.490-beta`:
[run 43334300815](https://github.com/nikoma/bicdb/actions/runs/43334300815),
Core `131586511694`, **1 passed, 0 failed, 964 filtered**, 0.01 s.
All-target compilation and the isolated no-default consumer passed in job
`131586477680`; the whole workflow completed successfully. Selector:
`recovery_role_observations_retain_local_and_transport_acquisition_cancellation`.

The two actual-store iterations now both completed: the sealed local-provider
mint and the component transport wrapping real quorum evidence. Each witness
was live before its original parent was canceled, then required `QueryCanceled`
before its unchanged deadline. The transport is a component CFT seam, not an
actual TLS attack or native serving test. The genuine TLS C1 step-role-parent
control is separately included in the [five-case 8e image result](native-closed-c1-image-8e-result.md).

The only test correction supplied recipient n1 as the authenticated acquisition
caller while retaining initiator n2 in the unchanged request. Production code
is unchanged from 8e; the provider prefix before its test module is byte-identical
(SHA256 `d1dd73c50f702d0d122865df5b0fd37620fba86d750e658a8a484ebbcd3ca2e6`).
The [original failed 8e run](native-recovery-role-cancellation-8e-first-result.md)
remains **0/1 failed**, with its transport cancellation assertions not reached.
The five image tests were not rerun for this test-only caller correction; their
pass stays attributed to exact 8e.

Private retained raw/index directory: `/tmp/bicdb-native-foundations-ci/`.

| Item | Retained identity |
| --- | --- |
| Core raw | `8ff-c1-role-job131586511694.log.gz`, SHA256 `f939988380b64dd7bc0401b95e95f243b889503c8f55c1838d2c6be439764bc5` |
| Compile raw | `8ff-c1-role-compile-job131586477680.log.gz`, SHA256 `234401fcaae2edb8c5926e026fa2081f79e7f50f7c2d02fb47d743fe0015855b` |
| Artifact | `13995264850`, ZIP SHA256 `2abd16f7f9451926f0fabaddb7989f059fd0e28b493c412b2f5490d40f86998a`, 40,354 bytes |
| Inventory | Four source/toolchain/test-log files; CRCs and exact source checked; no database roots |

No engine open/replay or production edit was performed during capture. This
focused cancellation result does not qualify Registered receiver admission,
fifth-node data population, NEW serving, physical HA or fault self-healing;
the final composed prototype still requires its relevant broad regressions.
