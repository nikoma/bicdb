# Original-writer policy baselines: later terminal compile checks

This is an additive follow-up to the two frozen original-writer diagnostic
records. Their references to checks still pending at Core-record freeze remain
historically unchanged. No test was rerun, canceled, ignored or relabeled.

| Exact source | Run | Runtime job/result | Separate compile job/result |
| --- | --- | --- | --- |
| `11f5b4b222f4bd84d1f3870dbd59da11ee8cd17c` | 43314169894 | 131517781459: **0 pass / 1 fail**, 2.12s; diagnostic snapshot before image | 131517719477: **PASS** all-target and isolated consumer checks |
| `68be040ebb00a4c606d519697f913e310c496812` | 43315453055 | 131522237759: **0 pass / 1 fail**, 2.31s; actual image-clear writer-policy refusal | 131522179911: **PASS** all-target and isolated consumer checks |

Neither compile job executes runtime tests. Counts are separate executions and
are not summed into a successful cohort. The source-68 Core compilation also
succeeded before its selected test ran; that did not make the test pass.

Source-11f full compile raw log/identity are preserved in the adjacent
`native-original-writer-image-policy-refusal-2026-11-12` record under their own
source/job names. Its raw SHA-256 is
`24919358848fcd3bd81576adc70b0d0951539a74d555106b9bc9a6358a53fe56`.
This follow-up adds the full terminal source-68 compile log, 2,505,885 raw bytes,
SHA-256 `9890b3a9065e76e7c8a5c9272cc1d426dc4a844f90b4875f7484bc277eaa613b`.

The corresponding runtime logs, exact before-image report and independent
private-custody verification receipts remain in the earlier folders. No engine
was opened/replayed. These checks do not close the missing DATA-policy producer,
prove a global joint decision or NEW serving, qualify automatic/remote 4→5,
8 GiB capacity or physical fault acceptance, or establish a deployment.
