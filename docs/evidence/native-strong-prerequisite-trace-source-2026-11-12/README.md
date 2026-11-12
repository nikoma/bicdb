# Strong prerequisite diagnostic source

Base source: `735b3de5a1380deacaf8dd88702945f78df741b9`. This is test-only diagnostic source, with no build, execution or latency result. The original eedf four-case cohort remains **0 passed / 4 failed**: each failed at StrongR2 before the CurrentJoint fault/observer path. Existing RecoveryFenceScope-based tracing consequently had no context there. These diagnostics do not explain that failure or establish a fix.

`distribution_native_read/trace.rs` is compiled only under `cfg(test)`. It emits `NATIVE_STRONG_TRACE` only when `BICDB_TEST_EVIDENCE_ROOT` or `BICDB_TEST_STRONG_READ_TRACE` exists. The existing focused Core workflow already sets the first variable. No workflow, test body or acceptance assertion changes.

The copied context contains actual node, business ID, challenge, range, epoch and original deadline. It has no role, proof, token, DB/Store/Operation handle, authority constructor or recovery-scope conversion. Thread-local contexts and spans are !Send and restore on unwind. They are constructed inside the existing workers rather than captured across their thread boundary. Diagnostic writes ignore stderr write failures; diagnostics cannot return a protocol result.

The spans cover:

- Strong caller/owned worker, original request and metadata-capped worker allowances, worker queue time, coordinator reservation, initial service wait, data quorum, birth check, snapshot, final MQ/service wait and final currentness/outcome.
- Data collector scheduling: actual peer/attempt, waves/outstanding count, original round deadline, actual child deadline, declared request budget, job queue time, complete returned error and local-versus-remote attempt.
- Actual receiver dispatch/owned worker, service acquisition and validation; DB acquisition, catalogue/writer admission, schema, current lineage, factory admission, protected abort controls, catalogue observation, physical proof and catalogue recheck.
- Every delivered result, including the already-delivered majority drain, expired-attempt disposition, owned final-service acquisition, round ancestry/currentness and final result. Service-side revalidation and snapshot/finalizer boundaries also retain their actual context.

`read-attempt` spans the existing authenticated participant-transport call; it is not a socket-only timer. An absent receiver trace is not alone proof of network failure. Early returns drop spans and are correlated with the actual outer error; an `end` marker alone does not mean success. Worker end markers describe that worker's own context release, not universal listener/runtime drain. No extra strong Operation owner is retained. Events are emitted at finite phase/attempt boundaries, not inside latch polling loops.

`t_us`, `deadline_us` and `remaining_us` use a process-local monotonic anchor and PID. They are not synchronized cross-host clocks. Elapsed spans exclude their own begin-line write and include nested work/logging; test instrumentation has overhead. There is no physical performance or 8 GiB feasibility claim. Normal and CurrentJoint observations share these factual timing boundaries without widening either purpose's permissions.

Every insertion into an existing file is enclosed by `STRONG_TRACE_BEGIN/END`, with its executable items individually `cfg(test)`. Run the accompanying source check after committing:

```sh
python3 docs/evidence/native-strong-prerequisite-trace-source-2026-11-12/verify_source_parity.py --repo /root/bicdb --candidate TRACE_COMMIT
```

It removes marked insertions and compares all six existing files with the exact base after the same rustfmt normalization and blank-line removal. This proves source parity for those files; it is not typechecking. The new helper is excluded from non-test builds by its module gate. Standalone validation calls, token checks, retry limits, worker counts, queue/channel operations, lock acquisitions, native reads, snapshots and finalization expressions remain unchanged and in their original order.

The next check is the unchanged four-case cohort under the original four test threads and original budgets, after independent source review and coherent compilation. Retain its first failure and complete trace; do not substitute single-case isolation for the four-case gate. No main/version, operator, physical or deployed-state claim is made by this source checkpoint.
