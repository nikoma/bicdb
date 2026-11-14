# Classification and smallest next tests

| Finding | Classification | Evidence and remaining limit |
| --- | --- | --- |
| Exact four original assertions | Passed | Four full-name selection, four runtime starts, four terminal successes and unchanged original assertions |
| Workspace and consumer compilation | Passed | Separate terminal workspace job; no runtime/application qualification |
| Three lifecycle APP returns and Strong values | Passed | Full values and hashes match for modes 0–2; six same-root opens for modes 0–1 |
| Complete diagnostic census | Inconclusive | 18 busy drops, no background join, global completeness explicitly false |
| Original c6 failure causes | Inconclusive | Failed original requests lacked linked per-voter/error/mode information; separate focused diagnostic passed |
| Runtime resource explanation | Not tested | Guard sampled build/check/list only; original job has no RAM or PSI measurements |
| Broad required native-suite qualification | Not tested by this packet | Four focused cases exclude whole-suite history; any exact repeat has separate evidence |
| Physical failover, scale-out, throughput, production admission | Not tested | No VM fault, real relocation or stable capacity experiment |

The original reproducible issue is the failed required serial suite at
`c6a081a6026f4efd00d80f392c94b67f2f2f4e12`, run 43529104485,
job 132242099377. Its command is:

```sh
cargo test --config 'profile.dev.package."*".opt-level=2' -p bicdb-core --lib --locked -- \
  distribution_native_host:: range_image_native::rf3_fixture:: --test-threads=1
```

The original normal-damage case failed before injection; both hot cases
failed during initial CONFIG preparation; the lifecycle case failed on
the current mode's APP append, whose effect was uncertain. Its mode and
earlier successful modes were not measured. Do not replace those facts
with this diagnostic's successful counts or infer rollback from timeout.

The smallest controlled whole-history discriminator is one separately
retained exact-source serial suite with the original selection, order,
budgets and assertions. It can test whether the failure reproduces; a pass
does not identify a fix or cause. If a new failure needs investigation,
retain bounded actual per-voter errors, acquisition IDs, request/mode
boundaries and terminal outcomes for that failing history. Do not infer a
cause from unrelated broken pipes, cancellation after successful quorum,
or build memory samples. Do not add retries, readiness bypasses, longer
budgets or skipped assertions to obtain a pass.

An APP error's native effect still requires separate retained outcome
evidence. This packet has no raw SDK frame journal, native database
reconciliation or external durable ledger, so it cannot settle an
uncertain write in the original failed run. Follow-up runtime fixes must
remain separate from these immutable observations.
