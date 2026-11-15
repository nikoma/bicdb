# Visibility sourcef516: eight ordinary Core controls passed

The [normal Core job](https://github.com/nikoma/bicdb/actions/runs/43664842686/job/132661837044)
for candidate **1.0.518-beta / `f5162c32bb4db5ff68affe3c440ca9d9d4bd7969`**
executed all eight ordinary visibility controls successfully. The actual
checkout was `29d85b99369e60e5cfbccfe9e55fdeb37cc3f360`, tree
`71c15b07202c2c9f5f7d12d17c9e7c965807ec6f`.

That checkout used earlier main514. Main subsequently advanced to517 through
PR206. This is not a test of a composed current-main517 candidate, a later
private integration, a binary release or a deployment.

The [actual controls](../../crates/bicdb-core/src/db/commit_visibility_tests/visibility_wait_tests.rs)
use real same-process ServerPaged storage with fsync. Root checked the exact
eight original execution lines, source file hash, full raw log and gzip hash.

| Actual control scope | Classification | Limit |
| --- | --- | --- |
| Short gap closes within the original wait; successful commit is visible to a fresh thread | Passed | Local ordinary visibility only |
| Actual five-second higher-commit wait expires with the original uncertain outcome | Passed | Does not establish replicated ACK safety or physical durability |
| Committed outcome is observable while the original transaction owner remains retained | Passed | No owner reconstruction after restart |
| Sync indexing defers below its actual source floor | Passed | No global index/fleet qualification |
| Real derived-write uncertainty retains its original identity without ordinary replay | Passed | Cold WAL/job-intent handoff gap remains open |
| Actual Unknown-job format rename failure fences and leaves status unpublished | Passed | Current-reader behavior; not execution of an old binary or a power-loss test |
| An older Pending worker cohort cannot replay an unresolved job | Passed | Same-process schedule |
| A held real job claim permits another eligible job at the same limit of one | Passed | Does not qualify stalled storage or range relocation |
| Native-local RF3 visibility control | Not tested by this Core job | Native namespace was filtered into its separate running job |
| Current-main integration, whole-candidate acceptance and physical qualification | Inconclusive | Other running jobs and source changes require independent results |

The ordinary Core library reported 941 passed, zero failed and eight ignored,
with 348 native tests filtered. The complete job contains 139 target summaries;
their aggregate is not a unique-test count because it includes repeated-suite
execution. It must not be published as 1,834 distinct passing tests.

The complete original raw Core log is 2,831,327 bytes, SHA-256
`478a9f974c1fbd9fd29f060a05eb3b82eedcbfc78d88a9632069dd2a9158932b`.
The 367,417-byte gzip has SHA-256
`b83893889f3699c2cb1d43a01e72c048eca4240fbb7d1f9384a17d695eb43a1b`.
The source-pinned execution projection SHA-256 is
`3985cc6dbc46048769886e517aadbc0eb75edb02a12ae03ce62a28619f77b59e`;
root independent verification is
`f84d2aa4ee576ed3d7bcf2dbe259f7061bcb4212cb653efc31f4826a17726672`.

The earlier [ec4 failed acceptance](visibility-ec4-compile-failure.md) remains
failed and unchanged. Sourcef516's actual all-target and nondefault-consumer
compilation passed, so its eight lifetime corrections have compilation and
execution evidence. At this checkpoint native, SQL and remaining-workspace
jobs were still running. These passes do not close the cold memory-job gap,
qualify multi-range transactions, or demonstrate automatic four-to-five-node
expansion, RPO/RTO or sustained capacity gain.
