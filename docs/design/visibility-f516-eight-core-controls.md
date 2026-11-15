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
execution evidence. SQL subsequently passed, including the actual generic
postcommit Sync dimension-error control (library 924/0/4; 282 target summaries
total 2682/0/4, without a unique-case claim). Root verified the exact test and
all summary lines against the complete original gzip/raw bytes, receipt
`4b0417a1eeb0c6dec8ca0cad08b24de766c3d387738f5e48c71e7cf2635ee539`.
These passes do not close the cold memory-job gap,
qualify multi-range transactions, or demonstrate automatic four-to-five-node
expansion, RPO/RTO or sustained capacity gain.

The [native job](https://github.com/nikoma/bicdb/actions/runs/43664842686/job/132661837027)
subsequently **failed: 332 passed, 15 failed, one ignored**, with 949 filtered
and 3,156.84 seconds reported. That elapsed time is not RTO or a performance
comparison. The remaining-workspace job was still live when this native
checkpoint was captured; the overall candidate has not passed.

The new native-local component reached actual follower application while the
other range's lower commit remained held: A WAL sequence 1, global prefix 0,
B origin returned `Ok(2)`, and the target's actual native point and local
snapshot were 2 with the full B value. It then failed because its required
public-finalizer probe returned `Disconnected`. This is useful partial
component evidence, not a passing native-local control or Strong-read proof.

Source inspection located a test diagnostic defect: the origin armed a
thread-local probe, but real quorum fan-out applies on follower workers. The
proposed test-only correction `e73b0ee16` selects a one-shot callback by actual
database open across those workers. It keeps the same actual transaction,
private preparation check, public finalizer, assertions and deadlines; the
callback receives data only. Independent review found no additional source
blocker in that correction; compilation and runtime execution remain untested.
The original failed result is unchanged.

Native raw log: 13,649,950 bytes, SHA-256
`bb32bf4308b381caf677717c172f6b46efb357a08d5f584c4f2bea3023305ff8`.
Gzip: 1,960,868 bytes, SHA-256
`0212ab3ec8aea49669ee84618867eb0c71ba2863ac875eb6db7949940c633be7`.
Root independently verified these bytes, the failed case line, actual partial
component evidence and five source pins, receipt
`5a27a7ff0e95ddf025c5836940b30ee7e26ea661736a43bfb6e0e6146fa26033`.

The final workspace job passed, ending November 15 at 03:37:43 UTC. Its
corrected schedule test actually passed at raw line 290112; the app-runtime
target reported 159 passed, zero failed and one ignored at line 290121.
This supplies runtime evidence for the test-only supervisor drain correction,
while the overall required run remains **failed because Native failed**.

Complete original capture is frozen: 88 pre-manifest files, eleven lossless
logs and eight original ZIPs/28 members, with zero missing captures or partials.
Manifest SHA-256:
`102acecd5348140ffe1e0dd8cc2308e124e69602b85c991bc96b0dce5d745241`.
Complete verification SHA-256:
`f0d7984046360ec61cab952a29b97eafe0df201cabb142e0110b6f1b8dcc659b`.
Root independently verified every pre-manifest file, all stored/raw log hashes,
artifact API digest/size/source identity, ZIP CRC and all 28 member hashes,
plus the exact schedule/target lines, receipt
`ba3fc24217c7d0c43146c01dc349f90356a7d9b0979536374669901f63bc89f4`.
The capture process exited normally and was not restarted.
