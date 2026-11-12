# Bounded same-open joint handoff: exact nine-test cohort

Classification: **passed focused component cohort**, not physical or production
qualification. Source `88082c179da81cb7f0a2af0c088a3d5c85227012`,
[run 43241594839](https://github.com/nikoma/bicdb/actions/runs/43241594839),
Core job 131263181223: **nine passed, zero failed, zero ignored**, 890 filtered,
75.50 seconds. All-target and isolated no-default consumer compilation passed.
The cohort retains four test threads and original four-second operation
budgets; neither was widened to get this result.

The typed full terminal reaches three final releases, target ACK2 and full
Strong R2. Other controls cover actual post-fsync cancellation, original-caller
expiry, exact reconciliation without rewrite, actual foreign writer replacement,
and original image/survivor release refusal. The exact selected test names are
in `summary.json` and the unchanged four-file artifact. Initial metadata
election is a declared fixture precondition; later metadata/data quorums and
native writes execute on this one CI host. No remote operator, automatic
controller relocation, independent VM, cold restart or throughput qualification
follows.

## Preserve the preceding failed cohort

Same nine-test selection at `eb5b454a578f77309900e2a248fc0b779391333e`,
[run 43233276416](https://github.com/nikoma/bicdb/actions/runs/43233276416),
job 131233899683: **five passed, four failed**, 890 filtered, 77.72 seconds;
compilation passed. Duplicate actual lineage reads and preparation rereads
exhausted original operation budgets. The original-image replacement control
expired during its immutable snapshot before replacement, so that intended cut
was not exercised in that failed case. The correction returns the same already
validated lineage from its owned scope, aggregates original lifetimes without
a duplicate three-replica sweep, and takes the immutable test snapshot before
receipt creation. Complete final foreign checks and actual local mutation
checks remain in place. The earlier 5/4 result remains failed; it is not hidden
by the later pass. Original raw-log SHA-256:
`f3779145fb319f664bed47f6d27bfc52ef5e5222db53b1bffde137d9f86ba1b0`.

The original failed artifact13950014858 stays in restricted custody because it
contains generated database key material: 926,055 bytes, SHA-256
`8b213bb5f75d727a8bba94b86568c17b58a0612d15b31f1eef7f35cdfd8c17be`.
Four manifest-listed cuts verified exact file sets, hashes and lengths; each
contains 75 files. Owners were alive; these are not atomic power-loss images
or independent engine replays.

## Immutable passing artifact

`original-artifact.zip` is unchanged GitHub artifact13955621769, 75,197 bytes,
SHA-256 `9f4babc7f3e806b8872c68d94366d9a5daf9e93d3c237f80cc1310cd98ced384`.
It contains only source revision, compiler identity, selected test list and test
output. Per-file hashes and complete raw-job-log hash are in `summary.json`.
The exact later post-joint recovery acceptance remains separately failed;
passing this cohort does not establish that recovery gate.
