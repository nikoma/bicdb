# Bounded-stack boolean evaluation for row-security predicates

Qualification base: `51ddb00a6016c3e25b2b3d7489ce43b0d264ff7d`, including
the latest fetched timezone-status and Oracle BLOB reconciliation changes.
Before integration, upstream advanced to
`8149bac3e35a5348e5960791122dda34dc21a414`. That update adds BLOB restart
validation and documentation; `git diff --exit-code` confirms no changes under
`crates`, Cargo manifests/lockfile, toolchain or Cargo configuration. It is
included in the integration base.

The actual hosted EHR program action successfully commits its program and audit
record, then the generated model getter crashes the native server. A current-
thread native backtrace identifies repeated `eval_row_truth` frames while
evaluating the program's expanded role-list RLS predicate. A reduced policy test
reproduces the stack overflow on a 2 MiB thread without the EHR or its data.

Boolean AND, OR, NOT and parentheses now use an explicit traversal stack in both
row and slot-row predicate evaluators. Leaf evaluation retains the existing
privilege checks and expression behavior. SQL three-valued logic and lazy,
left-to-right branch evaluation are preserved. Policies, grants, identity
validation and thread stack limits are unchanged.

`negative.log` records the new regression aborting with stack overflow on the
unmodified evaluator at `c837bd0`. That evaluator is byte-identical at the
qualification base. The positive regression checks visible/public rows, tenant
switching, denied/missing roles, a missing tenant, NULL truth values and
short-circuit avoidance of division by zero. Task core dumps are disabled.

- `positive.log`: all 63 focused SQL tests pass, including the new policy-depth
  regression, real Carrier query shapes, JSON SRFs, row locks and type contracts.
- `unit.log`: all 676 SQL unit tests pass; one existing benchmark is ignored.
- `protocol.log`: all 181 wire-protocol tests pass, including the latest
  timezone-status regression.
- Targeted `rustfmt --check` and `git diff --check` pass.

An initial positive build was interrupted by the task's 8 GiB disk-headroom
guard (exit 99); it was not a passing validation. Cleanup preserves other
processes and keeps reusable task artifacts in the owned RAM cache.
The subsequent cleanup removed 2,630,075,740 orphaned cache bytes and moved
2,631,632,100 reusable artifact bytes from disk into that cache, preserving
modification times. The cache then contained 19,270,823,592 bytes.

Real hosted program/companion qualification, remaining goal-workflow checks and
production cutover remain open. Production data and services were not changed.
