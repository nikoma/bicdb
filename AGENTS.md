# Repository agent instructions

## Required completion: merge and push to main

The repository owner's standing instruction is to merge completed work into `main`
and push it to GitHub. This is already authorized; do not ask for merge permission
again. A commit, feature branch, draft PR, or built binary alone is not completion.

- Run the checks relevant to the change, resolve conflicts, merge your work into
  `main`, and push. Verify GitHub `main` contains the resulting commit.
- Continue through the merge without waiting for another user reminder. Temporary
  branches are working tools, not the final delivery location.
- Delete your merged temporary branches after verifying remote `main`. Preserve
  other agents' unmerged work and dirty worktrees; use an isolated worktree when needed.
- Report the remote `main` commit, validation results, and binary location when relevant.
  Merged source, a built binary, a published release, and a verified deployment are
  separate states; describe each accurately.
- Do not disable repository protections or hide failed tests to obtain a merge.
  Fix actionable failures. If an external restriction prevents merging, identify
  the exact blocker and remaining action; do not describe the task as complete.
  Report unavailable CI separately from tests that actually ran and failed.


## Bump the source version on every merge

The owner's standing instruction is to bump the BicDB source version with every
merge into `main`, so the newest source is unambiguous. This applies to code,
tests, documentation and evidence merges.

- Include the bump in the change being merged; do not leave versioning to a later
  unmerged branch. Use `python3 scripts/bump-version.py NEXT_VERSION`, then run
  `python3 scripts/bump-version.py --check` and the relevant version-identity checks.
- Keep crate package versions, internal path-dependency requirements, tracked
  lockfiles and the current-source compatibility document synchronized. Preserve
  historical evidence and published-release download links at their actual versions.
- Coordinate merge order with other agents. Immediately before merging, verify
  that the candidate version is the next unused patch version above remote `main`.
  Do not reuse a parallel branch's version or overwrite a newer version.
- Validate the versioned final head, push, and verify remote `main` contains both
  the change and its new version. Report version and commit together. A source
  version bump does not imply a binary, release or deployment was produced.


## Batch fixes before broad validation

Group related fixes into a coherent implementation batch before running broad
test suites. While the batch is evolving, use focused regressions and relevant
compile checks to get fast feedback on each semantic rule. Use failures from
running application tests to identify related fixes rather than restarting the
whole validation cycle after every small edit.

- Run the required broad suites after the batch stabilizes, then resolve any
  failures before merging. Batching does not weaken the completion gates above.
- Reuse completed checks while their relevant code and dependencies remain
  unchanged. Repeat broad checks when new changes invalidate their coverage,
  failures appear, or an unresolved concern requires them.
- Record the source revision and scope of each result. Distinguish focused
  checks, broad regression suites and application acceptance; do not report an
  earlier revision's passing suite as a test of changed code.
- Keep PostgreSQL and Oracle regressions independent where behavior differs.
  Preserve failing evidence and original application assertions.
- Let useful application runs finish collecting evidence. Coordinate heavy
  builds with those runs and the memory policy below instead of repeatedly
  interrupting them for isolated validation cycles.

## Keep build artifacts bounded

Clean up artifacts produced by your work after validation batches. Remove
superseded test executables, interrupted compiler objects, and orphaned
task-owned caches. Preserve the current runnable binary, files used by active
processes, required compatibility-test binaries, and other worktrees' outputs.
Retain reusable dependency artifacts to keep subsequent checks efficient. Use
bounded build concurrency and disable incremental compilation for large engine
builds when it would accumulate obsolete generations. Report reclaimed space
and remaining build-cache size when cleanup is substantial.

## Monitor memory during builds

Watch available RAM, swap and memory pressure throughout long builds, rather
than checking only at startup. Limit concurrent memory-heavy compilation and
leave headroom for the other services on shared hosts. Use an active monitor
with a host-appropriate minimum headroom; stop only your own build process group
if memory becomes scarce, then resume validation when sufficient memory returns.
Preserve other agents' and services' processes. Keep the goal active while
handling resource pressure, and report an interrupted validation separately
from a completed test run.
