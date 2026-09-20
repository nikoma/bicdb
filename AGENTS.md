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

