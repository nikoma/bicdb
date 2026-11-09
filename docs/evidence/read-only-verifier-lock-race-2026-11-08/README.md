# Read-only verifier lock race

Review of PR #85 found a race between the read-only lock probe and a same-process
store open. The probe checked the ownership registry, released its guard, then
opened the lock file. A concurrent writer could acquire ownership in between.
POSIX closes release all the process's record locks on an inode, so closing the
probe descriptor could silently remove the writer's cross-process exclusion.

The probe now holds the ownership-registry guard until its descriptor closes.
The regression concurrently probes while repeatedly opening a store, and forks
a child to check that another process cannot acquire the writer's lock.

Validation on s3ny at source `9ee0a5b3` (8 GiB cap, two build jobs, incremental
off, active 10 GiB host-headroom monitor):

- Complete `bicdb-page` suite: 433 passed, 0 failed, 3 preexisting diagnostic
  tests ignored. The 15 verifier tests include store hashes before/after reads.
- `bicdb-cli --test verify_store_cli`: 2 passed, 0 failed.
- Formatting passed.
- Negative control: restoring the old check-then-open probe makes the new
  cross-process exclusion regression fail (exit 101). Restoring the fix makes
  it pass (exit 0). See `negative-control.txt`.

No production store was inspected or modified. These results validate the
verifier and lock race only. The verifier remains a page-level diagnostic; it
cannot establish complete SQL secondary-index consistency or independent
acknowledged-commit reconciliation.
