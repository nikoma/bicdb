# Paged application hosting, bootstrap and foreground write availability

This work supports the WalkNorth EHR HA migration. It does not establish a
production HA deployment. Clinical records and original keys stayed on the
production host; all repository test fixtures contain synthetic data.

The previous host always selected the memory engine. Four real application
regressions failed at paged open/recovery/failover. Pgwire also selected the old
engine before installing a staged standby snapshot. A separate partial fix
(reading the mode without first installing the snapshot) is tested independently.
The storage-mode and native replica-identity fences remain enforced.

Once mode selection worked, application migrations exposed a second failure:
creating a new table's primary-key index within BEGIN entered the restartable
paged index builder, which correctly refuses pending transactions. The new path
initializes only an empty B-tree for a table created by this SQL transaction,
before any pending writes to that table. Populated rebuilds retain their fence.
The regression checks commit, uniqueness failure, explicit rollback, absence of
orphan index definitions, refusal on a populated collection and cold reopen.

Three protocol regressions exercise async queries, synchronous queries and COPY.
The first COPY fixture used unsupported pg_sleep inside a SQL trigger and
failed before exercising idle accounting; that failure is not causal evidence.
The corrected trigger uses clock_timestamp in a bounded, cancelable SQL loop.
Execution/reply time must not consume the subsequent idle allowance. The actual
old CLI completed pg_sleep(1.5) with a one-second idle budget, then disconnected
before a follow-up after 400 ms. Idle policy and query deadlines are unchanged.

The bounded clinical transfer reconciled 60 tables before a foreground writer
waited out its 30-second lock budget. Private operational logs recorded a search
fold taking 213,114 ms for 14,901 terms/16,943 blocks while holding the global
read guard. No live database changed. All 289 target table guards were restored,
the failed attempt was retained, and the target remains unadmitted.

Automatic search folding now uses at most 16 whole terms per guard acquisition
and releases the global guard between batches. Whole-store synchronous
checkpoint/vacuum/sweep is retained for explicit full folds; automatic batches
leave reclamation to the separate governed maintenance workers. A single term
remains atomic: this is not a hard byte/time bound for an unusually large posting
list. The integration test covers both existing numeric blocks and string-key
blocks/tails, interleaved writes/deletes and reopen. The worker regression
observes partial real folding and requires an actual foreground write to proceed
before the entire fold finishes.

The integration also exposed a pre-existing numeric-segment layering bug:
folding away the last posting removed every paged override and allowed the old
immutable segment to answer again. Empty terms now retain a valid zero-row
numeric block and zero-frequency dictionary row in the same paged transaction.
Existing readers already understand these encodings; exact, prefix and ranked
paths stay on the authoritative paged layer. Reinsertions restore term counts
correctly. The causal control reproduces the deleted match at batch 20; the
corrected test covers disappearance, prefix lookup, restart and reinsertion.

The coherent source batch is qualified at `82a1ad0`, including the latest
literal-session-setting changes from main. Its 1,407 source inputs were checked
against the manifest before and after the batch. `cargo fmt --all --check` passes.
The relevant broad checks pass:

- 761 SQL library tests (four existing ignored cases).
- 452 core library tests (three explicit benchmarks ignored; a nested child
  probe is included, not counted again).
- 40 paged/index/transaction integration tests across five targets.
- 47 application-host tests across seven targets; its one root/UID isolation
  case is then run explicitly and passes, giving 48 executed checks.
- 187 protocol and 140 pgwire library tests.

The new regressions also fail semantically under the retained code controls;
compilation errors are never counted as regression proof. The source mode
controls fail four/one tests, the fresh-table index control fails one, whole-fold
and deleted-segment controls each fail one, the old fold worker fails the
foreground-write test, and the final idle controls fail all three tests before
the fix and pass all three afterward. The final COPY fixture asserts actual
work exceeds 1.4 seconds, below its five-second query timeout and above its
one-second idle limit.

`qualification/v6` deliberately retains an intermediate overall failure caused
by the unsupported initial COPY trigger. It includes passing mode/fold checks,
but is not a passing complete batch. `qualification/v7` contains the corrected
idle proof and the complete passing broad batch. Earlier fresh-table failures
are retained separately. See `summary.json` for exact counts and hashes.

The source gate does not qualify a frozen executable or the EHR deployment.
Retained intermediate failures are not passes.
Source merge, frozen runtime, EHR acceptance and physical HA activation are
separate gates; DigitalOcean currently refuses the two additional hosts because
the production account is locked.
