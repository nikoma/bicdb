# READ COMMITTED upsert completion ledger

This ledger keeps remaining concurrency work explicit. Passing a narrow primary
arbiter fixture does not complete every PostgreSQL conflict path or application
acceptance.

| Path | State | Evidence and next requirement |
|---|---|---|
| Autocommit conflicts on the declared arbiter | Main #104 retries a fresh shared snapshot without blocking committers | Existing concurrent_upsert target and repeated recovered protocol workload; unrelated unique violations remain23505. |
| Unsigned READ COMMITTED primary arbiter with exact SQL row identity | Narrow repair #116, pending actual CI/merge | In both modes, rival commit gives RETURNING30 and rival rollback gives10; earlier write/savepoint survives and waiter commits. |
| Signed/native mutation authority | Open, explicitly excluded from #116 | Preserve INSERT-only fresh/DO NOTHING contracts and actual UPDATE denial. The signed invisible-winner fixture still returns10 then requires40001; committed winner20 survives. A later fix must carry a core-verified row version while authorizing and auditing the actual mutation action, consuming grant budgets exactly once. |
| Typed primary identity | Open, excluded from #116's row-ID shortcut | Add PostgreSQL schedules with semantically equal keys whose SQL identity requires typed arbitration, including char padding and exact numeric identities. Fix key-to-row reservation without losing typed uniqueness, null policy or RLS checks. |
| Pending DELETE followed by key reuse | Supported on main; stale ignore reactivation #117 pending CI/merge | Original unchanged ignored test passed on actual main a9716518. PostgreSQL16 reference agrees. Test-only #117 runs original assertions in both modes; complete scaling target4/4 passed atb0925f67. Owner: storage/SQL follow-up track until merged. No new runtime fix claim. |
| Secondary unique arbiters | Open | Existing explicit after-snapshot DO UPDATE fixture still reports40001. Add natural uncommitted-winner and key-move/delete schedules; coordinate transaction-lifetime unique-key reservations across relevant writers, cancellation/deadlock handling and savepoint rollback, and recheck the winner's actual row before evaluating expressions. Qualify memory/performance before broad merge. |

Keep protected, typed and secondary regressions separate from the ordinary
primary proof until their behavior is repaired. Preserve PostgreSQL reference
schedules, red BicDB evidence and application assertions. Reuse the ordinary
primary row reservation and checked snapshot-write primitive where justified;
refreshing unrelated relations' whole statement snapshot is not an equivalent
PostgreSQL fix. Record source revisions for every focused, broad and application
result; final completion requires merged main plus integrated qualification.
