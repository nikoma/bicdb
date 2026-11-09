# READ COMMITTED upsert completion ledger

This ledger keeps remaining concurrency work explicit. Passing a narrow primary
arbiter fixture does not complete every PostgreSQL conflict path or application
acceptance.

| Path | State | Evidence and next requirement |
|---|---|---|
| Autocommit conflicts on the declared arbiter | Main #104 retries a fresh shared snapshot without blocking committers | Existing concurrent_upsert target and repeated recovered protocol workload; unrelated unique violations remain23505. |
| Unsigned READ COMMITTED primary arbiter with exact SQL row identity | Narrow repair #116, pending actual CI/merge | In both modes, rival commit gives RETURNING30 and rival rollback gives10; earlier write/savepoint survives and waiter commits. |
| Signed/native mutation authority | Open, explicitly excluded from #116 | Preserve INSERT-only fresh/DO NOTHING contracts and actual UPDATE denial. The signed invisible-winner fixture still returns10 then requires40001; committed winner20 survives. A later fix must carry a core-verified row version while authorizing and auditing the actual mutation action, consuming grant budgets exactly once. |
| Typed primary identity | Covered by isolated unsigned arbiter batch, not merged | Actual PostgreSQL16 reference and both-mode protocol cases for CHAR/NUMERIC pass in the isolated batch. Native key announcements and current index-to-row rechecks are under focused/broad qualification; no main completion claim. |
| Pending DELETE followed by key reuse | Supported on main; #117 merged e64b2e96 after all9 CI checks | Original unchanged ignored test passed on actual main a9716518. PostgreSQL16 reference agrees. Test-only #117 runs original assertions in both modes; complete scaling target4/4 passed atb0925f67. Merged and remote ancestry verified. No new runtime fix claim. |
| Secondary unique arbiters | Covered by isolated unsigned arbiter batch, not merged | Fourteen natural protocol cases pass including both NULL policies, DELETE and key move; strengthened original advisory-gated RC fixture commits its updated payload, and a direct native fixed-snapshot control retains40001. Cancellable/native reservation tests cover all shared blockers, deadlines, cycles, savepoint release, parsed/stored writers and index rename. Full source-current broad/performance/CI and independent review still required. |
| JSON semantic and non-executable arbiters | Open, explicitly excluded from the current index shortcut | Native physical keys and SQL canonical JSON keys need a proven bridge; an arbiter without an executable index retains its prior explicit retry path. No universal arbiter claim. |

Keep protected, typed and secondary regressions separate from the ordinary
primary proof until their behavior is repaired. Preserve PostgreSQL reference
schedules, red BicDB evidence and application assertions. Reuse the ordinary
primary row reservation and checked snapshot-write primitive where justified;
refreshing unrelated relations' whole statement snapshot is not an equivalent
PostgreSQL fix. Record source revisions for every focused, broad and application
result; final completion requires merged main plus integrated qualification.
