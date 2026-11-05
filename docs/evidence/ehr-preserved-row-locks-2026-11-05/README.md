# Preserved-table row locks beside outer and lateral joins

Base: `ca96174088ca527eaa2ef2acb8757bad219192b0` (fetched November 5).

The EHR reflection workflow selects check-ins beside a LEFT JOIN LATERAL of
their latest response, then uses `FOR UPDATE OF pci`. The previous blanket
outer-join refusal rejected that valid lock target, including when no check-in
matched. The executor now tracks which inputs can be null-extended and permits
explicit locks on preserved base tables. Locking either nullable side remains
an unsupported operation, including an earlier input made nullable by a later
RIGHT/FULL join. Existing privilege, RLS, key extraction and lock acquisition
paths remain in use.

The new integration tests prove that the preserved check-ins really contend
with another transaction, the response table stays independently lockable,
and rollback releases the check-in locks. They also cover RIGHT JOIN and
nullable-side refusals. Parenthesized nested joins and other previously
unsupported lock targets remain unsupported.

Validation on the task-owned build host, bounded to two compiler jobs with
incremental compilation disabled and active RAM/disk monitoring:

- `negative.log`: the two new tests fail on unchanged base source with the
  original inner-join-only error (exit 101).
- `positive.log`: all 73 tests pass across `locking_projection`,
  `select_row_locks`, `carrier_hub_compat`, `type_consistency`,
  `routine_assignment_types`, `routine_privilege_lists`, and
  `routine_owner_execution_identity`.
- `protocol.log`: all 179 pgwire protocol tests pass. The initial protocol
  build was interrupted by the 8 GiB disk headroom guard (exit 99); this log
  records the successful resumed run. Reusable task-owned artifacts were
  preserved in RAM to restore headroom. Test executables use stripped debug
  information to bound disk growth.
- `rustfmt --check` and `git diff --check` pass for the source and regression.

This is an engine compatibility fix, not EHR acceptance or production cutover.
Program enrollment, duplicate goals, and signed-write retry qualification
remain open. No production data or services were changed.
