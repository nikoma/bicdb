# Foreign-key reference reservations

A child write reserves the typed parent key in shared mode before checking the
current committed parent state with its own transaction's writes overlaid.
Independent transactions can insert or change children referencing the same
parent. The reservation is separate from ordinary row write locks.

Deleting a parent or changing its referenced key reserves that same key
exclusively before checking children or applying referential actions. A key
change therefore conflicts with another transaction's outstanding child
reference. Updating an unrelated parent column does not acquire an exclusive
reference reservation. Unique and exclusion constraint reservations remain
exclusive; sharing a reference does not permit duplicate unique keys.

Plain primary-key updates on untriggered parent tables now use the parent-key
validation path too. Deferred child follow-up updates, immediate reference
rejection, and final-key-set NO ACTION checks are covered by the regression
cases. UPDATE FROM/RETURNING, trigger-bearing parent key moves and cascades that
must change a child's physical primary key still need separate key-move work.

Both PostgreSQL and Oracle preview profiles use this integrity rule. The key
identity includes the parent relation and ordered referenced columns, with
values normalized using the parent's declared types. Primary-key and unique-key
references use the same reservation modes, including composite references.
NULL-containing MATCH SIMPLE references retain their separate exemption.

Reservations last through transaction completion, including deferred checks.
Rollback, abandoned-transaction cleanup and rollback to a savepoint release the
applicable acquisitions. Shared and exclusive acquisitions are tracked
separately: rolling back an upgrade restores a shared reservation acquired
before the savepoint rather than discarding its protection. Repeated acquisition
of a mode does not add duplicate rollback events.

A conflicting reservation currently returns a retryable transaction conflict
(SQLSTATE `40001`). This is a bounded integrity contract, not complete Oracle or
PostgreSQL lock-wait equivalence. Waiting for uncommitted parent creation,
foreign-key deadlock classification, lock diagnostics, fairness and distributed
writer coordination need separate qualification. Applications must retain their
transaction retry policy; a conflict must not be treated as a successful write.

The implementation's regression coverage is defined in
[core reservation tests](../crates/bicdb-core/src/db/integrity_reservations.rs)
and [independent profile cases](../crates/bicdb-sql/tests/shared_foreign_key_references.rs).
They exercise concurrent shared readers versus an exclusive writer, transaction
completion, savepoint upgrade rollback, immediate/deferred child insertion,
unique-key parents, non-key parent updates and retained parent-mutation
protection. These test definitions require execution results before acceptance;
their existence alone does not qualify an application or production workload.

The [2026-11-05 qualification checkpoint](evidence/oracle-foreign-key-reservations-2026-11-05/README.md)
records executed native, protocol and independent Oracle comparisons, including
candidate identity and remaining boundaries. Full application and production
acceptance remain separate gates.
