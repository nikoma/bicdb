# Re-enable the pending-delete uniqueness regression

The recovered `conflict_upsert_scaling::row_deleted_in_transaction_is_not_a_conflict`
test was ignored with a comment describing23505 after deleting a row and reusing
its secondary-unique key for a new row ID inside the same transaction.

An explicit run of the original, unchanged ignored test on actual main
`a97165184c86319aa24f0881d56fe0abf8c6e5ae` passed1/1 in0.09s. The source-pinned
remote log is `/root/bicdb-durability-s3/pending-delete-main-repro.log`.
PostgreSQL16.13 independently verifies DELETE old/same, INSERT new/same ON CONFLICT
DO UPDATE, COMMIT returns exactly new/fresh; its exact statements and result are
stored alongside this file.

The ignore was stale. This change re-enables the original assertions in both
EmbeddedMemory and ServerPaged modes, without changing production behavior or
claiming a newly implemented storage repair. The final row must have the new
record identity and fresh value, with exactly one row for the reused key.
