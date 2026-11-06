# Shared foreign-key references and bounded COPY admission

Independent child transactions can reference the same parent key concurrently.
Parent deletion and referenced-key mutation retain exclusive reservations;
non-key parent updates preserve concurrent references. Deferred checks resolve
one final child record by identity rather than rescanning its entire table for
every hook. Plain untriggered parent primary-key changes apply referential
checks and actions within the statement rollback boundary.

The [validation record](validation.json) records the exact candidate source and
binary, actual per-target test counts and log hashes. The independent
[Oracle comparison](reference-comparison.json) matches all seven cases, including
immediate/deferred concurrent children, a unique-key parent, rollback to a
savepoint, deferred parent/child key movement, rejection of a referenced key
change and a final-key-set swap. Both sides remove their owned tables; the
native database exits cleanly and is removed.

COPY now admits each insertion batch before acquiring database write access.
Metadata discovery and transaction control retain their own admission. COPY
therefore does not hold one query slot while waiting for another. All 19
COPY-filtered protocol tests pass on one CPU, including an explicit one-slot
regression with a short overload deadline, byte-exact results and error-frame
checks. The full protocol suite runs with normal host CPU affinity; compilation
remains bounded separately. The concurrent-reader fixture explicitly sets its
read-admission capacity, independent of host CPU count.

The prior pinned protocol attempt was interrupted after observed failures;
it was not a complete passing run. Its failure names remain in the record.
Passing COPY and normal-affinity tests does not qualify general transaction
progress when all query slots are occupied by lock waiters. Core API partial
write truncation and its integrity-reservation retention also need separate
qualification. These limitations remain distinct from the exercised SQL
statement/savepoint rollback paths.

The [reservation contract](../../foreign-key-reservations.md) states supported
key moves and remaining wait, error, trigger and distributed-ownership bounds.
These engine and source-reference checks do not qualify the entire application,
production operation, a deployed upgrade or the full compatibility programme.
