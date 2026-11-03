# Character migration verification connections

The persistent-disk trial completes all **910 affected tables** and **7,150
character declarations**. Fresh-connection verification and a separate server
reopen preserve the complete multiset fingerprints of **915 tables and 185,537
rows**. All 7,150 logical declarations validate after reopening.

The trial exposed a verification-harness lifecycle error. The harness read its
baseline on one connection, then ran the production migration CLI, which opens
its own connection. The unused baseline connection exceeded the server's
five-minute idle timeout. Reusing it for the after-inventory check produced a
client disconnect, although the migration connection had completed its work.

Retained server output shows the control connection closing 299 seconds after
the migration connection opened. The latter continued for more than two hours;
the completion metrics record 18,083 queries and zero failed queries. Verification
now closes the unused control connection and reconnects after migration. The
server timeout policy is unchanged. The already-complete migration journal was
reused for verification; the DDL was not repeated.

The [validation report](validation.json) records equal before, after and reopened
inventory hashes, verified declarations, the candidate hash and resource guards.
The target snapshot has 11 pre-existing table-count differences from the retained
Oracle export. Those rows are preserved: these fingerprints compare the actual
snapshot before and after reconciliation, not a claim of current source/target
row equality.

This supplements the earlier
[full-copy qualification](../oracle-character-full-copy-2026-11-03/README.md).
That checkpoint's retained failure remains part of the history. Its RAM-backed
trial used the same idle-control pattern but did not retain server output;
the repeated persistent trial supplies the connection-lifecycle diagnosis.

This qualifies exported character declarations and lossless reconciliation on a
development copy. Host-crash durability, full schema constraints, application
execution and security qualification are separate checks. The migration candidate
is the previously qualified character implementation, not a deployment of the
later routine-security patch.
