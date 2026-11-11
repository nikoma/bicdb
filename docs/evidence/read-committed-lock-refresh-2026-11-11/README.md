# Current-row observation after an earlier plain read

An explicit read-committed transaction first reads payload `10`. A peer commits
`20`. The transaction's next `UPDATE ... RETURNING` returns `21`, but an earlier
row observation can still cause COMMIT to reject that current update. A fresh
locking SELECT can leave the same stale dependency behind after statement
refresh. The tests-only control on main 1.0.462-beta reproduces the plain UPDATE
failure; native PostgreSQL 16 commits the expected result.

The correction records the committed version pinned by a successful locking
read or mutation recheck, including the fast path where the row is already below
the current applied watermark. It covers parsed and stored UPDATE paths and
DELETE. Fixed snapshots, existing buffered writes and ordinary native re-reads
retain their conservative dependencies. Grant validation still precedes the
lock and recheck.

The positive focused receipt is for source
`0acbe3c3306eb6654562fa288ae23c0f22715332`, 1.0.465-beta. All five commands passed:
version synchronization; core lock and isolation regressions; SQL statement
snapshots, primary-key schedules, native arbiter retry and version identity;
wire snapshots, read-only transactions and embedded HA; Oracle session defaults.
The SQL schedules cover both storage modes, INT and NUMERIC primary keys, plain
UPDATE and FOR UPDATE/NOWAIT/SKIP LOCKED, plus unique-key reuse after UPDATE and
DELETE. Ten independent native PostgreSQL schedules passed.

Both failed attempts are retained. The tests-only negative stops at the first
EmbeddedMemory INT/plain UPDATE COMMIT failure, so it does not establish a
failure in every matrix cell. The earlier lock-only implementation passes its
core checks but fails that same SQL schedule. Its private source version 464 was
retired before delivery; version 464 is reserved for the separate HA evidence
merge. An earlier non-primary-key SQL control passed without the fix and is not
claimed as a reproduction.

Full repository CI is recorded separately by GitHub for the delivery head.
These receipts establish focused correctness, not final iDempiere acceptance,
a new standard binary, a published release or a deployment. Original upstream
application assertions and the ten-second timeout are unchanged.
