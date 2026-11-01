# iDempiere runtime development evidence, 2026-11-01

The pinned iDempiere 13 Linux product uses its Oracle SQL provider with the
explicit PostgreSQL JDBC transport adaptation and connects to native BicDB's
`oracle-preview-v1`. The upstream product and seed remain external test inputs.
This extends the [verified seed import](../idempiere-seed-2026-11-01/README.md).

The application login form renders, and its exported GETDATE and CHARAT routines
install and execute on BicDB. These are two routine executions, not qualification
of all 62 stored routines or their business semantics. Authentication and full
business workflows have not passed. The initial login attempt exposed a
table-qualified column binding difference; the known seed credential matches
the imported data, while the application's lookup query initially returned no
user. After logical table-name binding was corrected, that lookup returned the
same one user as Oracle. A separate driver check confirms scalar VARCHAR
metadata for the application's same-named language column and table. These
query checks do not establish completed authentication or business workflows.

A sequence-allocation probe follows the application's query/update/commit
pattern with five concurrent clients, five requests each and at most three
attempts per request. Before the first-row locking fix, 20 of 25 requests
committed and five exhausted retries. After the fix, all 25 committed with 25
distinct IDs and no retry errors. Neither run produced duplicate committed IDs.
The successful run used no experimental first-lock-wait environment flag.
Separate PostgreSQL and Oracle-preview driver tests verify that an older waiter
waits for a younger owner and reads the newly committed value.

[`summary.json`](summary.json) records measured scope and source digests. The
application development database uses volatile storage and has test writes;
these results do not qualify durability, recovery, migration cutover, production
deployment, or the complete application. The original reconciled seed remains
separate. Foreign keys, remaining stored objects, release migrations, application
workflows and operational acceptance remain outstanding.
