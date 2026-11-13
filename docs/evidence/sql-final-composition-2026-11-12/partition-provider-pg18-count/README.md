# Original partition diagnostic reaches a PostgreSQL18 provider limitation

The unchanged original 95-test run on private source
`82a8311156e2293a92e7cf9c1ffd70d1df92d57a` finished with 94 passes, one failure,
zero errors and zero skipped. The original assertions, bytecode, provider,
verified baseline and waits were preserved. Both owned processes stopped and the
baseline remained unchanged. The earlier compaction and certificate-owner
failures were passed; index migration now receives SQLSTATE `2BP01` when it
attempts to drop a constraint-backed index. This is not a passing application run.

The stock PostgreSQL provider's `migrateDBContrainsts` first counts every
`pg_constraint` entry on the newly created partitioned parent and returns early
when that count is nonzero. PostgreSQL18 includes not-null constraints in that
catalog; PostgreSQL16 represents them separately. See the primary
[PG18 catalog contract](https://www.postgresql.org/docs/18/catalog-pg-constraint.html)
and [PG16 catalog contract](https://www.postgresql.org/docs/16/catalog-pg-constraint.html).
The provider therefore skips its primary/unique constraint migration on a fresh
PG18 parent that only has NOT NULL columns. Its following index migration then
tries to drop the child's still-required primary-key index.

Fresh, isolated cached-image controls independently returned count zero on
native PostgreSQL16.13 and count two, both type `n`, on native PostgreSQL18.4 for
the same two-column partitioned parent. Excluding only `n` returns zero on both.
The frozen 82a BicDB debug server advertises 18.4 and returns the same two `n`
entries and counts two/zero. The earlier untouched Native16 MTable class passed
all38 tests; the qualified migrated-fixture Native18 class passed37 and failed
the same constraint-backed-index drop. That older Native18 fixture removed one
invalid optional operator commutator for restore and is labelled separately in
`metadata-regclass/native-pg18-migrated-fixture-control`; the new tiny catalog
controls require no fixture migration. Together these identify a stock-provider
PostgreSQL18 compatibility issue, rather than permission to weaken index guards.

`proposed-provider-pg18.patch` is a minimal reviewable provider correction that
excludes `contype = n` only from its already-migrated predicate. It has not been
applied to the original source/provider, compiled, or qualified with the full
application. It is not a replacement for stock-provider acceptance, and no
upstream communication or PR was sent. Original failing evidence remains intact.

The tiny BicDB control also reveals an independent naming mismatch: automatically
generated not-null constraint names contain the encoded physical table identity
in the qualified `adempiere` namespace, while native18 names use the visible
relation name. Naming and identity stability require their own fix and controls;
matching the provider COUNT does not certify the complete catalog surface.

Two first harness attempts failed before collecting their intended comparisons:
the first SSH command stripped SQL quotes, and the first native16 container wrote
through its automatic anonymous data volume to the full local filesystem. Both
owned processes/containers were stopped. The corrected probes use stdin for SQL
script transmission and an explicit PGDATA inside bounded tmpfs; both native
containers and their anonymous volumes were removed. Those setup failures are
not SQL failures or passing tests. No protected database was connected or changed.

The cleanup receipt separately records126 superseded old490 Core test executables
removed after the current492 Core suite passed:5,681,971,200 allocated bytes
reclaimed. Remaining cache sizes then were493,621,128,935 bytes on the standard
lane and86,058,431,702 bytes on the metadata lane. Current tests, all dependencies,
frozen/current binaries, compatibility artifacts and other worktrees were preserved.
Final combined broad/crash/VM/full-application, Native portable-origin and HA gates
remain open; experimental production remains off and no cloud spend occurred.
