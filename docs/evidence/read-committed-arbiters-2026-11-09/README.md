# Remaining READ COMMITTED arbiter schedules

These are remaining correctness gaps, not passing application acceptance.
The protocol fixture atd772a6aacf450d52ef6b7d0db38e75079217f90e ran on
s3ny in both EmbeddedMemory and ServerPaged. The two NULLS DISTINCT controls
passed; twelve real runtime cases failed. Secondary-key writers completed
before their pending winner, typed primary rows returned10 instead of30,
and DELETE/key-move winners incorrectly returned the old identity/value.
Raw result: `/root/bicdb-durability-s3/read-committed-remaining-valid-repro.log`.
An earlier fixture version used unsupported NULLS NOT DISTINCT syntax and
its two parser precondition failures are retained separately, never counted
as runtime failures.

The independent PostgreSQL16.13 reference ran all seven case factories,
including inherited Python case overrides. The observer established real
Lock/transactionid waits before committing every waits=true winner. NULLS
DISTINCT finished before winner commit. RETURNING and complete final rows
matched the expected secondary, CHAR, NUMERIC, both NULL policies, DELETE,
and key-move cases. Exact statements/results are retained alongside this file.
Reference fixture SHA256:
`3dccb1b879cf3d50a72abfab3a06daaa6abe6e9e79c2fc8c76cf4222657e0aa8`.

The evolving repair is isolated from ordinary-primary PR116. Its cancellable
key-wait foundation passed6/6 meaningful tests atdc83453c, including cancellation,
actor deadlines, cycles, all shared blockers, savepoint release and fencing.
Subsequent native-key announcement hooks and catalog-generation cache remain
unqualified until their source-scoped regressions and broad suites pass. Signed
mutations additionally require core-checked current-row authority/version,
correct actual-action grant consumption and unchanged policy visibility.
No claim of repaired protected/typed/secondary behavior follows from the
foundation tests alone.


At3d212329 all14 natural protocol schedules passed2.84s. The original
concurrent_upsert target reported8pass/2fail because its two secondary tests
encoded the old READ COMMITTED40001 limitation and now received successful
CommandComplete1. Those tests were strengthened to require COMMIT and actual
updated byte payload02; a direct native fixed-snapshot API control still requires
TransactionConflict and preserves winner payload20. An attempted writable
REPEATABLE READ protocol control hit the existing profile's0A000 precondition;
its failure was preserved, not counted as a runtime proof or used to enable a
new profile. At5944023e both protocol targets24, signed authority2 and native/
static composite2 all passed:28total,0fail. Logs:
`read-committed-arbiters-first-fix.log`, `read-committed-arbiters-complete.log`,
and `read-committed-arbiters-supported.log`.

The native bridge at1ca4a355 passed10tests (6wait foundation+4native). Review
then found blind updates unnecessarily reserving an unchanged unique key while
a rival speculative insert held that key. The exact new regression at35ec5d01
failed immediately with TransactionConflict. The correction at e1e17bb1 uses an
available resident preimage without payload parsing/page I/O, preserves supplied
or read-cache preimages and original version/commit uniqueness validation, and
avoids preimage work when the collection has no unique index. Its final focused
result and broader qualification are pending. The red log is
`native-unique-unchanged-key-repro.log`.
