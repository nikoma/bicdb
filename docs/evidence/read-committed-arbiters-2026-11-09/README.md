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
