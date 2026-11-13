# Native Script error-origin acceptance

Run with Node against an explicit pinned ordinary native BicDB artifact:

`node tests/node-script-origin/run.mjs immutable-toolchain.json fresh-output-directory`

The toolchain JSON contains `{"native":{"binary":"/absolute/bicdb","sha256":"<64 hex>"}}`.
The driver verifies the actual binary hash, creates test-owned SQL tables and
source-authored signed SDK packages, and uses fresh synthetic RS256 identities.
It tests original host-owned transaction denials and rollback/savepoint behavior,
plus original/replaced/copied/string/cause errors, mutable diagnostic getters,
inherited message setters, replaced global Error constructors, stale native
origins, synchronous/asynchronous rejection and caught normal completion. Actual
retained rows are checked after SIGKILL and a cold store copy. It never changes
HTTP/VM quotas or an existing account.

`qualify-original.mjs` preserves the earlier complete assertion source. Its direct
view SQL submatrix is a separate unresolved host relation target restriction;
it is not executed, waived or qualified by this error-origin regression. This
fixture cannot establish whole ERP, direct-view, Hub or customer qualification.
