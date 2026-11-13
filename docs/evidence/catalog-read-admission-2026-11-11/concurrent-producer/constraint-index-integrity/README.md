# Constraint index integrity controls

Private source `f5090de8df6c7d432dbed66e3b57c6c21df43a98` passed **266 focused tests** across 13 test binaries. This is an implementation checkpoint, not a merged source, broad workspace result or application acceptance result.

The implementation resolves index drops using the actual namespace and search path, preserves exact quoted spelling, checks the complete multi-index statement before removing anything, and refuses direct removal of primary-key, unique and exclusion constraint backings. Materialized-view index descriptors and physical storage identities are included. PostgreSQL keeps SQLSTATE `2BP01`; the separate Oracle profile returns ORA-02429 for primary/unique backing removal. Oracle's diagnostic is checked by the new profile regression and its published error documentation, not a newly executed native Oracle control: https://docs.oracle.com/en/error-help/db/ora-02429/.

The focused batch contains six new integration tests, 41 existing SQL authorization/catalog/constraint tests, one existing case-mapped index library test and 218 catalog/read-admission/wire protocol tests. Both storage modes are exercised where each test specifies them. The original upstream application assertions and timeout limits were not changed.

The retained negative runs distinguish actual defects from test preparation:

- `852ee479`: the new fixture did not compile because closing a database consumes it. No product test ran.
- `43768d16`: the corrected fixture ran one passing and four failing tests against the old production code. It exposed constraint backing removal, incomplete multi-drop preflight and missing materialized-view descriptor cleanup. A quoted dotted-name case control passed.
- `a499113c`: only the case fixture changed to ASCII names differing by letter case. Old production code removed both Native indexes when asked to remove one; the test failed as intended.
- `1224f8a9`: the first fixed implementation passed all five then-current focused tests.
- `f5090de8`: the final quoted ASCII identity, view storage and separate Oracle diagnostic batch passed all 266 selected tests.

`native-pg16-control` records five real PostgreSQL 16.13 dependency refusals, preservation of the earlier standalone index after a refused multi-drop, and the acknowledged row. It used an owned control database, a new schema and a rolled-back transaction. The protected source database and frozen export were not modified.

Each receipt names its actual source. Raw logs are gzip compressed without changing their contents; `summary.json` records hashes. The original 95-test application probe has not been rerun on this source. Its earlier `a68fe5f3` result remains 93 passes and two failures, recorded separately.

Remaining work includes executable DEFAULT partition semantics, safe index rollback with pending owners, and authenticated portable Native origin binding for imported rename recovery. This batch does not certify the full foreign-key dependency graph of standalone unique indexes, automatic distributed partitioning, HA, backup acceptance or production readiness.
