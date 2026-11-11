# Routine defaults and function result declarations

These changes address routine behavior independently of the private transactional
rename and native catalog admission work. The candidate contains four SQL source
files and focused tests, on the table-row routine fixes delivered separately.

Omitted PostgreSQL arguments use the current database type catalog, so a default
such as `(ROW(1,'kept')::typed_row).payload` can resolve a table's composite type.
Scalar functions, procedures and set-returning functions complete defaults as the
caller before entering a SECURITY DEFINER body. Caller-local variables, slots and
expression memos are excluded. Fully supplied calls retain borrowed argument
slices; the existing Oracle parameter and frame default paths are preserved.

`pg_get_function_result` retains `TABLE(item typed_row)` declarations and reports
`SETOF integer` consistently through both the pg_proc row optimization and direct
OID evaluation. Numeric return type identities and the routine execution protocol
remain separate from this display declaration.

Independent PostgreSQL 16.13 controls passed the exact TABLE and SETOF declarations
through both evaluation paths, table row default casts, procedure calls and empty
table-function results before and after reconnecting. The caller-security control
verified three denied protected-function defaults, three successful explicit-value
calls and three successful default calls after an explicit grant. Temporary schemas
and the temporary role were removed; the protected application schema was not
modified. Control scripts and receipts are retained with SHA-256 hashes.

Tests-only source `a51de2226838a0e393eb34889cccefe90cecc666`, on the frozen
1.0.469-beta table-row fix parent, failed all three new tests: TABLE result
metadata was `SETOF record`, a protected-function default returned unsupported
SQL (`0A000`) instead of permission denial (`42501`), and the table row cast
default failed because `typed_row` could not be resolved. These failures occur
before the later reopen or grant checks; no unexecuted cases are counted as tested.

Source `79a0b992794395128927d7709c53c67e25300ee7` passed all four commands:
11 direct routine/default/permission/version tests, 29 selected type and read-only
tests, 864 SQL library tests and 419 general SQL tests. This is 1,323 executions
including repeated subsets. Four existing benchmark tests remain ignored. The
runner used the same fixed checkout and serial source-build lock; the logs record
compilation from that checkout at package version 1.0.469-beta. The actual negative
and positive outcomes use identical test assertions. This preliminary result is
not a test of the later versioned final head.

The delivery candidate integrates the frozen PR146 parent
`c895ef71dd90de577dc0177b58e7d3c819c71cd0` and prepares source
`1.0.471-beta`, following the separately delivered 469 and 470 batches. It requires
fresh versioned direct checks and all nine exact-head CI gates, and may merge only
after remote main contains the final 470 parent and 471 is its next unused version.
Historical result versions above are preserved. Final CI and delivery evidence
are reported on the pull request.

A prepared source version does not imply a built standard
binary, published release, deployment, complete iDempiere acceptance, or HA/VM
qualification. Original application assertions and the 10-second timeout remain
unchanged.
