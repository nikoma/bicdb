# Routine-local Oracle BLOB values

Compiler contract 29 retains BLOB declaration provenance in routine parameters,
local variables and return values. Binary results continue to use PostgreSQL
`bytea` transport. The version 28 language and OID remain available with their
previous binary value semantics; new rules do not reclassify PostgreSQL bytea.

`EMPTY_BLOB()` is a non-NULL empty value. Qualified RAW expressions can create
routine-local temporary BLOBs. Selected DBMS_LOB operations support temporary
APPEND, GETLENGTH and ISTEMPORARY, including named APPEND arguments and the SYS
qualifier. Assignment uses copy on write; self-append reads the original source.
Temporary changes survive commit, rollback and rollback to savepoint.

The public [comparison fixture](../../../examples/oracle/reference/temporary_blobs.py)
creates nonce-named functions, compares 18 observations and removes those
functions. It checks binary results, aliasing, length/temporary status, NULL
priority and the different empty-destination errors for SQL versus PL/SQL
callers. [Validation results](validation-results.json) record the executed checks.
The HEXTORAW comparison separately protects the previously qualified conversion
and binary parameter/result paths.

Local regressions additionally cover database reopen, nested declaration scope,
BLOB IN/OUT parameters, sorted function results and the previous compiler
contract. Locator provenance is private execution state: JSON and wire output
expose bytes, and JSON deserialization cannot manufacture a locator capability.

This checkpoint does not implement persistent row-backed locator reads or
mutation. Existing stored BLOB bytes do not become persistent locators when read.
Persistent identities, transactional locator snapshots, expression/view lineage,
all implicit conversions, LOB comparisons/arrays, CLOBs, streaming and complete
DBMS_LOB/JDBC APIs remain open. This is neither full application qualification
nor general Oracle conformance, and does not establish a deployed release.
