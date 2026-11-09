# Oracle simple case v1 reference

The frozen mapping comes from read-only Oracle 23.26.3 AL32UTF8 UPPER/LOWER
queries over every Unicode scalar (1,112,064 values; surrogates excluded).
There are 1,485 changed uppercase and 1,486 changed lowercase mappings.
The separate validation checks all changed scalars plus ASCII individually,
256 deterministic random strings and fixed context/ligature/Greek/Deseret probes.
All 3,307 validation queries agree. No application rows were read or changed.

The first capture's product-name filter returned no version row because the
vendor now calls this product Oracle AI Database 26ai Free. The separate
vendor-version query records the actual 23.26.3.0.0 version. Database character
sets and session NLS settings are retained. Scripts read the existing local
qualification connection configuration; no password or credential is included.

Reference SHA-256: `76de86121dd9dee1275a954ba60a98966cf0c5d0e5fed5cec6110ac092527350`.
This is a versioned compatibility table, not a dependency on future host
Unicode/locale data. It must not change existing ASCII Upper/Lower index keys.
The implementation and persistence fences must be qualified separately.

The native key foundation at `39f6c197` passed three integration regressions
(each in embedded-memory and server-paged modes), two mapping units and the
SQL crate compile check. It preserves legacy ASCII keys through backfill,
mutation, rollback and reopen; new Unicode unique keys reject actual Unicode
collisions; a Unicode expression used only in a partial predicate also enables
the sticky format capability. Source `f039cdf0` repeats these checks and adds
only an optional synthetic fixture capture hook.

A real old native reader at `0af1b55e` opened that captured store and refused
`oracle_simple_case_v1` with exit 101. Every file's contents, size and mtime,
and the complete listing, remained unchanged. The proof contains hashes and
an error message; generated signing material stays in the private task store.
The format fence is persisted before a native build checkpoint can contain a
new definition, and remains after index DROP.

This is **not yet the Oracle SQL UPPER/LOWER fix**. Front-end execution and safe
migration of existing Oracle expression and partial indexes remain necessary.
Merely bypassing an old ASCII index in the planner would leave its uniqueness
constraint with different semantics. Existing native ASCII key tags and
PostgreSQL C-locale behavior must remain unchanged.

National character case mapping has a separately captured AL16UTF16 reference:
`UPPER(TO_NCHAR(:value))` and `LOWER(TO_NCHAR(:value))` over all 1,112,064 scalars.
It has 1,505 uppercase and 1,488 lowercase changes: 20 uppercase and two lowercase
overrides relative to AL32UTF8. All 3,329 independent standalone/random/context
checks agree. Reference SHA-256:
`d9e5861bd20418a4a25d83436ad886b47ef367f1760e17b8a613cf6c88f5bd4e`.
The national index tags and helpers preserve this distinction, including partial
predicates and uniqueness. SQL must select the family from the argument's Oracle
character type; a database-profile switch alone does not establish NCHAR semantics.
These reference and native-foundation checks do not qualify the unfinished SQL
migration or runtime dispatch.
