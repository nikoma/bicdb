# Named nested-table SQL qualification

This checkpoint implements registered SQL nested-table types in the experimental
`oracle-preview-v1` profile. It does not qualify PL/SQL collection methods or the
complete application.

The runtime source is `177cdb67e56f2112294aeff398f76227474ea118`. The tested
development binary has SHA-256
`276e81f421a0aae9405fe50d0b7ef7aa2d3195a7d3323eb5721bd5b7643109a6`.
It is an unoptimized development build with debug information and incremental
compilation disabled and one compiler job. The retained local executable is
`/tmp/bicdb-nested-table-qualified-lguq9tgo/oracle_server`; this is a development
artifact, not a release or a verified application deployment.

## Behavior and independent controls

`CREATE TYPE name AS TABLE OF element` registers logical element declarations
with native array-domain carriers. SQL constructors preserve empty collections,
whole NULL collections and NULL elements. Character limits retain BYTE/CHAR units,
reject overflow without truncation, and preserve fixed-character padding.

The public [Oracle fixture](../../../examples/oracle/reference/nested_table_types.py)
matches 15/15 source-qualified cases through PostgreSQL drivers. These include
four declarations, Unicode, quotes and delimiters, fixed-character padding,
whole NULL, empty and NULL elements, integer NUMBER elements and ORA-22814 byte
overflow. The fixture registers standard array loaders for the target's logical
result OIDs; it does not bypass unregistered-type checks or claim automatic
Oracle collection decoding by every PostgreSQL driver.

The independent [PostgreSQL controls](../../../examples/oracle/reference/postgres_domain_carriers.py)
match 7/7 native text/binary cases. The previous security-qualified binary
`fb6d5e1bcf5bee3c2695e39aca8bc1f40fa218ebe9fb5602bcc63d0e2e551bb7`
matched 2/7. The general fix makes domain text output delegate to its base type's
codec, including arrays and nested domains, rather than sending JSON array text.
Logical domain result-OID parity with PostgreSQL is outside this contract.

Six reader checks preserve existing routines, execute registered constructors
before and after reopen, verify that the previous reader refuses format-5
metadata, and reopen successfully after that refusal. Temporal, character and
hierarchy saves preserve the format-5 marker. Native domains remain untagged.

## Remaining application work

The original application collection type is not installed by this checkpoint.
PL/SQL constructors, COUNT/EXTEND and indexed assignment, TABLE(collection),
multiset operations, collection replacement, object elements and sparse methods
remain outstanding. The routine compiler stays at version 18. The existing
62/62 definition inventory is not full runtime acceptance, and historical
original collection probes remain unqualified. Dictionary rows, constraints,
startup, business workflows, performance, recovery and the broader migration
programme remain open.

The machine-readable validation record records the completed checks separately
from these remaining requirements. Reference fixtures use only owned synthetic
objects and clean them up; application diagnostics use a disposable database
copy and do not qualify valid business inputs or workflows.

## Validation

The [validation record](validation.json) reports 1,358 passing Rust tests, three
existing ignores, 58 passing Python tests and the final four focused collection
tests. Formatting, license, repository-boundary and whitespace checks pass.
Fifty retained reference reports pass on the same binary, including the hierarchy,
character, national, routine, transaction and security contracts. The separately
tracked dictionary comparisons remain 0/11 and the known NVL/conversion gap batch
remains 2/7; neither is counted as a passing capability. The application clone
still accepts 62/62 definitions, without source procedure execution comparison.

Build and reference processes ran under active memory guards with one compiler
job, at least 8 GiB of required host headroom and bounded output storage. All
recorded guards completed normally. These checks are local validation and do not
claim GitHub CI results, failure-injection recovery or full application operation.
