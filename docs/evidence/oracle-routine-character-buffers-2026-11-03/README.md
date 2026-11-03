# PL/SQL character buffer units

Preview compiler version 18 preserves BYTE and CHAR units for local CHAR and
VARCHAR2 variables, including table-column type anchors. The AL32UTF8/BYTE-default
contract uses byte capacity for implicit declarations, character capacity for
explicit CHAR declarations, and a 32,767-byte ceiling for both. Empty text becomes
NULL. Oversized assignments, including excess trailing spaces, raise the PL/SQL
buffer error ORA-06502 through SQLSTATE 22001. They do not use SQL column overflow
codes or PostgreSQL's trailing-space truncation rule.

Fixed CHAR values receive spaces in the declared unit. At the byte ceiling,
Oracle limits padding to the remaining bytes: a CHAR(32767 CHAR) variable holding
one two-byte character has 32,766 characters and 32,767 bytes after padding.
An input that already exceeds the byte ceiling still fails. The initial fixture
expected the padding case to fail; actual Oracle execution corrected that
expectation before final qualification. Multibyte RPAD display-width behavior,
general conversions and unrestricted NLS configuration are separate work.

The modifier is local to the versioned routine compiler. Native PostgreSQL
routines and stored preview routines using older compiler versions retain their
behavior. Formal argument modifiers continue to follow the existing signature
rules. The new compiler does not automatically recompile old routines. The
reference reader can open the shared profile format and execute its old routine,
but refuses execution of the new compiler version. A current reader then reopens
both definitions successfully. This is an execution fence, not a claim that every
older reader rejects the whole database at startup.

Three Rust regressions cover strict assignment, BYTE/CHAR padding, default CHAR,
both units of table anchors, the byte ceiling, reopen, older compiler behavior
and independent native routine behavior. The public differential fixture checks
53 contracts against Oracle, including supplementary characters, empty/NULL
values, trailing-space overflow and large buffers:

```sh
python examples/oracle/reference/routine_character_buffers.py \
  --oracle-config /private/reference.json --output /private/buffers.json
```

Set BICDB_PG_DSN privately to a dedicated Oracle-preview target. Both databases
need the same existing fixture schema. The fixture requires an AL32UTF8 source
with BYTE defaults, creates only randomly named objects, and removes them. The
previous candidate matches 36/53; the new candidate matches **53/53**. All seven
cross-reader execution/reopen checks pass.

Validation of the integrated source passes **1,329 Rust tests**, with three
existing ignores, and **58 Python tests**. All **266/266** independent PostgreSQL
controls pass. The retained Oracle corpus preserves **1,475** positive/error
contracts plus one separate negative national-alias boundary. The new 53 buffer
contracts are counted separately. Nine reconciliation checks, six character
metadata reader checks and three importer checks also pass. Original counter,
record and sequence fixtures retain 14/14, 8/8 and 9/9 matches.

A preliminary validation was intentionally interrupted to correct the measured
padding rule. Another compilation received SIGTERM without a recorded resource
threshold breach; those attempts are not passing validation. The successful
integrated run rechecks the complete suite after merging current dependencies.
Builds use one compiler, disabled incremental compilation and an active minimum
8 GiB available-RAM guard. The qualified development example optimizes only the
SQL package at level 1; its full runtime comparisons use that exact binary.
This is not a published release or a performance benchmark.

Application definitions remain **60/62**, NULL-input diagnostics remain **1/13**,
and Oracle dictionary queries remain **0/11**. Separate character assignments
remain 2/2, and five tracked NVL/conversion mismatches remain explicit. Full
character reconciliation, dictionary metadata and visibility, hierarchy and
collections, constraints, application startup and business workflows remain
unqualified. Neither the running application database nor its binary was
upgraded by these checks.
