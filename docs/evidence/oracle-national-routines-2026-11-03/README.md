# National character routine declarations

Oracle preview compiler version 17 adds bounded AL16UTF16 semantics for
NVARCHAR2 and NCHAR routine declarations. Logical national-character types
survive compilation while the PostgreSQL driver uses ordinary VARCHAR wire
codecs. Existing compiler versions retain their previous behavior. Full Oracle
compatibility and the original Oracle-provider iDempiere application remain
incomplete; the running application is not upgraded by this qualification.

## Independent comparisons

[oracle-national-characters.json](oracle-national-characters.json) records
**96/96 declared-contract checks** against Oracle 23.26.3.0.0 with an AL16UTF16
national character set. Ninety-five checks cover accepted behavior and mapped
assignment errors. One additional negative check records Oracle's invalid
function error for an NVARCHAR declaration and the preview's explicit 0A000
rejection. That check does not establish invalid-object catalog parity.

The bounded contract includes NVARCHAR2 UTF16 code-unit capacity, empty values,
NCHAR default length and fixed padding, supplementary and combining characters,
maximum routine buffer limits, LENGTH/LENGTHB/LENGTH2/LENGTH4, and the observed
LENGTHC complete-character rules. Fixtures distinguish combining marks,
Hangul composition, spacing/enclosing marks, emoji modifiers and ZWJ sequences;
complete characters are not assumed to be extended grapheme clusters.

Qualified routine expressions retain national provenance through nested scope,
formal and return types, supported concatenation, NVL, DECODE, static host
variables and dynamic IN values. Fixed NCHAR equality/inequality with character
literals uses blank-padding semantics; mixed varying-character comparisons keep
the varying-character behavior. A prepared PostgreSQL-driver call confirms the
physical VARCHAR result codec. Physical persisted signatures contain no new
national typmod; versioned definitions recreate the compiler-local descriptor.

[postgres-character-controls.json](postgres-character-controls.json) records
**19/19** independent PostgreSQL 18.4 controls for native VARCHAR assignment,
Unicode scalar lengths, trailing-space handling, empty values and overflow.
They use a separate native-profile target. National preview rules do not select
native PostgreSQL behavior.

Run the public entrypoints with dedicated databases and private connection
configuration:

```sh
python examples/oracle/reference/national_characters.py \
  --oracle-config /private/reference.json \
  --output /private/national-characters.json
python examples/oracle/reference/postgres_national_controls.py \
  --output /private/postgres-character-controls.json
```

Set BICDB_PG_DSN to the appropriate dedicated target. The Oracle comparison
requires a matching pre-existing schema; the native controls require
ORACLE_CONTROL_PG_DSN. Fixtures remove their own created functions.

## Original routines and application boundary

[original-counter-comparisons.json](original-counter-comparisons.json) records
**14/14** comparisons using unchanged GET_SYSCONFIG, NEXTID and NEXTIDFUNC
bodies with minimal dependency tables in generated schemas. The tests cover
ordinary/system table counters, repeated calls while a parent write is pending,
separate observer visibility, and independently committed counters surviving
parent rollback. Native sequence and missing-row handler branches are not
qualified by this fixture.

[original-record-comparisons.json](original-record-comparisons.json) retains
**8/8** minimal-dependency TORECORDID checks; its tested long-name branch still
returns NULL on the reference and target. The matching result does not establish
working long-name record discovery.
[original-sequence-comparisons.json](original-sequence-comparisons.json) retains
**9/9** ordinary sequence procedure checks.

[application-copy-diagnostics.json](application-copy-diagnostics.json) records
**60/62** original definitions on a disposable imported application copy, with
**1/13** existing NULL-input procedure diagnostics succeeding. The remaining
definition blockers are hierarchical query/collection support and an Oracle
dictionary %TYPE anchor. Neither definition acceptance nor isolated routine
comparisons establish JDBC application startup or business workflow acceptance.
No application-specific engine exceptions were added.

General national SQL column types and literals, broader NLS/collation behavior,
all comparison/operator combinations, positional operations, NCHAR declarations
above 16,383 units, packages and the other programme workstreams remain open.
National SUBSTR/SUBSTRING, OVERLAY, REVERSE, LPAD and RPAD operations reject
execution pending a UTF16 position contract; they do not silently use PostgreSQL
scalar positions.

## Retained validation and recovery

[retained-oracle-controls.json](retained-oracle-controls.json) records
**1,201/1,201** earlier generated Oracle cases. Adding the 95 new accepted/error
contract cases gives **1,296** passing cases; the explicit alias rejection
boundary is tracked separately. [retained-postgres-controls.json](retained-postgres-controls.json)
records **227/227** earlier native controls, giving **246/246** with the new
character controls. Original application routines are separate from generated
corpus totals. [existing-semantic-gap-observations.json](existing-semantic-gap-observations.json)
retains five mismatches among seven separately tracked probes; their outcomes
are unchanged and excluded from the passing corpus.

[reader-and-restart.json](reader-and-restart.json) records a version-16 reader
refusing a version-17 routine before its side effect. The current reader executes
the dynamic INSERT once, and the committed row survives restart.
[validation.json](validation.json) records **1,311** passing Rust tests, three
existing ignores, Python checks and source/binary identities. An expanded
reference run initially found seven incorrect NVARCHAR-alias acceptances.
The implementation now rejects that spelling, retains the negative boundary
case and passes the corrected positive matrix and retained qualification.

Builds use one compiler job, disabled incremental compilation and an active
8 GiB available-memory floor. Task-owned temporary data and obsolete executables
are cleaned after validation; current binaries, compatibility readers and other
work are preserved. Merged source, a development binary, a published release and
a verified application deployment remain separate states.
