# Public dictionary type anchors

Oracle-preview table/view %TYPE resolution now considers the public dictionary
synonym after local object lookup. Local tables and views retain precedence.
Explicit SYS qualification uses the fixed dictionary descriptors; an explicit
application-schema prefix requires an object in that schema. Quoted names retain
their case, and a missing local column does not fall through to the dictionary.
This is a general namespace rule, with no application-specific engine behavior.

The registry describes SYS.USER_TABLES and SYS.USER_TAB_COLUMNS from the qualified
Oracle 23 AL32UTF8 environment. VARCHAR2 descriptors preserve BYTE width, NUMBER
descriptors retain their unconstrained declaration, and DATE remains a logical
DATE when used in a routine. %TYPE does not inherit the column's NOT NULL or
default constraint. Source controls also verify local numeric rounding, local
view shadowing and explicit SYS lookup. The DATE regression returns TIMESTAMP so
that return coercion cannot conceal a missing DATE assignment conversion.

All 135 source descriptors were independently bound and executed on Oracle.
The public cross-database fixture matches **132/135**: all VARCHAR2, NUMBER and
DATE cases pass. DATA_DEFAULT (LONG), LOW_VALUE (RAW) and HIGH_VALUE (RAW) remain
unsupported and return explicit feature errors. They are not counted as passing
Oracle contracts. The fixture deliberately returns nonzero while those three
contracts remain unmatched:

```sh
python examples/oracle/reference/dictionary_type_anchors.py \
  --oracle-config /private/reference.json --output /private/type-anchors.json
```

Set BICDB_PG_DSN privately to an Oracle-preview target with a matching existing
fixture schema. The source must use AL32UTF8, BYTE defaults, decimal-point numeric
characters and the Gregorian calendar. Only randomly named functions are created
and removed. Source dictionary metadata is read; target dictionary contents are
not supplied by this implementation. An initial report-serialization failure
was corrected by converting integral Oracle descriptor widths before JSON
encoding; that attempt is not passing qualification.

The isolated application copy now accepts **61/62 unchanged original routine
definitions** through PostgreSQL drivers. AD_COLUMN_SYNC is newly accepted.
TOTABLEOFVARCHAR2 remains rejected for hierarchical-query support and also needs
collection behavior. NULL-input procedure diagnostics now cover 14 accepted
procedures, of which only one completes. No source application procedures were
executed. Acceptance of the new definition does not establish dictionary queries,
dynamic DDL behavior, execution equivalence or business-workflow correctness.

The broad local batch passes **1,332 Rust tests**, with three existing ignores.
The retained **266 PostgreSQL controls**, **1,475 Oracle positive/error contracts**,
one separate negative national-alias boundary and **53 buffer contracts** retain
their results. The seven routine-reader checks, nine reconciliation checks, six
character-metadata reader checks and three importer checks pass. Original counter,
record and sequence cases retain 14/14, 8/8 and 9/9 matches.

Oracle dictionary-row contracts remain **0/11**; separate character assignments
remain 2/2. Five tracked NVL/conversion mismatches remain explicit. Dictionary
defaults and visibility, view-expression type provenance, unsupported scalar
types, hierarchy and collections, constraints, startup and full workflows remain
programme work. No application runtime, release or deployment is qualified.

One compiler, disabled incremental compilation and the active minimum 8 GiB
available-RAM guard remain in use. Cleanup removed 11 completed test executables,
reclaiming 3,231,031,264 bytes; reusable inputs and runnable readers were preserved.
The Oracle build cache is 6.9 GiB. The existing running database is unchanged.
