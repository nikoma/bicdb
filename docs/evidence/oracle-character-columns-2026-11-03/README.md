# Character column capacity and provenance

New Oracle-preview CHAR/VARCHAR2 columns preserve BYTE/CHAR length units rather
than mapping both declarations to the same native character constraint. The
qualified environment is AL32UTF8, NLS_LENGTH_SEMANTICS=BYTE and
MAX_STRING_SIZE=STANDARD. Assignment enforces the declared unit and SQL byte
capacity, normalizes empty strings, and applies fixed padding within the byte
budget. INSERT, UPDATE and COPY retain mapped ORA-12899 errors through ordinary
coercion wrappers. Failed UPDATE and ALTER checks preserve saved values.

Source observations include the global byte ceiling for CHAR(2000 CHAR) and
VARCHAR2(4000 CHAR). One `é` in the fixed column produces 1,999 characters and
2,000 bytes. A value containing 2,000 copies of `é` fits the VARCHAR2 column;
2,001 copies exceed its 4,000-byte maximum. Unit and byte-limit checks both apply.
These are source-observed rules, not approximations inferred from native typmods.

Profile metadata format 3 fences the new serialized character modifier. Older
readers refuse the new format at startup; current readers accept earlier
formats. Native pgwire varchar/bpchar codecs remain in use. New declarations are
available through `bicdb_oracle_column_type`; unknown legacy provenance remains
unknown. SQL unit regressions cover persistence, metadata validation, reader
requirements, atomic ALTER checks and independent PostgreSQL trailing-space
behavior.

The production migration importer retains exported units and validates target
behavior and logical declarations before continuing. Resume preflight examines
completed legacy tables before capability-probe DDL can change the profile
format. The executable reader checks show that failed read-only preflight leaves
legacy data and old-reader access intact. Importer checks also exercise generated
DDL, COPY and source/target row hashes. This change does not reconcile or upgrade
the existing application database.

The retained original sequence procedure exposed a shared expression type-cache
bug during qualification: temporary cloned argument AST addresses could survive
in a routine's address-based memo and later be reused for a different type.
Ordinary arguments are now borrowed from their original AST. Synthesized
SQL-standard JSON arguments suspend the memo scope for their lifetime, including
runtime evaluation; record-population calls also borrow their arguments. The
regression checks both type-inference paths and rejects memo entries outside the
original AST. It failed before the correction. This is a general cache ownership
rule; application names never select engine behavior.

Routine-local BYTE capacity, BYTE `%TYPE` propagation, casts, national table
columns, configurable character sets/NLS, EXTENDED strings and legacy metadata
reconciliation remain incomplete. USER_TABLES/USER_TAB_COLUMNS are not implemented
here: the dictionary fixture continues to report 0/11, separately from the now
passing 2/2 character assignments. The isolated application-copy baseline remains
60/62 accepted definitions and 1/13 NULL-input diagnostic calls. These checks do
not establish application startup, workflows, complete schema migration or full
Oracle compatibility. The running application binary and database are unchanged.

Validation and binary identities are recorded in validation.json. Real Oracle
and PostgreSQL comparisons are external reference runs, separate from CI.

The final character-column fixture matches **138/138** Oracle comparisons. The
retained generated corpus plus view anchors brings passing positive/error
contracts to **1,475**, with one separate negative NVARCHAR-alias boundary; the
retained table-anchor rerun is not double-counted. Independent PostgreSQL
controls match **252/252**. Original minimal-dependency fixtures match counter
14/14, record lookup 8/8 and sequence 9/9. Reader/restart checks pass 6/6 and
production-importer checks pass 3/3. The dictionary failures and the five
separately tracked NVL/conversion mismatches remain explicit.

Local validation passes **1,319 Rust tests**, with three existing ignores, and
**52 Python tests**. After naming the scope-guard field to remove a compiler
warning, all 473 SQL unit tests were repeated and the example server rebuilt;
all external candidate reports identify that final binary. Formatting, license
checks and the public dependency boundary pass. One compiler and disabled
incremental compilation preserve memory headroom; the successful final batches
observe at least 18.8 GiB available RAM. Earlier disk and memory interruptions
are recorded separately from successful checks. The last two cleanup batches
reclaim 3,202,742,352 bytes of completed test executables, leaving the Oracle
build cache at 6.8G. An earlier cleanup reduced the older schema cache from
48G to 22G without removing current executables, compatibility readers, active
files or other worktrees' output. No release is published by this milestone.
