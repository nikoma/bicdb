# Oracle compatibility programme

The full programme covers all workstreams below. Running the HR fixture is an
initial acceptance milestone, not completion of the programme. The supported
client transport is the PostgreSQL protocol with explicit SQL-profile opt-in.
Unchanged Oracle drivers would require a separate protocol implementation.

`oracle-hr-v1` remains an experimental, bounded profile. Its
[contract and limitations](oracle-hr-profile.md) apply until a broader profile
has an explicit implementation and qualification record. Default PostgreSQL
behavior must remain covered by regression tests throughout this work.

Compatibility fixes must implement general semantic rules, independent of
application, tenant, schema or table names. Each change needs regressions for
default PostgreSQL behavior and the intended Oracle behavior independently;
Oracle reference comparisons establish the latter where available. Application
workloads expose gaps and provide acceptance tests. Their identities must never
select special engine behavior.

The separate [`oracle-preview-v1`](oracle-preview-profile.md) profile is now used
for [iDempiere seed development](evidence/idempiere-seed-2026-11-01/README.md).
The latest [dictionary type-anchor checkpoint](evidence/oracle-dictionary-type-anchors-2026-11-03/README.md)
accepts **61/62 unchanged routine definitions** through PostgreSQL drivers.
The remaining definition needs hierarchical queries and collection behavior;
dictionary rows, constraints, complete execution and workflows remain incomplete.
The milestones below retain their results at the time each was measured.

The original 913 tables and 185,292 rows loaded and passed complete value
reconciliation. A separate Oracle reference clone was upgraded through 24
release-13 migrations; its 915 tables and 185,471 initial rows were also imported
and reconciled. The [Oracle-provider application login](evidence/idempiere-release13-desktop-2026-11-01/README.md)
reached a desktop shell through PostgreSQL drivers with application errors.
Startup and business workflows are not qualified. The later
[object inventory](evidence/idempiere-object-coverage-2026-11-01/README.md) records
30 accepted routine definitions and 90 repeated non-clock seed-view matches;
catalog acceptance and bounded seed results are not full execution acceptance.
The subsequent [column-anchor checks](evidence/oracle-column-anchors-2026-11-02/README.md)
raise accepted original routine definitions to 35 of 62 and exercise selected
account-balance, remittance-location and payment-availability calls. Remaining
routine failures are retained as migration blockers. Two additional unchanged
views match repeated typed-row comparisons after manual dependency refresh,
raising cumulative strict non-clock seed-view matches to 92. Automatic view
dependency refresh remains outstanding.
The [procedural CASE qualification](evidence/oracle-procedural-case-2026-11-02/README.md)
adds general statement semantics with independent PostgreSQL regressions and
34 live Oracle comparisons. The selected original-routine recheck now matches
12 of 13 cases in that batch. The later
[nested-block qualification](evidence/oracle-nested-block-contracts-2026-11-02/README.md)
implements scoped blocks, versioned statement rollback and bounded ROWNUM prefix
predicates. After recreating the unchanged GET_SYSCONFIG source under compiler
version 6, all 13 selected original-routine comparisons match, together with
eight existing-configuration cases. This does not increase the 35 accepted
definitions or qualify all execution paths.
The [typed concatenation qualification](evidence/oracle-routine-concatenation-2026-11-02/README.md)
raises accepted original routine definitions to 42 of 62. Its 76 generated
Oracle comparisons cover bounded conversion, NULL and caller-length contracts;
37 additional selected original-routine calls match, alongside the previous 21
comparisons. Definition acceptance does not establish execution of every routine
or branch. Required foreign keys, complete routine/view behavior and remaining
application behavior are still outstanding.
The [parameterized-cursor qualification](evidence/oracle-parameterized-cursors-2026-11-02/README.md)
raises accepted original definitions to 43 of 62. Its 34 cursor and 218 NUMBER
arithmetic cases match Oracle; the previous 342 generated Oracle cases and 58
selected original calls still match. The newly accepted NEXTBUSINESSDAY source
matches only one of three execution probes: weekday/weekend calls still hit the
unimplemented Oracle DECODE semantics. Its NULL result is not DECODE qualification.
That execution blocker is superseded by the
[version-9 DECODE qualification](evidence/oracle-decode-2026-11-02/README.md): all
three probes and 96 expanded business-date calls now match. Broader holiday
coverage exposed and fixed unstable COALESCE argument type memoization on cursor
reopen. The generated corpus now contains 705 matching Oracle cases and 104
native PostgreSQL controls. The 43/62 definition count is unchanged.
The later [temporal-column qualification](evidence/oracle-temporal-columns-2026-11-02/README.md)
adds durable DATE/TIMESTAMP provenance, temporal column anchors, reader fencing
and source-driven metadata reconciliation with target value fingerprints. The
unchanged PRODUCTATTRIBUTE definition is now accepted, raising the baseline to
44/62. Its six initial execution probes still fail on unresolved default TO_CHAR
input; CHR type/conversion behavior remains outstanding. The application-wide
2,263-column backfill was in progress at that checkpoint. All 705 retained generated
Oracle cases plus 21 new column cases match, alongside 110 native PostgreSQL
controls. The remaining 18 definitions, foreign keys and full application
workflows remain migration blockers.
The [character-expression qualification](evidence/oracle-characters-2026-11-02/README.md)
resolves nested CHR/default TO_CHAR under compiler version 10 and fixes generic
COALESCE short-circuiting. All six unchanged PRODUCTATTRIBUTE seed probes now
match on the upgraded application database. The definition count remains 44/62.
All 2,263 temporal declarations across 906 tables are reconciled; independent
pre/post-snapshot comparisons preserve all 915 application tables and 185,537
current rows, including earlier application writes. The corpus now has 845
matching generated Oracle cases and 132 independent PostgreSQL controls. Full
routine/view behavior, deferred foreign keys and business workflows remain
unqualified. A named-schema primary-key UPDATE failure is independently
reproduced under both profiles and remains a separate engine blocker.
The [NVL qualification](evidence/oracle-nvl-2026-11-02/README.md) adds eager,
type-directed NVL and timestamp precision propagation under compiler version 11.
All 93 new comparisons and 845 retained generated cases match, alongside 132
native PostgreSQL controls. The upgraded development database preserves all
915 tables and 185,537 current rows. PRODUCTATTRIBUTE was explicitly recreated
from unchanged source and retains its six passing probes. The definition
baseline is still 44/62; the remaining definitions and full application behavior
are not established by these scalar/routine checks.
The [procedure qualification](evidence/oracle-procedures-2026-11-02/README.md)
adds version-12 procedure compilation, namespace binding and shared CALL output
semantics. The definition inventory advances to 57/62. Thirteen newly accepted
original procedures still fail diagnostic calls on a disposable application copy;
this is not application execution acceptance. The generated passing corpus is
964 Oracle cases with 145 native PostgreSQL controls. Five separately recorded
semantic mismatches reproduce on both the previous and new runtimes and remain
open. All 915 tables and 185,537 current rows remain preserved across the upgrade.
The [routine grammar qualification](evidence/oracle-grammar-2026-11-02/README.md)
adds version-13 legacy outer-join lowering and column lock-list binding, with
978 generated Oracle matches and 154 independent PostgreSQL controls. The
unchanged original AD_SEQUENCE_NEXT body also matches Oracle on eight controlled
synthetic-data checks, including state changes and rollback. This is a minimal
dependency fixture, not full application acceptance. All thirteen separate
NULL-argument application-copy procedure probes still fail; definition acceptance
remains 57/62. This qualification builds a development candidate without upgrading
the running application database. Five separately tracked semantic mismatches
remain unchanged on the candidate.
The [application-error qualification](evidence/oracle-application-errors-2026-11-02/README.md)
adds version-14 RAISE_APPLICATION_ERROR and bounded current SQLCODE/SQLERRM,
including saved-message lifetimes, reraising and SQL/PL entry boundaries.
The generated corpus reaches 1,086 matches with Oracle and 163 independent native
PostgreSQL controls. Native rethrow diagnostics are preserved. The original
sequence procedure is tested with minimal dependencies, including its missing-row
application error. Definitions remain 57/62; full application execution and the
remaining compatibility workstreams are not established. The running application
database is not upgraded by this qualification.
The [qualified key-update qualification](evidence/oracle-key-updates-2026-11-03/README.md)
removes generated SQL from primary-key moves, preserving UPDATE authorization,
row policies, typed values and rollback across named schemas. The combined
passing corpus reaches 1,127 Oracle comparisons and 214 native PostgreSQL controls.
Independent references distinguish Oracle statement-end uniqueness from PostgreSQL immediate
checks, including ordered shifts. Referenced parent keys, triggers, FROM/RETURNING,
concurrent moves and deferred-constraint modes remain outside this qualification.
No application database upgrade or full application acceptance is implied.
The [independent-transaction qualification](evidence/oracle-autonomous-transactions-2026-11-03/README.md)
adds version-15 standalone autonomous functions/procedures, with separate
transaction ownership, parent-lock protection, snapshot boundaries, Describe
safeguards and no speculative replay across independent calls. Its 38 new
Oracle matches bring the passing generated corpus to 1,165, with 214 independent
PostgreSQL controls. Recompiling original definitions on an isolated application
copy advances the definition baseline to 60/62. A valid original NEXTIDFUNC call
still fails at the dynamic-SQL namespace guard; hierarchy/collection support,
dictionary anchors and further runtime dependencies remain. The five separately
tracked semantic mismatches are unchanged. This is a development candidate,
with the running application binary and database preserved.
The [dynamic-statement qualification](evidence/oracle-dynamic-sql-2026-11-03/README.md)
adds version-16 EXECUTE IMMEDIATE with compiler-bound namespaces, typed positional
IN binds, scalar query INTO, quote-preserving SQL rendering and SQL/procedural
NO_DATA_FOUND boundaries. Its 36 new matches bring the passing generated corpus
to 1,201 Oracle cases, with 227 independent PostgreSQL controls. The unchanged
TORECORDID body matches eight isolated minimal-dependency checks, and the
ordinary sequence procedure retains nine matches. These are not application
workflow acceptance. Definitions remain 60/62, the NULL-call diagnostics remain
1/13, and a valid NEXTIDFUNC call still fails on national-character variable
typing. Five separately tracked semantic mismatches persist. Dynamic OUT binds,
bulk/RETURNING, DDL/PLSQL blocks and broader query/type binding remain incomplete.
The [national routine qualification](evidence/oracle-national-routines-2026-11-03/README.md)
adds version-17 AL16UTF16 routine declarations and logical host-value provenance.
It records 95 new accepted/error-contract cases, a separately tracked unsupported
alias boundary and 19 new native PostgreSQL controls. The generated passing
corpus becomes 1,296 cases, with 246 independent PostgreSQL controls. Unchanged
GET_SYSCONFIG/NEXTID/NEXTIDFUNC bodies now pass 14/14 isolated minimal-dependency
comparisons, including counter commit, parent rollback and observer visibility.
Definitions remain 60/62 and NULL-input diagnostics remain 1/13; the running
application is not upgraded or qualified. Broader national SQL/NLS semantics,
hierarchical queries, collections, dictionary anchors and the other workstreams
remain incomplete.
The [view-anchor prerequisite](evidence/oracle-view-anchors-2026-11-03/README.md)
adds metadata-only named view-column %TYPE resolution, qualified by 41 Oracle
cases and 41 retained table-anchor cases. It does not implement dictionary
views or advance application definition/runtime acceptance.
The [dictionary contract inventory](evidence/oracle-dictionary-contracts-2026-11-03/README.md)
fixes logical names and schema filtering in native column catalogs, qualified
against six PostgreSQL cases. Its Oracle dictionary fixture remains 0/11 and
its separate BYTE/CHAR assignment checks remain 1/2. Source observations show
that byte/character length provenance and byte-capacity enforcement must be
preserved before dictionary lengths can be generated reliably. Dictionary
views, visibility and source defaults remain implementation work.
The [character column qualification](evidence/oracle-character-columns-2026-11-03/README.md)
adds durable BYTE/CHAR provenance and STANDARD byte-capacity enforcement for new
CHAR/VARCHAR2 columns, with importer preflight and resume checks. The separate
assignment comparison now matches 2/2; dictionary contracts remain 0/11. Legacy
metadata reconciliation, routine BYTE propagation and broader character semantics
remain open. A shared expression-cache ownership correction preserves stable AST
nodes and excludes synthesized arguments from address-based memoization. Original
sequence checks retain 9/9 matches. These changes do not advance the 60/62
application definition baseline or the 1/13 NULL-input diagnostics.
The [legacy character reconciliation](evidence/oracle-character-reconciliation-2026-11-03/README.md)
adds read-only source/target preflight, value-preserving table transactions and
resume validation for CHAR/VARCHAR2 declarations. It also fixes native qualified
view-dependency checks and removes full-row undo copies for proven metadata-only
attachments. Nine live reconciliation checks and 257 independent PostgreSQL
controls pass. All 7,150 declarations across 910 application tables passed the
initial copy's read-only preflight; its apply run exposed the corrected undo
memory problem. Full application-copy reconciliation remains pending at this
checkpoint. Definitions remain 60/62, dictionary contracts remain 0/11, and
runtime/workflow qualification is still incomplete.
The [ALTER constraint batch qualification](evidence/oracle-alter-constraint-batches-2026-11-03/README.md)
prepares CHECK expressions once per rewritten row batch while retaining every
row's constraint validation and failure rollback. A 500-row regression verifies
the preparation count and exact restoration; nine new independent PostgreSQL
controls pass, bringing the retained native corpus to 266/266. The full Rust
batch passes 1,326 tests. A fresh persistent-copy character reconciliation trial
is pending. This optimization does not change the 60/62 definition inventory,
the 0/11 dictionary result or application/workflow qualification.
The [PL/SQL character buffer qualification](evidence/oracle-routine-character-buffers-2026-11-03/README.md)
adds compiler-version-18 local BYTE/CHAR buffers and preserves both units through
table-column anchors. All 53 source contracts and seven cross-reader execution
checks pass; native PostgreSQL and older stored compilers retain their behavior.
The integrated source passes 1,329 Rust tests and 58 Python tests. SQL column
limits, routine buffer limits and fixed-width padding remain distinct contracts.
Dictionary query contents, view-expression type provenance, full NLS behavior
and application qualification remain open; definitions remain 60/62.
The [dictionary type-anchor qualification](evidence/oracle-dictionary-type-anchors-2026-11-03/README.md)
adds local-object precedence and bounded SYS/public dictionary type descriptors,
raising definition acceptance to 61/62. All 132 VARCHAR2/NUMBER/DATE descriptor
contracts match Oracle; three LONG/RAW cases remain unsupported. The integrated
Rust batch passes 1,332 tests and all 266 retained PostgreSQL controls pass.
Dictionary queries remain 0/11, and only one of 14 accepted-procedure NULL-input
diagnostics completes. Hierarchy, collections and full execution remain open.
The [hierarchy contract inventory](evidence/oracle-hierarchy-contracts-2026-11-03/README.md)
qualifies 16 source cases covering root initialization, PRIOR direction, output
filtering, sibling order and expression-based cycle handling. The target remains 0/16 with explicit
unsupported-feature errors. In particular, nonpositive/NULL generator bounds
retain a root unless START WITH removes it; native series generation alone is
not equivalent. Distinct rows can share a PRIOR cycle key, while volatile PRIOR
values change cycle behavior. This checkpoint leaves the 61/62 definition baseline unchanged.
These are development data baselines, not full schema/application acceptance. The
[foreign-key inventory](evidence/idempiere-foreign-key-inventory-2026-11-02/README.md)
records all 3,826 relationships as initially deferred; 2,914 have source
NOT VALIDATED state. Preserving that timing and validation contract is required
before the foreign-key migration can be qualified.

“Partial” means a specific implementation or test exists, not that the full
workstream is compatible. “Unqualified” means Oracle behavior or operational
acceptance remains unproven, including when related native BicDB features exist.

| Workstream | Required acceptance | Current programme coverage |
| --- | --- | --- |
| 1. Versioned profiles | Durable profile identity, explicit startup selection, incompatible-open rejection, upgrade rules and PostgreSQL isolation | Partial: bounded HR and separate preview profiles, durable identity and versioned preview routine compiler; general upgrade qualification outstanding |
| 2. Migration assessment | Inventory source and runtime dependencies; report object locations, dynamic SQL, wrapped code, blockers and manual changes | Partial: [offline lexical inventory](oracle-migration-assessment.md); semantic/catalog/runtime assessment and dependency resolution outstanding |
| 3. SQL value semantics | Differential null/empty-string, concatenation, padding, comparison, coercion, uniqueness and composite-key tests | Partial: HR lowering and version-7 typed routine concatenation with NULL/padding/caller-length reference cases; general SQL coercion and padding qualification outstanding |
| 4. Types and conversions | Boundary/property tests for NUMBER, byte/character lengths, national types, DATE, timestamps/time zones, intervals, RAW and LOBs | Partial: native mappings, bounded NUMBER arithmetic/reference infinity transport, and durable DATE/TIMESTAMP column provenance with storage/anchor/reference checks; 2,263-column application temporal backfill and complete target-value preservation verified; durable BYTE/CHAR columns with STANDARD capacity and importer provenance checks; legacy character reconciliation, timezone regions and full conversion rules unqualified |
| 5. Scalar and analytic functions | Oracle type selection, short-circuiting, NLS, date arithmetic, conversion, aggregation and analytic behavior | Partial: bounded NVL, date formats and Gregorian ADD_MONTHS; version-9 bounded DECODE, version-10 AL32UTF8 CHR/default TO_CHAR, and version-11 eager typed NVL with timestamp precision; NVL2, PL/SQL constant propagation and broader scalar/NLS/type semantics outstanding |
| 6. SQL and DML constructs | DUAL, binds, alternative quoting, legacy joins, ROWNUM, hierarchy, MERGE, multitable INSERT, RETURNING, PIVOT/UNPIVOT | Partial: DUAL, pgwire parameters and literal-integer ROWNUM prefix predicates in plain blocks; other Oracle constructs unqualified; assess MODEL and MATCH_RECOGNIZE per workload |
| 7. Names and schema DDL | Quoted/unquoted resolution, cross-schema objects, synonyms, current schema and an explicit physical-clause migration policy | Partial: application-schema selection, versioned static function/procedure relation binding, CALL namespaces and documented heap adaptation; general resolution and synonyms outstanding |
| 8. Transactions and locking | Oracle statement rollback and error continuation, isolation, waits, locks, deadlocks, cancellation and read-only behavior | Partial: [nested-block contracts](evidence/oracle-nested-block-contracts-2026-11-02/README.md) distinguish new Oracle routine statement rollback from native PostgreSQL block rollback; general transactions, DDL commits and locking qualification outstanding |
| 9. DDL lifecycle | Oracle implicit commits, replacement, dependency invalidation, invalid-object catalogs and recompilation | Partial: tested procedure replacement after cached calls; Oracle implicit commits, dependencies and invalid-object lifecycle remain outstanding |
| 10. PL/SQL | Records, anchored types, cursors/refcursors, collections, parameter modes, control flow, dynamic SQL, exceptions and bulk operations | Partial: scoped blocks/declarations and nested handlers, standalone functions and version-12 procedures with OUT/IN OUT copyback, version-8 parameterized bound cursors with query-local formals/defaults and independent native scope checks, version-16 bounded EXECUTE IMMEDIATE with typed IN binds and scalar query INTO, declared-type procedural TRUNC and versioned logical DATE arithmetic; version-17 bounded AL16UTF16 NVARCHAR2/NCHAR routine variables and typed host values; broader national SQL/NLS semantics, full language and application routines outstanding |
| 11. Packages | Specs/bodies, visibility, overloads, initialization, session variables and invalidation with pool/session isolation | Outstanding |
| 12. Autonomous transactions | Independent commit/rollback and lock behavior, nested calls, exceptions and restart tests | Partial: [version-15 standalone routine transactions](evidence/oracle-autonomous-transactions-2026-11-03/README.md), separate parent/observer visibility, active-return errors, parent-lock deadlocks, snapshot boundaries, Describe safeguards and restart; child DDL, package/session-state integration, distributed commits and broader failure qualification outstanding |
| 13. Supplied package APIs | Qualify used DBMS/UTL APIs, including LOB, SQL, OUTPUT, UTILITY, APPLICATION_INFO, RANDOM, LOCK and SCHEDULER; file/network ACL enforcement | Outstanding |
| 14. Triggers | Row/statement/compound/INSTEAD OF/system triggers, order, transition state, mutating-table rules and transactional effects | Outstanding for Oracle; sample triggers explicitly deferred |
| 15. Session state | NLS, temporary tables, cursors, sequence visibility and cache behavior, pool reset, reconnect and isolation | Partial: sample sequences; broader Oracle state semantics unqualified |
| 16. Drivers and frameworks | PostgreSQL simple/extended/binary protocol, bindings, metadata and representative application/framework acceptance | Partial: tokio-postgres/psycopg HR acceptance and bounded procedure output/DATE bind metadata over PostgreSQL drivers; full application acceptance outstanding, Oracle Net/TTC conditional on unchanged-driver scope |
| 17. Catalogs and errors | USER_/ALL_/DBA_ objects, source, arguments, dependencies, compile errors, ORA diagnostics and SQLCODE/SQLERRM | Partial: version-14 application errors, bounded current diagnostics and PostgreSQL error metadata; broader error catalog, SQLERRM(code), EXCEPTION_INIT, backtraces and Oracle catalogs outstanding |
| 18. Security | Roles/grants, definer/invoker rights, context/VPD, auditing, encryption, external identity and key rotation under adversarial tests | Unqualified for Oracle semantics; native security does not establish compatibility |
| 19. Physical and specialized objects | Partition/index maintenance and materialized refresh; assess external tables, object/collection types, XML, spatial, text and queues for target workloads | Unqualified; unsupported reference features need a full/appropriate Oracle edition |
| 20. Optimizer and performance | Statistics, pruning, indexes, joins, spill and hint policy; measured representative workload and peak/batch SLOs | Partial: typed-index join regression; no Oracle workload qualification |
| 21. Availability and distributed work | Fencing, replication, recovery, uncertain commits, session recovery, XA/JTA and defined distributed SQL/transaction behavior | Unqualified for Oracle workloads |
| 22. Backup and flashback | Backup/PITR, historical-query equivalence, retention, corruption handling, restore and disaster-recovery drills | Unqualified for Oracle workloads; native mechanisms require workload validation |
| 23. Schema and data migration | Dependency ordering, resumable import, exact value/LOB reconciliation, constraints, sequences and schema validation | Partial: adapted HR and exported application table/data import; resumable index, temporal and source-backed character declaration reconciliation; complete source-value checks; full schema, foreign-key timing/validation and cutover qualification outstanding |
| 24. Snapshot and CDC | Consistent source position, SCNs, transaction ordering, long transactions, deletes, LOBs, DDL, gaps and replay | Outstanding; source capture tooling and Oracle privileges must be explicit |
| 25. Cutover and rollback | Write fencing, catch-up, reconciliation, go/no-go criteria, reverse migration and rehearsed rollback | Outstanding |
| 26. Qualification and operations | Differential/property/application testing, release gates, tooling, documentation, maintenance/support policy and operational runbooks | Partial: local regression suites and reference harness; broad qualification outstanding |

The [reference harness](../examples/oracle/reference/README.md) uses an actual
Oracle Free Lite database. Its measured version is recorded in each report.
Results on that edition cannot establish Oracle 19c, Enterprise Edition, RAC,
Data Pump or RMAN behavior. Those require the relevant reference environment.
Commercial application certification and licensed runtime dependencies such as
Forms, APEX, E-Business Suite and SAP are separate, conditional qualification
items; they must never be inferred from SQL parser acceptance.

Programme completion requires implemented behavior and executable evidence for
every required workstream, resolved migration blockers for the qualified target
workloads, PostgreSQL regression coverage, and tested cutover/recovery procedures.
External or conditional items must have explicit scope and evidence. A source
commit, sample import or result-match count alone does not satisfy that gate.
