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
2,263-column backfill is in progress, not complete. All 705 retained generated
Oracle cases plus 21 new column cases match, alongside 110 native PostgreSQL
controls. The remaining 18 definitions, foreign keys and full application
workflows remain migration blockers.
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
| 4. Types and conversions | Boundary/property tests for NUMBER, byte/character lengths, national types, DATE, timestamps/time zones, intervals, RAW and LOBs | Partial: native mappings, bounded NUMBER arithmetic/reference infinity transport, and durable DATE/TIMESTAMP column provenance with storage/anchor/reference checks; legacy application backfill in progress; timezone regions and full conversion rules unqualified |
| 5. Scalar and analytic functions | Oracle type selection, short-circuiting, NLS, date arithmetic, conversion, aggregation and analytic behavior | Partial: bounded NVL, date formats and Gregorian ADD_MONTHS; version-9 bounded DECODE values, conversions and lazy evaluation; NVL2 and broader semantics outstanding |
| 6. SQL and DML constructs | DUAL, binds, alternative quoting, legacy joins, ROWNUM, hierarchy, MERGE, multitable INSERT, RETURNING, PIVOT/UNPIVOT | Partial: DUAL, pgwire parameters and literal-integer ROWNUM prefix predicates in plain blocks; other Oracle constructs unqualified; assess MODEL and MATCH_RECOGNIZE per workload |
| 7. Names and schema DDL | Quoted/unquoted resolution, cross-schema objects, synonyms, current schema and an explicit physical-clause migration policy | Partial: application-schema selection, versioned static function relation binding and documented heap adaptation; general resolution and synonyms outstanding |
| 8. Transactions and locking | Oracle statement rollback and error continuation, isolation, waits, locks, deadlocks, cancellation and read-only behavior | Partial: [nested-block contracts](evidence/oracle-nested-block-contracts-2026-11-02/README.md) distinguish new Oracle routine statement rollback from native PostgreSQL block rollback; general transactions, DDL commits and locking qualification outstanding |
| 9. DDL lifecycle | Oracle implicit commits, replacement, dependency invalidation, invalid-object catalogs and recompilation | Outstanding; native transactional DDL is not an Oracle equivalent |
| 10. PL/SQL | Records, anchored types, cursors/refcursors, collections, parameter modes, control flow, dynamic SQL, exceptions and bulk operations | Partial: scoped blocks/declarations and nested handlers, standalone functions, version-8 parameterized bound cursors with query-local formals/defaults and independent native scope checks, declared-type procedural TRUNC and versioned logical DATE arithmetic; full language and application routines outstanding |
| 11. Packages | Specs/bodies, visibility, overloads, initialization, session variables and invalidation with pool/session isolation | Outstanding |
| 12. Autonomous transactions | Independent commit/rollback and lock behavior, nested calls, exceptions and restart tests | Outstanding |
| 13. Supplied package APIs | Qualify used DBMS/UTL APIs, including LOB, SQL, OUTPUT, UTILITY, APPLICATION_INFO, RANDOM, LOCK and SCHEDULER; file/network ACL enforcement | Outstanding |
| 14. Triggers | Row/statement/compound/INSTEAD OF/system triggers, order, transition state, mutating-table rules and transactional effects | Outstanding for Oracle; sample triggers explicitly deferred |
| 15. Session state | NLS, temporary tables, cursors, sequence visibility and cache behavior, pool reset, reconnect and isolation | Partial: sample sequences; broader Oracle state semantics unqualified |
| 16. Drivers and frameworks | PostgreSQL simple/extended/binary protocol, bindings, metadata and representative application/framework acceptance | Partial: tokio-postgres and psycopg HR acceptance; Oracle Net/TTC conditional on unchanged-driver scope |
| 17. Catalogs and errors | USER_/ALL_/DBA_ objects, source, arguments, dependencies, compile errors, ORA diagnostics and SQLCODE/SQLERRM | Outstanding |
| 18. Security | Roles/grants, definer/invoker rights, context/VPD, auditing, encryption, external identity and key rotation under adversarial tests | Unqualified for Oracle semantics; native security does not establish compatibility |
| 19. Physical and specialized objects | Partition/index maintenance and materialized refresh; assess external tables, object/collection types, XML, spatial, text and queues for target workloads | Unqualified; unsupported reference features need a full/appropriate Oracle edition |
| 20. Optimizer and performance | Statistics, pruning, indexes, joins, spill and hint policy; measured representative workload and peak/batch SLOs | Partial: typed-index join regression; no Oracle workload qualification |
| 21. Availability and distributed work | Fencing, replication, recovery, uncertain commits, session recovery, XA/JTA and defined distributed SQL/transaction behavior | Unqualified for Oracle workloads |
| 22. Backup and flashback | Backup/PITR, historical-query equivalence, retention, corruption handling, restore and disaster-recovery drills | Unqualified for Oracle workloads; native mechanisms require workload validation |
| 23. Schema and data migration | Dependency ordering, resumable import, exact value/LOB reconciliation, constraints, sequences and schema validation | Partial: reproducible adapted HR import; general migration outstanding |
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
