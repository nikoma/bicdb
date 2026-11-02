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
Required foreign keys, complete routine/view behavior and remaining application
behavior are still outstanding.
These are development data baselines, not full schema/application acceptance.

“Partial” means a specific implementation or test exists, not that the full
workstream is compatible. “Unqualified” means Oracle behavior or operational
acceptance remains unproven, including when related native BicDB features exist.

| Workstream | Required acceptance | Current programme coverage |
| --- | --- | --- |
| 1. Versioned profiles | Durable profile identity, explicit startup selection, incompatible-open rejection, upgrade rules and PostgreSQL isolation | Partial: bounded HR and separate preview profiles, durable identity and versioned preview routine compiler; general upgrade qualification outstanding |
| 2. Migration assessment | Inventory source and runtime dependencies; report object locations, dynamic SQL, wrapped code, blockers and manual changes | Partial: [offline lexical inventory](oracle-migration-assessment.md); semantic/catalog/runtime assessment and dependency resolution outstanding |
| 3. SQL value semantics | Differential null/empty-string, concatenation, padding, comparison, coercion, uniqueness and composite-key tests | Partial: HR lowering and initial reference queries; coercion and padding qualification outstanding |
| 4. Types and conversions | Boundary/property tests for NUMBER, byte/character lengths, national types, DATE, timestamps/time zones, intervals, RAW and LOBs | Partial: native mappings used by HR; full Oracle rules unqualified |
| 5. Scalar and analytic functions | Oracle type selection, short-circuiting, NLS, date arithmetic, conversion, aggregation and analytic behavior | Partial: bounded NVL, date formats and Gregorian ADD_MONTHS; NVL2/DECODE and broader semantics outstanding |
| 6. SQL and DML constructs | DUAL, binds, alternative quoting, legacy joins, ROWNUM, hierarchy, MERGE, multitable INSERT, RETURNING, PIVOT/UNPIVOT | Partial: DUAL and pgwire parameters; other Oracle constructs unqualified; assess MODEL and MATCH_RECOGNIZE per workload |
| 7. Names and schema DDL | Quoted/unquoted resolution, cross-schema objects, synonyms, current schema and an explicit physical-clause migration policy | Partial: application-schema selection, versioned static function relation binding and documented heap adaptation; general resolution and synonyms outstanding |
| 8. Transactions and locking | Oracle statement rollback and error continuation, isolation, waits, locks, deadlocks, cancellation and read-only behavior | Unqualified: HR currently uses BicDB's PostgreSQL-shaped transaction contract |
| 9. DDL lifecycle | Oracle implicit commits, replacement, dependency invalidation, invalid-object catalogs and recompilation | Outstanding; native transactional DDL is not an Oracle equivalent |
| 10. PL/SQL | Records, anchored types, cursors/refcursors, collections, parameter modes, control flow, dynamic SQL, exceptions and bulk operations | Partial: bounded procedures/blocks, standalone functions, unparameterized bound cursor loops, declared-type procedural TRUNC and versioned logical DATE arithmetic; full language and application routines outstanding |
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
