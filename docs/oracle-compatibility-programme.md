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

The [persistent BLOB application checkpoint](evidence/oracle-persistent-blob-application-2026-11-05/README.md)
records the newer 53/53 original persistence-class result on an isolated clone,
source-backed reconciliation of 16 BLOB columns, and PostgreSQL JDBC catalog
metadata interoperability. Historical checkpoint counts below retain their
original source/test scope. This newer result does not qualify the whole
application or update the accepted running deployment.

The [temporal-binding checkpoint](evidence/sql-temporal-bindings-2026-11-04/README.md)
preserves typed temporal parameters and column comparison semantics across
queries, mutations and projections, including explicit spaced UTC offsets.
Independent Oracle and PostgreSQL controls pass 21/21 each. The original
optimistic-locking test passes over PostgreSQL transport; the persistence class
passes 52/53 and the BOM test 1/1. The remaining Oracle unique-error translation
failure, LOB support, broader datetime/NLS behavior and full application
acceptance remain open.

The [lookup-binding checkpoint](evidence/sql-join-key-bindings-2026-11-04/README.md)
defers unavailable join inputs before primary/secondary index probes and keeps
residual mutation predicates for unbound or repeated key equalities. Independent
references pass 23/23 join and 12/12 mutation controls in each profile. The
original Oracle-provider BOM test passes over PostgreSQL transport with rollback
reconciliation on a disposable copy. Full application and programme acceptance
remain incomplete.

The [connection-isolation checkpoint](evidence/session-transaction-defaults-2026-11-04/README.md)
separates future connection defaults from active transaction modes, preserves
transaction boundaries after batch setup statements, and checks Oracle-profile
stale snapshot writes at the statement boundary. Independent behavior references
pass 11/11 Oracle and 8/8 PostgreSQL controls. Native PostgreSQL SERIALIZABLE,
broader Oracle ALTER SESSION and full isolation conformance remain unsupported
or unqualified as described in that checkpoint.

The [project issue checkpoint](evidence/idempiere-project-issue-2026-11-04/README.md)
completes, posts and reverses a work-order stock issue through the original UI.
All 77 selected observations pass, including the prior 67. Stock totals and
project balance are restored; ledger entries offset and average-cost rounding
matches upstream rules and independently checked currency precision. Original
definitions and saved report rows are preserved. Broader project operations,
full costing coverage and the entire application remain unqualified.

The [row-valued UPDATE checkpoint](evidence/sql-row-update-2026-11-04/README.md)
adds selected constructor/subquery assignments with source cardinality checks,
original-row values and rollback controls: 18/18 independent expectations in
each profile. DEFAULT/generated columns, broader statement snapshots, exact
Oracle error identities and full UPDATE conformance remain unqualified.

The [NVL2 checkpoint](evidence/oracle-nvl2-2026-11-04/README.md) adds bounded
Oracle result typing and eager evaluation, including declared national literal
identity, with 59/59 independent Oracle and 59/59 PostgreSQL controls. Compiler
contract 27 preserves previous contract 26 and its language OID. Broader result
types, NLS, descriptor widths and full NVL2 conformance remain unqualified.

The [production checkpoint](evidence/idempiere-production-2026-11-04/README.md)
completes and reverses one single-product BOM build through the original UI.
Stock totals return to their initial values; both accounting schemas balance
and all eight product/account groups net to zero. All 67 selected observations
pass, including the prior 57, with definitions and saved reports preserved.
Production plans, disassembly, broader manufacturing and the full application
remain unqualified.

The [physical inventory checkpoint](evidence/idempiere-physical-inventory-2026-11-04/README.md)
completes one count-list generation, inventory adjustment, posting and correction
reversal through the original Oracle-schema application's UI. All 57 selected
observations pass, including the prior 46 financial/reporting checks. Stock and
per-account balances return to their prior values; all original definitions,
view metadata and 141 saved report rows are preserved. The subsequent
[logical primary-key checkpoint](evidence/sql-primary-key-extrema-2026-11-04/README.md)
fixes the numeric MIN/MAX and range decoding gap found by verification tooling:
20/20 independently satisfied Oracle controls and 20/20 PostgreSQL controls.
The entire application and broader programme remain incomplete.

The [matched-update MERGE checkpoint](evidence/sql-merge-matched-update-2026-11-04/README.md)
adds one matched UPDATE action with independent Oracle/PostgreSQL cardinality
rules, action filters, bound parameters and rollback controls: 15/15 reference
cases in each profile. A general row-binding fix preserves distinct quoted
column identities. Multi-action MERGE, target column-level read authorization,
concurrency and complete MERGE/identifier conformance remain outstanding.

The [Trial Balance checkpoint](evidence/idempiere-trial-balance-2026-11-04/README.md)
completes the original HQ reporting workflow through the UI: 47 stored and
rendered rows, including 19 opening balances and 28 current-period entries,
with a verified 12-page PDF. All 46 financial/reporting observations pass after
the clean runtime upgrade. An additional all-organization report also renders
its expected 47 rows. This remains one bounded report and selected business
workflows; the full application and broader programme remain incomplete.

The [typed BETWEEN checkpoint](evidence/sql-typed-between-2026-11-04/README.md)
retains datetime operand types at range boundaries. It preserves 16/16 explicit
Oracle controls and improves PostgreSQL comparisons from 4/16 to 14/16. Two
pre-existing PostgreSQL volatile-evaluation differences were unqualified at
that checkpoint. The subsequent [evaluation-context checkpoint](evidence/sql-between-evaluation-2026-11-04/README.md)
closes both, retaining 16/16 typed-boundary cases in each profile, and passes
19 Oracle / 20 PostgreSQL SQL/procedural evaluation cases. The separate
application checkpoint above verifies the selected rendering workflow. The
[subsequent clean upgrade](evidence/sql-between-evaluation-2026-11-04/README.md#subsequent-application-upgrade)
preserves all 46 observations, 62 routine and 160 view fingerprints, view
metadata and all 141 saved report rows; the upgraded UI opens Physical Inventory.

The [RAW conversion checkpoint](evidence/oracle-raw-character-conversion-2026-11-04/README.md)
corrects implicit hexadecimal character conversion for proven RAW intrinsic
operands and binary result metadata over prepared PostgreSQL queries. Its
independent references pass 13/13 Oracle and 13/13 PostgreSQL cases. Broader RAW
column/LOB semantics and the subsequent application reporting workflow remain
unqualified by those expression tests.

The latest completed application checkpoint runs the original Oracle-schema
application through PostgreSQL drivers, with 62 routine definitions and 160 view
definitions/type families loaded. The
[shipment posting recovery](evidence/parenthesized-query-metadata-2026-11-04/README.md#subsequent-application-posting-recovery)
completes and posts a real shipment through the UI, with balanced entries in two
accounting schemas and one stock movement. Subsequent
[scalar-subquery defaults](evidence/scalar-subquery-defaults-2026-11-04/README.md)
and [numeric pictures](evidence/numeric-format-pictures-2026-11-04/README.md)
have independent Oracle/PostgreSQL reference evidence. The subsequent
[invoice checkpoint](evidence/idempiere-invoice-posting-2026-11-04/README.md)
generates, completes and posts one invoice through the standard UI, with balanced
entries in both accounting schemas and no repeated stock movement. The
[payment/allocation checkpoint](evidence/idempiere-payment-allocation-2026-11-04/README.md)
completes and posts the receipt and allocation, marks that invoice paid and
verifies zero remaining balances, with balanced accounting in both schemas.
The [bank checkpoint](evidence/idempiere-bank-reconciliation-2026-11-04/README.md)
then reconciles the receipt, completes/posts one statement and verifies its
bank balance and balanced accounting. The selected
[purchase order](evidence/idempiere-purchase-order-2026-11-04/README.md) and
[material receipt](evidence/idempiere-material-receipt-2026-11-04/README.md)
then complete through the original UI, with one +3 movement, purchase matching
and balanced receipt accounting. The
[supplier invoice](evidence/idempiere-supplier-invoice-2026-11-04/README.md)
then completes/posts from that receipt, with posted invoice/receipt and purchase
matching, balanced accounting, and preserved stock and prior sales/bank state.
The [supplier payment](evidence/idempiere-supplier-payment-2026-11-04/README.md)
then completes/posts with allocation and zero open balances, followed by
[bank reconciliation](evidence/idempiere-supplier-reconciliation-2026-11-04/README.md)
with balanced accounting and a bank balance of 87.49. Remaining application
workflows and the broader programme remain unqualified.
The [prepared-request checkpoint](evidence/prepared-request-boundaries-2026-11-04/README.md)
subsequently qualifies single-statement Parse requests and transaction recovery
against PostgreSQL in both profiles, retaining independent Oracle/PostgreSQL
set-operand comparisons. Its subsequent local runtime upgrade preserves both
settled business flows and all 62 routine/160 view definitions across a clean
restart, with 27 preservation gates, 25 workflow checks and fresh login/navigation.
The subsequent [inventory transfer](evidence/idempiere-inventory-transfer-2026-11-04/README.md)
completes/posts two units from HQ to HQ Transit through the standard UI, with
paired stock transactions, conserved product quantity, balanced accounting and
36 replayed transfer/preservation checks. Its subsequent
[Reverse Correct](evidence/idempiere-inventory-reversal-2026-11-04/README.md)
creates the linked posted reversal, restores stock positions and nets accounting
and quantity to zero in each locator/account group. Other reversals, reporting
and remaining application workflows remain unqualified.
The checkpoint history below records what was measured at each earlier stage.

The separate [`oracle-preview-v1`](oracle-preview-profile.md) profile is now used
for [iDempiere seed development](evidence/idempiere-seed-2026-11-01/README.md).
The latest [function-attribute checkpoint](evidence/oracle-function-attributes-2026-11-03/README.md)
accepts **62/62 unchanged routine definitions** through PostgreSQL drivers.
The [SQL collection checkpoint](evidence/oracle-nested-table-sql-2026-11-03/README.md)
adds registered nested-table declarations and constructors. The subsequent
[dense procedural collection checkpoint](evidence/oracle-dense-collections-2026-11-03/README.md)
adds version-19 collection variables, methods and typed function returns. The
[text-pattern checkpoint](evidence/oracle-text-patterns-2026-11-03/README.md)
qualifies version-20 TRIM/regex semantics and five source-compared calls to the
original CSV collection function. The [multiset checkpoint](evidence/oracle-multisets-2026-11-03/README.md)
adds version-21 named nested-table operators and empty predicates. The original
intersection function now matches 11/11 read-only source calls; the CSV function
retains 5/5. Dictionary rows, constraints, complete application execution and
business workflows remain incomplete. The [session-context checkpoint](evidence/oracle-session-context-2026-11-03/README.md)
adds bounded version-22 USERENV inspection and immutable connection facts. The
[startup expression checkpoint](evidence/oracle-startup-expressions-2026-11-03/README.md)
adds shallow SQL concatenation and version-23 DML date provenance. One development
candidate reached the admin desktop; reliable login, navigation and business
workflows remain unqualified.
The [number-format checkpoint](evidence/oracle-number-text-formats-2026-11-03/README.md)
adds version-24 text-minimum numeric TO_CHAR behavior with 55/55 bounded Oracle
comparisons. Native formatting retains its baseline, including three explicitly
tracked pre-existing PostgreSQL parity gaps. This does not qualify full application
execution or close the other programme workstreams.
The [blocking row-lock checkpoint](evidence/sql-row-lock-waits-2026-11-03/README.md)
addresses premature conflicts in SELECT FOR UPDATE with cancellation and live
row-cycle detection in both profiles. Broader locking and application qualification
remain open.
The [view-predicate checkpoint](evidence/sql-view-predicates-2026-11-03/README.md)
adds bounded predicate propagation through ordinary view projections, with separate
Oracle/PostgreSQL comparisons and exact reconciliation of the selected original
application field view. This improves a measured materialization bottleneck;
full optimizer and application qualification remain open.
The [draft-order observation](evidence/idempiere-sales-order-draft-2026-11-03/README.md)
adds an existing-order read matching Oracle and a saved draft header verified by
another database connection and a fresh browser session. Lines, processing and
accounting remain unqualified. A separate
[output-buffer baseline](evidence/oracle-output-buffer-baseline-2026-11-03/README.md)
qualifies 27 expectations on Oracle; all 27 target cases are blocked at the
missing DISABLE call. It defines package implementation dependencies without
claiming supplied-package support.
The [version-25 output checkpoint](evidence/oracle-output-buffers-2026-11-03/README.md)
adds six supplied operations with connection-local nontransactional state and
retains native connection-memory admission behavior. It matches 32/38 current
source expectations; six GET_LINES cases and the other package work remain open.
The [aggregate checkpoint](evidence/sql-aggregate-conditionals-2026-11-03/README.md)
fixes the reproduced aggregate NVL typing gap, with 19/19 Oracle and 12/12 native
PostgreSQL source comparisons. Four unchanged inventory calls now execute, but
only two return the correct values. A generic SQL column/local-variable shadowing
reproduction isolates the next name-resolution dependency. Line entry and the
full application remain unqualified; 62 accepted definitions are not 62 correct
routine executions.
The [first saved line](evidence/idempiere-sales-order-line-2026-11-03/README.md)
checkpoint subsequently verifies product selection and a committed order line
through independent SQL and a fresh browser. Its parent totals remain zero;
header/tax propagation, processing and the complete order workflow are unqualified.
The [column-binding checkpoint](evidence/sql-column-binding-2026-11-03/README.md)
fixes the physical-alias/local-value capture: all four selected unchanged
inventory calls now match Oracle, with separate Oracle and native source controls.
It retains a correlated numeric result-metadata failure and does not qualify all
62 routines, header/tax propagation or the complete application.
The [scalar result-type checkpoint](evidence/sql-scalar-subquery-types-2026-11-03/README.md)
subsequently matches twelve Oracle and twelve native value/type cases through
prepared text/binary results. It also repairs concurrent diagnostic log framing.
A generic pending-write/index regression now reproduces the zero-total pattern;
that transaction-visibility dependency and the complete application remain open.
The [pending typed-index checkpoint](evidence/sql-pending-index-visibility-2026-11-03/README.md)
subsequently corrects that generic visibility failure using the committed key
projection. Oracle and native PostgreSQL controls each match 24/24. The actual
application's save/totals workflow still requires independent requalification;
the general transaction programme and full application remain open.
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
The [hierarchy execution qualification](evidence/oracle-hierarchy-execution-2026-11-03/README.md)
advances this subset to 19/19 reference matches, including nested scopes and
wildcards. Saved hierarchy views and routine cursors survive reopen, numeric
driver metadata is preserved, and older readers refuse the new format-4 profile.
Definition acceptance remains 61/62; DETERMINISTIC is now the first rejected
declaration in the remaining routine. Collections, regular expressions and full
application execution remain open.
The [function-attribute qualification](evidence/oracle-function-attributes-2026-11-03/README.md)
maps DETERMINISTIC declarations to native IMMUTABLE, preserves declared routine
volatility metadata and advances definition acceptance to 62/62. Ten Oracle and
four independent PostgreSQL cases match, and six reader checks pass. Five
read-only original collection calls qualify on Oracle but remain 0/5 on BicDB
because the collection result type is not registered. The 1/14 NULL-input
procedure diagnostics and full application qualification remain unchanged.
The [routine security qualification](evidence/oracle-routine-security-2026-11-03/README.md)
derives authority from declaration attributes and corrects stale cached flags
on read. Five native permission cases, six process-reopen checks and independent
Oracle value regressions pass. Qualified user schemas retain normal execute
permissions while actual administrative builtins remain role-restricted.
Definition and application-execution counts are unchanged.
The [full character-copy verification](evidence/oracle-character-full-copy-2026-11-03/README.md)
completes reconciliation of all 910 affected tables and 7,150 declarations.
A byte-identical disk copy passes full 915-table/185,537-row fingerprints and
declaration checks on two independent opens. The later
[connection-lifecycle verification](evidence/oracle-character-reconnect-2026-11-03/README.md)
identifies an idle control connection in the repeated trial. Fresh-connection
and process-reopen verification preserve all 915 tables/185,537 rows and validate
all 7,150 declarations after the completed migration. Host-crash durability and
application workflows remain unqualified.
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
| 4. Types and conversions | Boundary/property tests for NUMBER, byte/character lengths, national types, DATE, timestamps/time zones, intervals, RAW and LOBs | Partial: native mappings, bounded NUMBER arithmetic/reference infinity transport, and durable DATE/TIMESTAMP column provenance with storage/anchor/reference checks; 2,263-column application temporal backfill and complete target-value preservation verified; durable BYTE/CHAR columns with STANDARD capacity and importer provenance checks; legacy character reconciliation, timezone regions and full conversion rules unqualified; [BLOB declaration provenance and bounded HEXTORAW conversion](evidence/oracle-blob-types-2026-11-05/README.md), selected transaction-aware APPEND/locator execution and [source-backed column reconciliation](evidence/oracle-persistent-blob-application-2026-11-05/README.md); complete LOB/DDL lifecycle still open |
| 5. Scalar and analytic functions | Oracle type selection, short-circuiting, NLS, date arithmetic, conversion, aggregation and analytic behavior | Partial: bounded NVL, date formats and Gregorian ADD_MONTHS; version-9 bounded DECODE, version-10 AL32UTF8 CHR/default TO_CHAR, and version-11 eager typed NVL with timestamp precision; bounded [logical primary-key extrema/ranges](evidence/sql-primary-key-extrema-2026-11-04/README.md) with independent Oracle/PostgreSQL references; version-27 bounded [NVL2 and national literal provenance](evidence/oracle-nvl2-2026-11-04/README.md); PL/SQL constant propagation, broader NVL2 types, scalar/NLS and analytic semantics outstanding |
| 6. SQL and DML constructs | DUAL, binds, alternative quoting, legacy joins, ROWNUM, hierarchy, MERGE, multitable INSERT, RETURNING, PIVOT/UNPIVOT | Partial: DUAL, pgwire parameters and literal-integer ROWNUM prefix predicates in plain blocks; bounded [row-valued UPDATE](evidence/sql-row-update-2026-11-04/README.md); [single matched-update MERGE](evidence/sql-merge-matched-update-2026-11-04/README.md) with separate Oracle/PostgreSQL duplicate-match rules, filters and statement rollback; multi-action MERGE and other Oracle constructs unqualified; assess MODEL and MATCH_RECOGNIZE per workload |
| 7. Names and schema DDL | Quoted/unquoted resolution, cross-schema objects, synonyms, current schema and an explicit physical-clause migration policy | Partial: application-schema selection, versioned static function/procedure relation binding, CALL namespaces and documented heap adaptation; general resolution and synonyms outstanding |
| 8. Transactions and locking | Oracle statement rollback and error continuation, isolation, waits, locks, deadlocks, cancellation and read-only behavior | Partial: [nested-block contracts](evidence/oracle-nested-block-contracts-2026-11-02/README.md) distinguish new Oracle routine statement rollback from native PostgreSQL block rollback; [blocking locking reads and protocol progress](evidence/sql-row-lock-waits-2026-11-03/README.md) have separate Oracle/PostgreSQL comparisons; general transactions, DDL commits and broader locking qualification outstanding |
| 9. DDL lifecycle | Oracle implicit commits, replacement, dependency invalidation, invalid-object catalogs and recompilation | Partial: tested procedure replacement after cached calls; Oracle implicit commits, dependencies and invalid-object lifecycle remain outstanding |
| 10. PL/SQL | Records, anchored types, cursors/refcursors, collections, parameter modes, control flow, dynamic SQL, exceptions and bulk operations | Partial: scoped blocks/declarations and nested handlers, standalone functions and version-12 procedures with OUT/IN OUT copyback, version-8 parameterized bound cursors with query-local formals/defaults and independent native scope checks, version-16 bounded EXECUTE IMMEDIATE with typed IN binds and scalar query INTO, declared-type procedural TRUNC and versioned logical DATE arithmetic; version-17 bounded AL16UTF16 NVARCHAR2/NCHAR routine variables and typed host values; version-19 dense named nested-table constructors, COUNT/EXTEND, element access and registered routine types; version-20 bounded TRIM/regex and typed character conversion; version-21 bounded named nested-table multiset operators and empty predicates; sparse/bulk collections, broader national SQL/NLS semantics, full language and application routines outstanding |
| 11. Packages | Specs/bodies, visibility, overloads, initialization, session variables and invalidation with pool/session isolation | Outstanding |
| 12. Autonomous transactions | Independent commit/rollback and lock behavior, nested calls, exceptions and restart tests | Partial: [version-15 standalone routine transactions](evidence/oracle-autonomous-transactions-2026-11-03/README.md), separate parent/observer visibility, active-return errors, parent-lock deadlocks, snapshot boundaries, Describe safeguards and restart; child DDL, package/session-state integration, distributed commits and broader failure qualification outstanding |
| 13. Supplied package APIs | Qualify used DBMS/UTL APIs, including LOB, SQL, OUTPUT, UTILITY, APPLICATION_INFO, RANDOM, LOCK and SCHEDULER; file/network ACL enforcement | Partial: [version-25 output operations](evidence/oracle-output-buffers-2026-11-03/README.md) match 32/38 source expectations; six GET_LINES cases, package types/ACLs, further conversions and the other DBMS/UTL APIs remain open |
| 14. Triggers | Row/statement/compound/INSTEAD OF/system triggers, order, transition state, mutating-table rules and transactional effects | Outstanding for Oracle; sample triggers explicitly deferred |
| 15. Session state | NLS, temporary tables, cursors, sequence visibility and cache behavior, pool reset, reconnect and isolation | Partial: sample sequences; broader Oracle state semantics unqualified |
| 16. Drivers and frameworks | PostgreSQL simple/extended/binary protocol, bindings, metadata and representative application/framework acceptance | Partial: tokio-postgres/psycopg HR acceptance and bounded procedure output/DATE bind metadata over PostgreSQL drivers; explicit [PostgreSQL JDBC adapter](evidence/oracle-jdbc-blocks-2026-11-05/README.md) has a newer [124-check metadata/input record](evidence/oracle-persistent-blob-application-2026-11-05/README.md); callable OUT bindings, complete LOB APIs and full application acceptance outstanding, Oracle Net/TTC conditional on unchanged-driver scope |
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

The [saved draft totals checkpoint](evidence/idempiere-sales-order-totals-2026-11-03/README.md)
subsequently verifies a real UI line edit, committed header totals of 123.50 and
a fresh-browser reload. It covers the selected zero-rate sample tax only; order
completion and the full application remain unqualified.

The subsequent [order-processing and bounded lock-wait baseline](evidence/oracle-row-lock-deadlines-baseline-2026-11-03/README.md)
records the actual Complete attempt failing at unsupported `FOR UPDATE WAIT n`.
The order remains draft. Four of five source expectations qualify; the target
matches zero, and timed reference cancellation remains separately unqualified.
Deadline syntax/execution and the full application workflow remain open.

The [bounded row-wait checkpoint](evidence/oracle-row-lock-deadlines-2026-11-03/README.md)
adds per-row integer-second `FOR UPDATE WAIT n` through PostgreSQL-driver prepared
paths, with independent source comparisons and native lock controls. Four of five
deadline comparisons and both multirow cases match; timed source cancellation
remains unqualified. A pre-existing DDL/BEGIN batch-boundary gap is retained.
The actual application Complete action and the full programme remain open.

The [statement-clock checkpoint](evidence/oracle-current-time-2026-11-03/README.md)
subsequently matches 75 Oracle current-time/alias cases and 12 independent native
PostgreSQL controls. It provides bounded precision, typed prepared results and
Oracle statement-time advancement while preserving native transaction time.
Compiler version 26 retains version 25 and durable metadata format 5; 11 reader
checks cover the boundary. This addresses a NULL timestamp observed during
application order processing, but a standalone comparison does not qualify that
workflow. Complete clock/temporal semantics, application operation, migration,
cutover and all outstanding workstreams above remain required.

The [first committed sales-order completion](evidence/idempiere-sales-order-complete-2026-11-03/README.md)
subsequently verifies the original UI Complete action, committed status CO,
two-unit reservation, totals of 123.50, a fresh browser session and a graceful
database reopen. It does not qualify the entire application. Background matching
record decoding and growing database memory remain active investigations; the
isolated application was intentionally stopped to preserve host headroom.

The [native type-relocation correction](evidence/sql-type-relocation-2026-11-03/README.md)
addresses a separately reproduced main CI failure while retaining Oracle
custom-type regressions and source-backed clock controls. It qualifies native
function declaration/type identity updates, not Oracle schema-move syntax or
schema removal. All outstanding workstreams in the programme remain required.

The [logical wildcard projection checkpoint](evidence/join-wildcard-projection-2026-11-03/README.md)
qualifies 23 Oracle and 23 native PostgreSQL join/projection cases, preserving
logical source order, typed row slots and distinct repeated-label ordinals through
physical join reordering. The stopped application copy's matching query now reads
81 correct columns with ordinary driver decoding, and the completed order and
reservation survive reopen. Oracle's declared-column joined-wildcard view form
remains unqualified after a retained source-side ORA-25716 observation; its final
view controls use explicit aliases. Full background execution, legacy view metadata,
memory behavior, remaining application workflows and the wider programme remain open.

The [wildcard Describe checkpoint](evidence/wildcard-describe-2026-11-04/README.md)
adds metadata-only preparation of fully typed wildcard projections, preserving
quoted result labels independently of folded type-binding names. Nine Oracle and
nine native PostgreSQL preparation controls match, including sequence invariance;
executed join controls retain 23/23 in each profile. A stopped application-copy
metadata check matches 127/160 source view descriptions: 32 views are absent and
one has different labels. A general ANSI character-type parser gap is reproduced
separately. Missing view definitions, their values, full application operation,
memory behavior and the wider compatibility programme remain open.

The [ordinary view definition checkpoint](evidence/oracle-view-definitions-2026-11-04/README.md)
adds ANSI varying-character type phrases, metadata-only UNION view creation and
joined scalar-subquery result typing. Three independent fixtures match 18 Oracle
and 18 native PostgreSQL cases; native ANSI syntax retains its previous 6/6
controls. A dependency-ordered application-copy rebuild accepts all 160 ordinary
views and matches exact output labels/order and transport families for 11,983
columns. The retained 159/160 type baseline exposed three numeric fields described
as text; the general scalar type fix resolves them. Compiler 26 and metadata format
5 remain unchanged, with 24 new/prior/new reader checks across both profiles.
Full view values/type modifiers, remaining application workflows and the wider
compatibility programme remain open.

## Constraint identity and logical names checkpoint

The [constraint diagnostic qualification](evidence/oracle-constraint-diagnostics-2026-11-04/README.md)
carries resolved schema/table/constraint identity through write failures and
distinguishes row writes, primary-key validation, unique-constraint validation
and unique-index builds. Selected Oracle preview diagnostics preserve SQLSTATE
23505 over PostgreSQL transport; default PostgreSQL messages remain unchanged.
Quoted public table/index names use safe storage identities, and upgrade controls
preserve old public view OIDs and positive/negative grants. Independent Oracle
and PostgreSQL references each pass 15 cases. The selected original application
foundation advances to 168/170 tests; LOB block execution, the savepoint test
assertion and broader full-application acceptance remain open. This checkpoint
does not complete any broader Oracle conformance or production qualification.

## Recursive query analysis checkpoint

The [recursive query qualification](evidence/sql-recursive-query-stack-2026-11-04/README.md)
records a stack overflow exposed by subsequent browser qualification and a
neutral correlated-query reproducer. The type resolver and lock-policy scanner
carry boxed diagnostics through recursive AST visitors, preserving the ordinary
2 MiB worker stack and structured error behavior. Both profile controls pass;
independent Oracle and PostgreSQL source controls and the corrected protocol
server agree on ownership and private-access exclusions. All 1,118 selected
engine regressions and 13 retained reference families pass. The failed runtime
was rejected and the previous runtime restored with 77 workflow observations
preserved; full application qualification remains a separate gate.

## Anonymous input and statement recovery checkpoint

The [anonymous input checkpoint](evidence/oracle-anonymous-inputs-2026-11-05/README.md)
preserves input bindings, declaration/SQL type inference and namespace selection
in preview anonymous blocks over PostgreSQL extended protocol. Eight native
Oracle controls qualify bounded caller rollback, continuation and explicit commit
behavior. Default PostgreSQL failed-transaction behavior and ordinary lock-error
paths retain independent regression coverage. The source checkpoint passes 993
selected Rust regressions and 13 retained reference families. JDBC application
integration, LOB locators, Parse/Bind continuation, broader procedural semantics
and complete application/programme acceptance remain open.


## JDBC anonymous block checkpoint

The [JDBC adapter qualification](evidence/oracle-jdbc-blocks-2026-11-05/README.md)
records 112 integration checks, ten Java unit tests and four generated-provider
compilation tests. The explicit adapter preserves input-only anonymous submissions
over PostgreSQL JDBC; ordinary PostgreSQL connections retain their own driver.
A separately staged original application provider loads the adapter and passes
19 of 20 tests in the selected original PO insert class. The remaining failure
reaches unsupported `EMPTY_BLOB()` execution, exposing the next server-semantic
gap after transport repair. Oracle LOB locators, callable OUT bindings, broader
JDBC APIs and complete application/programme acceptance remain open. This client
checkpoint does not replace the accepted running application or qualify a release.


## BLOB declaration and hexadecimal input checkpoint

The [BLOB/RAW prerequisite checkpoint](evidence/oracle-blob-types-2026-11-05/README.md)
retains logical BLOB column declarations in a durable reader-fenced format while
preserving native PostgreSQL binary columns. HEXTORAW now uses typed character,
NUMBER and RAW conversion, including binary protocol binds, with all 24 native
Oracle comparisons matching. Stored routine contract 28 preserves prior contract
identities. The combined source passes 209 selected Oracle unit regressions,
45 Oracle protocol tests, 174 general protocol tests, binary storage, ordinary
worker-stack and routine-privilege controls. Persistent and temporary locator
semantics, BLOB expression/view provenance, complete LOB APIs and full application
qualification remain open. The accepted running application was not replaced.


## Routine-local BLOB checkpoint

The [routine-local BLOB checkpoint](evidence/oracle-temporary-blobs-2026-11-05/README.md)
adds compiler contract 29 for logical BLOB declarations and selected temporary
DBMS_LOB operations. Prior compiler identities and PostgreSQL binary value
semantics remain independent. Its reference fixture covers 18 observations,
including copy-on-write assignment and SQL/PLSQL error-context differences.
Persistent row-backed locators, the original application's full LOB test,
complete application acceptance and the remaining programme workstreams stay
open. This checkpoint does not replace the accepted running application.


## Transaction receipt prerequisite

[Native outcome/write receipts](transaction-outcome-receipts.md) now preserve
transaction outcome and write survival across savepoints, reused log positions,
commit, abort and post-decision recovery failures. The
[qualification checkpoint](evidence/transaction-write-receipts-2026-11-05/README.md)
records native, SQL, protocol and actual WAL append-failure checks. These are
building blocks for persistent LOB snapshot semantics. Persistent LOB storage and
mutation, source-type reconciliation and complete application acceptance remain
open; this checkpoint does not change the accepted application deployment.

## Persistent BLOB ownership source contract

The [operation-specific ownership observations](evidence/oracle-persistent-blob-ownership-2026-11-05/README.md)
separate old-locator reads, SQL value copies and APPEND after deletion,
replacement, primary-key movement, commit and rollback. Eight cases repeated
twice on the native reference resolve the previously ambiguous combined
DELETE/INSERT/APPEND failure: copying succeeds before commit, while APPEND raises
ORA-22275. Uncommitted replacement instead raises ORA-22922 for APPEND while
retaining readable/copyable old bytes. Committing either invalidation also
invalidates the observed old locator's reads and copies. These are implementation
requirements, not delivered persistent-LOB support or application qualification.

## Persistent BLOB read-version infrastructure

[Origin-local read histories](oracle-lob-read-versions.md) use transaction/write
receipts to preserve copied snapshots, rollback prefixes, acquisition modes and
origin commit outcomes across short-lived SQL sessions. The registry is lazy,
weakly held and distinct across nested transaction contexts. Completed histories
release obsolete change versions. The
[source acquisition probe](evidence/oracle-lob-read-versions-2026-11-05/README.md)
records further requirements for read modes and later-transaction locator reuse.
Durable value identity, current-owner resolution, stored-column binding,
authorized row-backed mutation, legacy source-type reconciliation and complete
application acceptance remain open; this prerequisite does not implement those
operations or replace the accepted application deployment.

## Persistent BLOB storage integration

[Identity-bearing cells and transactional owner metadata](oracle-blob-storage.md)
add a storage layer beneath future persistent locators. New Oracle-preview BLOB
columns use reader-fenced metadata format 7; PostgreSQL BYTEA remains native.
SQL copies and replacements receive independent identities, while unchanged
values survive primary-key moves. Owner changes share native row transactions.
This does not connect stored values to the read-version layer or implement
persistent APPEND. DDL lifecycle coverage, legacy source-manifest reconciliation,
full application acceptance and the remaining programme work stay open.

## Selected persistent BLOB execution

Static SELECT INTO and RETURNING now bind stored BLOB values to origin-local
read histories. APPEND resolves current ownership, checks current SQL rights,
row policy, database instance and held locks, and writes through native
transactions without ordinary UPDATE triggers. Later-transaction reuse restores
the current owner's baseline on rollback. Explicit binary-column reconciliation
is bounded to unpartitioned tables outside active transactions. See the
[BLOB contract](oracle-blob-storage.md) for remaining DDL, acquisition, package,
index and migration limits. Full application and programme acceptance remain
open; a local regression run does not qualify a deployed application.

## PostgreSQL JDBC field-origin interoperability

Selected qualified PostgreSQL catalog SELECT statements retain their native
syntax on Oracle-profile connections. This lets stock JDBC resolve base-column
metadata without interpreting PostgreSQL system names as Oracle application
identifiers. Parsed relation boundaries protect application SQL and literal
contents. Independent native/default PostgreSQL checks and real JDBC metadata
checks accompany the change; complete driver, application and programme
qualification remain separate gates.

## Source BLOB reconciliation and original application class

The [application checkpoint](evidence/oracle-persistent-blob-application-2026-11-05/README.md)
records explicit reconciliation of all 16 source-manifest BLOB columns on a clean
isolated clone, preservation of exported binary values and original routine/view
definitions, and a 53/53 original `POTest` class result. PostgreSQL JDBC base-column
metadata checks accompany the general catalog-read boundary. This is selected
application evidence; the accepted application still uses its earlier binary,
and full application, broader original tests and programme acceptance remain open.
