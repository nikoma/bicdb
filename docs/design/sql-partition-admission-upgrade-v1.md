# SQL partition admission and upgrade v1

Status: implementation in progress on the private partition branch. This is a
logical SQL compatibility transition, not P2 storage-format promotion, a portable
namespace receipt, distributed placement, or HA qualification.

## Observed inputs

The genuine merged `1.0.482-beta` CLI at source
`f43583da4c34209e810c5e6a8f5925831b2949a1` created six immutable stores on S3:
`/root/bicdb-durability-s3/partition-old-store-fixtures-482-20261112`.
Both storage modes cover old parent-heap rows with native indexes, an acknowledged
out-of-bound child row, and independently stored child policies. The external
ledger has fourteen acknowledged rows. Candidate runs use copies and recheck
every original file's size and SHA256.

The unchanged partition runtime at `97aabad9`, with probe-only source `1f6a25ed`,
fails all six required upgrade cases. An upsert creates a second logical primary
key beside an old parent-heap row; invalid bounds and ambiguous policy state are
admitted. Both old-reader controls are accepted and mutate copied store files.
Those failures, old stdout, hashes and ledgers remain evidence, not passing tests.

Old policy storage is genuinely ambiguous. In the controlled policy fixture:

- `p` has forced RLS and `parent_guard USING (k < 10)`.
- `low` stores forced RLS with no policies; the old loader nevertheless applies
  its parent's policy and shows acknowledged key2 to `legacy_reader`.
- `high` stores both `parent_guard` and independently created `authored_child
  USING (k = 11)`; the old loader replaces that list with the parent's and hides
  key11. The new loader would allow key11 if those stored policies were silently
  activated.

The migration must not guess authorship from identical names, expressions or
flags. Historical effective state follows the actual old loader: the immediate
parent's raw policy state, not invented recursive inheritance. An explicit local
operator decision is required where stored and old
effective child-policy states differ.

## Native capability component

`sql_partition_execution_v1` is a sticky required-reader capability. Activation
uses the existing durable replacement primitive with named write/sync/rename/
directory-sync fault points and permanently fences the affected handle on a
critical I/O failure. An activation retry always syncs, even when a prior failed
directory sync left the flag visible. Later format replacements also force sync
while the capability is present.

The bit alone does not prove SQL admission. Native only supplies activation,
its current local status, and a legacy-repair prerequisite check: fsync, loaded
secondary indexes, no pending transactions/row-lock owners, and healthy recovery.
SQL separately publishes a versioned completion certificate. Actual old-binary
refusal and unchanged bytes have been observed for the explicit upgrade API;
default startup and ordinary SQL admission were still unimplemented at that
stage. Their later implementation is recorded below.

## Implemented components and observed checks

The bounded inspector at `08f42204` passed six actual old482 stores across both
storage modes, with stable inspection results over two reopens. `485f5317`
additionally binds the canonical local directory and supporting schema, role,
grant, routine/type, collection and index metadata to the operator fingerprint.

The explicit API at `eccbe714` and again at `e45a5118` passed the same eight
upgrade scenarios: parent-heap migration, reviewed invalid-child relocation,
and both policy decisions, in each storage mode. These are repeat validations
of eight scenarios, not sixteen distinct cases. All cases passed two reopens;
the real old482 CLI refused each upgraded copy without changing any file bytes.
Fourteen independently acknowledged source rows and the original fixture hashes
remain preserved. An authorized upsert deliberately changes one migrated row's
payload; preservation does not mean that later authorized updates are forbidden.

Core tests at `b715e546` and test-only `3b0289b2` cover sticky activation, eight
first-activation I/O boundaries, unsafe configuration/pending-owner prerequisites,
and already-visible-flag synchronization retry (four focused tests total).
Actual PostgreSQL 16 rejects primary/unique keys that omit a partition key with
0A000. The legacy preflight at `e45a5118` matches that rule before activation.

The current source adds typed optional execution markers and completed-plan
receipts. Empty upgrade bookkeeping collections do not invalidate an otherwise
unchanged reviewed plan after interruption. A completed explicit retry must
match both the local directory and the digest of the original complete plan;
it returns the prior receipt without another commit. This receipt reports a
historical migration, not authority for imported data or certification of later
catalog changes. Unknown/unfenced completion certificates refuse. The current
focused run at `adfa24f3` passes four tests. Its eight genuine-old-store scenarios
also pass, including exact-plan retries immediately after migration and after
two reopens, with no new Native commit on any retry. Old482 refuses all eight
upgraded copies without modifying their bytes. These remain explicit API tests.

The initial `f5ee9eec` focused run has one pass and three setup failures: its
helper bundled raw partition DDL in one call and did not create either child
before INSERT. `d965e015` separates those DDL calls without changing the runtime
or the required retry assertions. The corrected fixture passes at `adfa24f3`;
retain the failed run. Raw multi-statement partition DDL handling
is separately unqualified and must not be reported as fixed by this test change.

Automatic admission was implemented at `fcf2617b`: SQL text/AST/COPY/procedure
entry, refusal by the direct read-only engine, and pgwire construction before a
listener can serve data. The original six default requirements and two actual
old-reader controls pass at that source and again at `08cbbfc0` and `8ed4a67b`.
The original failed baseline remains intact. Fresh partition descriptors carry
v1 only after the sticky reader fence and initial certificate are durable.

The completion certificate now binds the exact affected schema/trigger catalog
digest. Changes and undo publish that digest in the same Native transaction.
Ordinary schemas do not enter the digest. A 64MiB/128-instance memo uses Native
process-local incarnation/generation stamps and complete recent-change coverage;
replacement, missing coverage or an untracked generation bump causes a bounded
metadata rescan. It neither grants Native permission nor supplies portable
backing provenance. The Native EmbeddedMemory batch API now selects bounded
resident images before decoding, rather than materializing the whole collection.

Related focused runs found and retained real regressions: an uncertified local
child trigger (`fcf2617b`), implicit promotion of an unauthored visible `id` to
an incompatible partitioned-table PK (`08cbbfc0`), and genuine-old-store COPY
parent-heap writes plus copied child PK names breaking startup (`08cbbfc0`).
`8ed4a67b` fixes the latter paths. COPY shares the existing per-row physical
INSERT path. The v1 SQL view gives same-key inherited child PKs local identities
while preserving raw legacy descriptors. Actual PG16.13 independently confirms
child PK/UNIQUE names, nullable/nonunique `id` without an authored PK, and COPY
BEFORE/AFTER physical context and prior-row visibility.

At `8ed4a67b`, all eight default, eight explicit-plan and fourteen extra
entry/startup scenarios pass in both storage modes, with two reopens. Extra
entry cases preserve exact old full-record hashes and check read-only refusal
plus shared-session exclusive fallback before repair. Actual old482 refuses
every applicable upgraded copy without changing its bytes. The fourteen
independent original ACKs and every immutable source fixture remain intact.

The stabilized focused coverage is 97 SQL checks (94 unchanged source-scoped
checks from `8ed4a67b`, plus three COPY checks at test-only `c3588bf8`) and six
Native checks at unchanged `fcf2617b` Core/Page/workspace source. The initial new
COPY test's postcondition SELECT had no policy under FORCE RLS; the fixture now
adds a verification-only read policy after the COPY checks finish. Its INSERT
policy, absent child grants, refusal assertion and ACK requirements remain.
The failed fixture run is retained. Broad validation at `c3588bf8` finished with
failures: Core 1378 passed/1 failed/12 ignored, SQL 2531 passed/6 failed/4 ignored,
wire 490 passed/1 failed/2 ignored. Three Core crash families used stale external
example helpers. Rebuilding all three helpers from exact `c3588bf8` and rerunning
those unchanged families gives 40 passed/0 failed/3 ignored. The original failing
run is preserved and is not relabeled as passing.

The private branch now includes merged main `1.0.488-beta`, `d1d816af`, through
`f7fb9888`, retaining the SQL partition, Decimal and application unique-scope
reader fences. New PK backing and batch tests have a fixture-only baseline at
`a4ce8ac3` over unchanged `f7fb9888`: one pass/six product failures. A partial PK
made read-only SQL omit two acknowledged rows. The complete constraint-index
matcher now includes predicate, exclusion and NULL policy. Parsed `PARTITION OF`
uses the existing physical creation path. All seven regressions pass at
`57dcb89c`, including modes, repaired exact backing, batch rollback and reopens.
Earlier harness compile/setup failures remain separate evidence.

Actual PostgreSQL 16.13 rejects four old SQL fixture setup shapes: missing
partition keys in PKs (0A000), or parent INSERTs without a leaf (23514). Corrected
setup retains every original assertion; all four tests pass at `57dcb89c`.
The wire fixture has the same invalid PK shape and was corrected at `cc84f24f`.
This does not independently qualify its legacy metadata-only ALTER INDEX ATTACH
behavior. Original upstream application tests and timeouts remain unchanged.

Two original ALTER tests expose real intermediate parent/child publication
failures. Fixture-only `8dcad32b` over unchanged `57dcb89c` has one pass/five
failures in six new transition checks, plus both original ALTER failures.
Source `cc84f24f` publishes all converted/default/generated rows, schemas and
certificate in one Native decision and restores the complete tree together on
undo. A proposed certificate validates the complete proposed forest. All six
new transition checks and both original ALTERs pass at `cc84f24f`; the seven
PK/batch tests pass there too. The complete focused batch finished: 112 SQL
and 195 protocol tests passed. The later broad run passed all Core tests
(1,391 passed, zero failed, thirteen ignored) and completed SQL successfully;
protocol remains live at this checkpoint. These results cover `cc84f24f`,
not later changed SQL or the independently advanced main489 Core.
Serial/identity ADD and independent process/I/O/VM qualification remain open.
Raw logs, exact sources, fixture corrections and reference controls are under
`docs/evidence/partition-execution-2026-11-12/catalog-transitions`.

Whole-child column/default/generated/independent-key lifecycle, non-PK UNIQUE
identities, FKs, namespaces/rename, other raw DDL batch forms, remaining hash types,
ONLY/snapshots/concurrency, protected Native authority, process/fault/VM and HA
propagation remain required. The fixes above still require qualification on the
final delivery source. No delivery version is reserved, and no new standard
CLI/server binary, release or deployment was produced.

The subsequent child-schema batch preserves current-version child-owned
columns/defaults and independent physical primary keys. Parent ADD/TYPE changes
preserve unrelated child defaults; parent SET/DROP DEFAULT changes only the
specified column with whole-tree undo. Parent reads, locks, INSERT, UPDATE,
DELETE and cross-leaf movement materialize visible leaf key fields while
retaining the actual physical identity. At `e036228f`, all 118 focused SQL
tests pass, including six new child/default/key controls in both storage modes
with reopens. This is focused evidence, not full application acceptance.

The declared PostgreSQL target is **18.4**, as recorded in `identity.rs` and
`POSTGRES_COMPATIBILITY.md`. Initial identity experiments mistakenly used
PostgreSQL16's different behavior: recursive identity ADD with children refuses,
and newly created children do not inherit identity. Both attempted PG16-only
runtime changes were reverted; the actual PG18.4 controls confirm successful
recursive identity backfill and a shared root identity on every partition.
Earlier nonidentity child/DML and invalid-fixture controls were independently
rerun on PG18.4. All historical reference and failed product logs are retained.

Valid multi-option identity SQL exposed sqlparser's fixed option order. A token
and source-preserving adapter now orders recognized options for the parser
without altering literals/comments or hiding duplicate/unknown options. A
separate constructor correction honors sqlparser's `NO CYCLE` boolean polarity.
Both parser units pass at `c4c98a55`; eight child tests pass and recursive
identity publication still fails at that revision. Source `8739d3e6` adds atomic
publication of all rows/schemas, the new owned sequence, default ACLs and the
certificate, with complete retryable undo. The composed main489 source
`3867be58` passes both parser units and all twenty-five schema/key integration
tests, including recursive backfill, shared serial allocation and rollback of
rows/schema/sequence/default grants. Its new exhaustion fixture needed an
explicit MINVALUE0 so START1/MAXVALUE1 reaches exhaustion rather than rejecting
invalid bounds. Both the failed reference fixture and corrected PG18.4 pass
are preserved. See `child-schema-identity/summary.json` beside the earlier
`catalog-transitions` evidence.

Remote main independently advanced to `b9394418` / 1.0.489-beta (PR157 native
read/recovery foundation). This private branch includes it from merge
`3c20d54d` and inherits489; version identity and full formatting checks pass.
No new Root delivery version, CLI,
release, deployment or physical HA/scale-out result follows from that merge.

The next baseline `4508ece8`, tests only over `3867be58`, exposed five unsafe
stationary-ACK certifications. Source `6c16e66e` preflights every resulting row
against proposed v1 schemas before the fence, including constraints, canonical
Native PK binding, final-state foreign keys and exclusion constraints. It
preserves legitimate `NOT VALID` exemptions. Thirteen admission units and the
same twenty-five schema/key tests pass. Thirty actual old482-store controls
also pass, preserving all fourteen independent source ACKs and original files.

Separate examples with runtime trees identical to `6c16e66e` pass 152 process
cut/volatile-emulation cases and 96 scoped one-shot/persistent EIO cases across
the genuine old stores. The independent verifier checks exact coverage, whole
old/new row/schema snapshots, three stable opens, independent migration ACKs,
failed-handle refusal, Native indexes and old-reader byte identity. Those cuts
qualify these legacy migrations; they do not replace crash qualification of
every other DDL path. Volatile emulation is not a VM power-cut result. The new
combined broad Core/SQL/wire run is active, with no assumed terminal pass.
See `upgrade-recovery/README.md` and its source-scoped archive/manifest.

## Implementation and qualification checklist

1. Add optional execution-version fields to PostgreSQL `PartitioningSchema` and
   `PartitionOfSchema`; missing means legacy, unknown means refuse. Oracle
   partition metadata remains separate. New partition definitions must carry v1
   only after capability/certificate activation. Generic saves preserve markers.
2. Inspect raw schema/trigger records with exact stored identities and parent
   links. Reject cycles, wrong namespaces, incompatible columns, overlapping
   bounds, unknown versions and malformed binding/certificate state. Do not
   infer lineage from an unqualified name or donor identity.
3. A bounded read-only inspector reports a SHA256 snapshot fingerprint, legacy
   relations, policy-decision requirements and invalid child rows. Stream native
   batches where possible; collect only rows to move, with an explicit memory
   budget. Bind the fingerprint to relevant raw metadata, actual native
   collection metadata, definitions and acknowledged row content. This is a
   local operator snapshot binding, not authenticated portable provenance. Bind
   the canonical local directory as an accidental-replay guard and stream role,
   membership, privilege, routine/type and complete schema definitions into the
   fingerprint too; a directory path never supplies Native backing authority.
4. The public offline plan binds that exact snapshot. Policy choices are
   `keep_stored` or `preserve_historical_effective`; preserve exact old raw
   policy records in an immutable migration archive when a reviewed decision
   changes them. Out-of-bound child moves require explicit source/id/destination
   entries tied to the snapshot and the same exact partition tree. No row is
   dropped or merged to make a uniqueness check pass. Missing decisions, stale
   fingerprints, extra actions and conflicting acknowledged keys refuse.
5. Automatically route old parent-heap rows into the actual final leaves. This
   preserves their logical parent relation. Valid leaf rows stay in place.
   Validate all resulting bounds and constraints, destination identities and
   live index effects. Preserve original record content. A genuine duplicate or
   unroutable ACK requires explicit data/schema reconciliation, never deletion.
6. Preflight the entire migration. Persist the reader fence before any upgrade
   publication. Use one Native transaction for source deletions, destination
   inserts, schema-version/policy updates, required trigger clones, archived raw
   policy records and the completion certificate. The existing approved
   TransactionExceedsBufferPool safety policy still applies; do not weaken it.
   Native rollback/crash recovery must expose either the complete old set or
   the complete upgraded set with its certificate. A flag-only or empty-system-
   collection interruption is not a completion certificate and must resume.
7. Cache admission by actual database instance and schema/trigger/certificate
   generations. For ordinary schema changes, use the Native recent-change window
   to inspect only changed records; fall back to a complete bounded metadata
   scan if coverage is unavailable. Avoid a full catalog scan per statement or
   per ordinary CREATE during ERP baseline loading. Imported or directly changed
   legacy descriptors must invalidate admission too.
8. Cover SQL text, parsed AST, COPY, procedure entry and direct read-only engine
   entry points, plus host startup before a listener serves data. Read-only
   entry points never mint a certificate or modify a store. Shared SQL sessions
   request the existing exclusive-access fallback when actual repair is needed.
   A certified current store takes the fast path. No GUC can bypass admission.
9. Reader flag without a certificate is an incomplete activation; unknown or
   unfenced certificates refuse. New stores without PostgreSQL partitions should
   retain ordinary compatibility until the feature is actually used. Sticky
   fencing remains after partition DROP or rolled-back first use.
10. Default startup must refuse ambiguous policies and invalid child ACKs with
    SQLSTATE55000 and actionable inspection output. The explicit trusted offline
    plan is a real recovery path, not a fabricated replacement fixture.

## Required evidence and remaining gates

- Make all eight original old-store cases pass without editing old fixtures or
  weakening assertions. For valid migration, exercise upsert arbitration, all
  fourteen independent ACKs, native index verification and two reopens.
- Explicitly qualify each policy choice and child relocation, stale/wrong/extra
  plan controls, duplicate ACK refusal and exact raw policy preservation.
- Use the real old482 CLI to prove rejection before any copied store byte changes.
- Inject process cuts at fence, staging/publication and ACK boundaries; inject
  write/fsync/rename/directory-sync failures; reconcile independent ledgers.
- Complete Native-protected-collection authority, standby/replication capability
  propagation and process/fault proofs. Public unprotected Native APIs must
  refuse rather than bypass a required context or signed mutation grant.
- Finish trigger namespace/rename/mixed operator-type lifecycle, other partition
  constraints/FKs/hash/ONLY/concurrency/snapshot paths, then broad SQL/protocol.
- Integrate the next actual main, choose the next unused delivery version, run
  final checks/CI, merge/push and verify. This private source is not delivery.

The authenticated portable Native backing-birth/accepted-provenance producer for
replica rename recovery is still independently unimplemented. The original
Oracle/PostgreSQL inventories, unchanged10s waits, UI400/encrypted backup/restore,
physical HA, independent crash/VM and approved P1/P2 tracks remain in the full goal.
