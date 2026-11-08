# Necessary row-policy equalities as existing index candidates

A common enterprise query filters `number = $1` while a SELECT policy supplies
`org_id = (SELECT current_tenant())`. An existing `(org_id, number)` index cannot
serve the query if the index planner only receives the authored WHERE. On the
pinned pre-fix engine, a protected 100-row tenant plus 0/1,000/5,000 unrelated rows
has count-query medians 31.84/298.50/1,370.51 ms; the diagnostic control with the
identical explicit org equality stays around 5–6 ms. This exact before/after probe
declares its tenant getter STABLE, matching generated Carrier context getters;
the earlier volatile-getter diagnostic remains separate historical evidence. Those shared-host spans are a data-growth
reproduction, not an absolute production performance claim.

The source correction makes a **necessary candidate predicate** available to the
existing planner and row-set locator. Full applicable RLS still filters every fetched
record. No application SQL, policy, authentication, native collection policy, index
storage, or secondary query optimizer is introduced.

- The existing `prepare_rls_with_schema` resolver chooses applicable policies and
  handles SELECT action, role inheritance, owner/FORCE, BYPASSRLS and trusted native
  bypass. Bypassed policies provide no inferred restriction.
- Permissive policies admit the union of their branches. Only an identical top-level
  AND conjunct present in **every** applicable permissive policy is necessary.
  Restrictive policy conjuncts are necessary alongside that union. With no applicable
  permissive policy the existing default-deny path remains untouched.
- Only equality on an existing column is considered. Existing lookup-cast rules
  reject lossy casts, collation transforms and unsupported expression keys.
- Values are deliberately conservative: literals, casts, NULLIF/COALESCE, or explicit
  FROM-less InitPlans through declared STABLE/IMMUTABLE zero-argument routines (plus
  literal `current_setting` expressions). The existing overload-aware routine resolver
  supplies the actual declaration. Direct user calls and volatile/unknown InitPlans
  remain scans. This preserves per-row/side-effect behavior rather than speculatively
  invoking arbitrary policy functions.
- The existing statement/context-scoped policy InitPlan cache supplies typed values
  only when execution needs a lookup key. Existing structural LocatorStrategy plans
  supply unevaluated key expressions to a DeferredIndexLookup candidate in the normal
  costed candidate vector. Ordinary EXPLAIN reports `RuntimeKeys deferred` without
  invoking policy routines. Actual execution reuses normal typed index lookup,
  pending-write overlays and full residual RLS. No second optimizer is added.
- Literal FALSE and false integer-literal comparisons retain empty-input short circuits.
  Authored function/subquery/custom-operator/domain expressions conservatively prevent
  policy augmentation so newly usable composite keys cannot hoist their effects above
  residual RLS. The general NULL-equality shortcut shares that effects guard.
- Static comparison extraction treats Unsupported constant evaluation as a nonconstant
  optimizer result, rather than rejecting an otherwise valid stored/contextual call
  during EXPLAIN. Other data errors retain their diagnostics.
- Deferred augmentation requires one distinct policy InitPlan value shared by every
  eligible lookup, and no existing authored lookup. Multiple policy values or an
  alternative index that can avoid that value retain the prior execution path.
  Tables carrying non-B-tree indexes conservatively retain their prior path too,
  since those alternatives are excluded from the B-tree strategy metadata.
  This prevents the row-set locator from initializing an unused policy helper while
  comparing alternative indexes. The common tenant-plus-number composite lookup
  remains eligible. Literal-only candidates do not require InitPlan initialization.
- Current statement identity is resolved again, and execution-time folded literals use
  the existing content-verified locator memo. No tenant-blind plan is reused.

The focused regression asserts a two-prefix composite IndexScan and one fetched row,
which is deterministic under host load, rather than asserting a fragile latency limit.
It also covers tenant changes on repeated statement text, empty/NULL settings,
residual denial, permissive OR, common permissive AND, restrictive AND, role inheritance,
policy replacement, unforced owner bypass, forced default deny, SQL BYPASSRLS, native
trusted bypass, direct calls, lossy casts, and nonexecuting volatile sequence InitPlans
in ordinary EXPLAIN.

Reproduce focused checks using the repository Rust toolchain:

```bash
CARGO_BUILD_JOBS=4 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p bicdb-sql --test rls_index_candidates --locked -- --test-threads=1
```

The initial deferred implementation was `5c93ee52fe8102696846cebe3b98cb1575dd1f0a`;
subsequent controls corrected unused keys and domain-column assumptions. Source
`b4e0142437a0518dac5deb176b9dccf088987d51` includes main through
`3f3f9a781030b895e068cfdfdbf7a19fb507aa94` and the conservative alternate-index guard.
Final validation and executable receipts below identify their exact source separately. The baseline's four failing plan controls
and the eager prototype's three failing execution-order controls are retained.
The initial SQL-function sequence-backed positive controls exposed an unrelated existing
`nextval`-inside-stored-function limitation and were replaced by deterministic exception
controls; those harness-invalid attempts also remain in the raw evidence. The negative
sequence controls continue verifying that unsupported/volatile/default/domain expressions
are never speculatively invoked by EXPLAIN.

Primary PostgreSQL 16 controls are included alongside this report: arbitrary PL/pgSQL
STABLE and SECURITY DEFINER SQL STABLE policy exceptions are not raised by EXPLAIN or
false-filter queries. VOLATILE authored predicates must not execute on RLS-hidden rows.
These controls motivated deferred keys and effects guards; a SQL-inlineable constant
exception alone was insufficient reference evidence.

Validation results and exact executable receipts accompany the bounded remote runs. Unsupported predicate shapes retain their existing residual scans;
this correction does not claim universal policy theorem proving or arbitrary-RLS
performance parity with unprotected SQL.

The alternate-index control was established against PostgreSQL 16.15 before changing
the implementation: with 10,000 rows and an existing unique number index, both one-key
and two-key policies return no rows for a missing number without executing a failing
STABLE helper. EXPLAIN shows the number index and residual policy InitPlans. A first
control using a composite tenant/number index legitimately chose that index and
executed its key; therefore the final control deliberately demonstrates the alternate
lookup path rather than claiming that all empty indexed reads skip policy functions.
The BicDB FOR UPDATE row-set controls failed with SQLSTATE22012 before the guard
(15 passing, 2 failing). Their matching-row controls still require the policy error,
so preserving an empty read cannot silently omit the policy. All PostgreSQL objects
were transaction-rolled-back.

A separate PostgreSQL JSONB GIN probe was not a valid extension of the alternate
B-tree contract: the non-leakproof JSONB predicate remained after the row policy,
and PostgreSQL legitimately initialized a tenant-index key. No GIN parity assertion
is included. Non-B-tree tables simply decline this new optimization; their broader
security-barrier/expression-order qualification is outside this change.

## Final validation and measured growth

Source `b4e0142437a0518dac5deb176b9dccf088987d51` passed formatting, all **592**
selected SQL/security/upstream tests (16 binaries, zero failures or ignored cases),
and the CLI build. This includes the 419 general SQL cases, all 17 new RLS cases,
60 Wewobo compatibility cases, 23 pg_dump restore cases, and commit-outcome
resolution. The separate 46 core/page durability checks passed on `07b15b2`; two
original repeat-helper tests were ignored. Their code/dependencies are unchanged
by the final SQL-only delta, so those gates were reused rather than repeating the
394-second crash matrix. `validation.json` records the scopes separately.

The final executable SHA-256 is
`45ae4fc04e68351373f86156c87d929aa5515c5d1658df539e012409de57255d`.
All 3,623 tracked source files were independently hashed against the remote tree
before the final build. Cargo used jobs=4, tests used at most 4 threads, debug=0, and
incremental compilation disabled, serialized under the shared task Cargo lock.
An active guard retained 8 GiB available RAM and 10 GiB available disk. An earlier
07b SQL gate stopped when foreign shared-host builds exhausted disk headroom;
its completed core/page tests remain valid, and the unfinished SQL gate was rerun.
A cancelled exploratory GIN wrapper raced with lock acquisition and source copying;
its output is excluded from correctness/provenance claims.

Fresh native PG-wire probes used authenticated unprivileged readers, parameterized
queries, unchanged 100-row own tenants, identical schemas and the same STABLE getter.
Each cell is a median of 7 measured requests after warmup.

| Unrelated rows | Baseline implicit policy ms | Final implicit policy ms | Final explicit control ms |
| ---: | ---: | ---: | ---: |
| 0 | 31.837 | 5.222 | 5.373 |
| 1,000 | 298.500 | 5.733 | 5.612 |
| 5,000 | 1,370.509 | 5.181 | 5.466 |

Baseline source is `83377bb6f897052ac26e6640998e9bea261e0ee4`, executable SHA
`ffe771ac227ebb8b3062c86ace819ea92e92619ecdc3bfbb8aa58c1ee9caed0e`.
The baseline implicitly scans 100/1,100/5,100 rows. The final implicit query uses
`IndexScan`, `PrefixKeys 2`, and `ActualRowsFetched 1` in all three windows; the
existing policy remains the residual authority. Its final/initial median ratio
is 0.992, and the probe explicitly reports `data_growth_goal_met:true`. Repeated
tenant changes, empty identity, owner bypass, permissive OR and restrictive AND
controls all pass. This demonstrates the specific workload; it does not establish
a universal growth-free runtime or production latency SLA.

The baseline and final engines differ by other mainline correctness fixes too.
Same-lineage red/green focused tests establish the planner mechanism independently
of the shared-host timing comparison; neither historical durations nor absolute
current latencies are represented as controlled Rust authoring benchmarks. No
retained benchmark source, acceptance test, hidden test, or application data was
modified for this correction.

Run `probe.mjs` against a **fresh isolated** authenticated BicDB instance. Set
`CARRIER_RLS_PROBE_OWNER_URL`, `CARRIER_RLS_PROBE_PG_PATH` to an installed
`pg/lib/index.js`, `CARRIER_RLS_PROBE_ENGINE_COMMIT`,
`CARRIER_RLS_PROBE_ENGINE_SHA256`, and `CARRIER_RLS_PROBE_REPORT`. It creates its
own random-password restricted role, refuses conflicting existing objects, and
records no connection environment values. The diagnostic explicit-tenant query is
only a reference control, not an application SQL workaround. Complex predicates,
multiple policy InitPlans, existing alternative lookups, non-B-tree tables and
custom/domain columns retain their existing behavior and need separate performance
qualification.

## CI fixture boundary correction

The first PR84 CI run passed all-target compilation, Rails application qualification,
core tests, and the complete SQL suite (2,212 passed across 201 test summaries).
Its five original ignored SQL cases include four opt-in benchmark probes and one
known functional limitation: unique validation ignores a pending delete. This
change does not hide or resolve that separate transaction/unique-delete blocker.

The PG-wire job failed its existing journal-replication test during setup. One
`with_write` callback held the engine guard across CREATE TABLE, mesh enable and
INSERT: three separately committed operations. Native confirmations need that
guard released; on the busy runner the 1.5-second write lease correctly fenced the
INSERT. The fixture now uses three separately acknowledged `with_write` calls.
Production authority, timeouts, replication, journal/promotion assertions and
application/benchmark acceptance inputs are unchanged. No blind retry was added.

Three unmodified remote rounds passed 18/18, demonstrating that this was not a
deterministic ordinary-load failure. A controlled diagnostic adds the same
two-second host pause between mesh setup and INSERT. The old held-guard arm
reproduces the exact CI lease refusal; the split-boundary arm passes all journal
and promotion assertions. The two tiny diagnostic patches and source hashes are
included for reproduction. Neither diagnostic pause is present in the committed
fixture. The final fixture passes three full rounds of all six embedded HA tests
(18/18), including neighboring admission, demotion, promotion, return-writer fencing
and shutdown controls. `ha-fixture-validation.json` keeps each scope and the
original CI failure separate.
