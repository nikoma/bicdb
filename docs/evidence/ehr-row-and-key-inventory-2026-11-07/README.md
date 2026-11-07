# EHR row routines and bounded key inventory — 2026-11-07 UTC

Status: source regressions verified; **not production admission**. The final
runtime code under test is `fe68680104b8f1bc4bc43b1ca581c0020edaa0db`, including
main `8d18dfb` (index-range fixes) and `871ddc2` (test expectations). Earlier
`ded71d1` results are retained as historical evidence; the broader SQL/protocol
gates were repeated after the final JSON wildcard change and latest-main merge.

## Changes and causal evidence

- Row routines use their caller's transaction for `txid_current()` and nested
  read-only table functions. They still refuse scalar writes, nested writes,
  and row locks. The first identity regression failed before the change; the
  nested regression then exposed both automatic BEGIN and own-transaction
  checks. The resulting five cases pass.
- Simple administrative UUID-primary-key inventory queries load only the
  requested visible payloads. The new Core API returns key candidates without
  deserializing the record corpus; callers must resolve visibility before using
  candidates as results. SQL uses that API only for an eligible superuser query
  without pending writes or a Core collection policy. Residual filters, offsets,
  windows, other roles, and other unsupported shapes retain their ordinary path.
  The allocator regression failed at over 34 MB for four keys from an 8 MiB
  corpus; all required ordered/seek/authenticated/transaction modes now satisfy
  its 8 MiB allocation limit. This bounds payload loading, not the total key
  inventory, and does not repair every query's memory use.
- Comma LATERAL PL/pgSQL sources use the existing table-function execution path,
  including its transaction and read-only restrictions. Both new cases failed
  before the fix and pass afterward.
- `#variable_conflict use_column` is parsed before DECLARE, preserving the first
  local variable. Other unimplemented compiler modes/options are explicitly
  refused. All four regressions failed before the fix and pass afterward.
- Procedural statement and assignment scanning skips comments and dollar
  literals. Apostrophes in comments previously swallowed the aggregate query's
  terminating semicolon. Dollar delimiters are distinguished from characters in
  identifiers. Two new cases failed alongside two passing controls before the
  lexical fix; the added identifier case exposed a further keyword-scanner
  failure. The final five cases pass, including the original aggregate shape.
- JSON function wildcards use the actual result columns, excluding additional
  scalar aliases used only for expression typing. `SELECT *`, qualified
  wildcards, empty results, joins, explicit column aliases, ordinality, and a
  real scalar routine loop retain their visible shape. All four regressions
  failed with `42703` before this fix and pass afterward. Scalar alias expression
  typing is retained; wildcard metadata no longer invents `x.x`.

The retained `*-before*`, intermediate `*-after*`, and final logs distinguish
those stages. A passing control is not counted as causal proof.

## Verification

- Final focused run at `fe68680`: 48 passed across 15 targets.
- Final SQL library: 763 passed, four existing ignored.
- Final routine, inventory, JSON, and index integration: 81 passed across 24 targets.
- Final protocol integration: 188 passed; procedural migration protocol: one
  passed; protocol library: 140 passed. Full logs use the `fe68680-` prefix.
- Historical focused runs at `afb1c3e` (38 across 12 targets) and `ded71d1` (24
  across eight targets), and the preceding broader `ded71d1` gates, remain
  available. These overlapping runs are not added into a purported unique count.
- Core library: 452 passed, three existing ignored; transaction/snapshot/recovery
  integration: 24 passed across seven targets. These were run at `fc63926`.
  `fe68680-core-reuse-source-hashes.json` proves all 270 Core/paged-storage inputs
  and root dependency manifests/lock are identical at `fe68680`; SQL results
  are not reused through that proof.
- Formatting and diff checks run locally on the owned sources; full Cargo
  formatting was also run on the qualification host.

Builds run on the separate build host with one Cargo job, incremental compilation
disabled, and active guards retaining at least 12 GiB available RAM and 8 GiB
free disk. No clinical data or original clinical keys are used there.

## Protected catalog verification

The retained, failed, unadmitted clinical rehearsal remained on the production
host with its ordinary guards restored. A read-only diagnostic on frozen
`fc63926` compared every catalog value against the retained PostgreSQL source:
**133,537 source rows and 133,537 native rows matched exactly**. Peak native RSS
was 907 MiB, minimum available host memory 4,275 MiB, and native reading plus
canonical comparison took 641,430 ms. Cold listener connection took another
48,395 ms. This was an unoptimized diagnostic, not a latency acceptance run.

Only the allowlisted aggregate summary is exported in
`clinical-catalog-read-summary.json`. Row values, keys, and row hashes stayed on
the production host. Read-only transactions were rolled back and the owned
listener stopped. The live PostgreSQL database, applications, record retention,
and production routing were unchanged. This does not accept the whole clinical
transfer, patient decryption, native backup, or independent recovery.

## Application and deployment boundary

The frozen `fe68680` development CLI and supervisor have Core/SQL optimization
level zero. Their immutable build identity is retained in the frozen metadata.
The actual generated monitoring action now claims and completes its synthetic
job with the real actor context, with no failed authored SQL statements. Its
development diagnostic reused a cold synthetic scenario with the owned job clock
adjusted; it is not the complete background-worker acceptance test. The preceding
`ded71d1` wildcard failure is also retained.

The wider frozen-package diagnostic passes meal/mobile operations and the seven
earlier patient workflow areas. Its monitoring test waits for a future reminder
although the worker intentionally creates only due reminders; the EHR fixture is
being corrected through the real scheduling API while retaining the original
45-second dispatch assertions. The wider run also records a scheduled-email
deadline failure followed by passing encrypted persistence/provider-acknowledgement
checks. Neither failure is a complete-release pass.

Complete background monitoring, all hosted workflows, optimized runtime,
browser/capacity, full clinical transfer/recovery, and installation beside
PostgreSQL are still pending. No treatment rule or production safety gate was
relaxed. The
owner deferred HA; the existing PostgreSQL server's admin SSH access is still
unavailable. Production remains on PostgreSQL.
