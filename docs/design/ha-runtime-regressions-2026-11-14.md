# Native recovery candidate: runtime regressions — November 14, 2026

Exact failed source: **1.0.510-beta**,
`d8115fedf13b0bd7107cfddd2664f2c6ea2f261f`, required workflow
[43572675143](https://github.com/nikoma/bicdb/actions/runs/43572675143).
The actual checkout `b38f461ffc2a6729abb7a441bf25951009bbd721` has the same
complete tree `be4b39ad95e86abac73d7c7ca889301d10d88c90` as that candidate.
Workspace/all-target and separate no-default-Core consumer compilation passed.
The runtime failures below prevent describing the assembled candidate as green.

## Oracle HR process stack overflow

**Failed runtime acceptance; exact crashing case and frame inconclusive.**
PGwire job 132380250996 completed thirteen targets with 227 passes and one
ignored case. Its Oracle HR target started 50 cases and recorded 33 individual
passes before a Tokio worker stack overflow and SIGABRT 6. The other 17 cases
have no completion record; they are not 17 demonstrated assertion failures.
The last passing case does not identify the crashing case. Cargo exited 101;
the memory guard did not interrupt the process. No physical RPO/RTO follows.

The exact parent main `f69b242c9db3afe0d24e83520f94a853093d4b98` passed all 50
Oracle HR cases in 5.84 s in job 132376807669/run 43571509341. PGwire and SQL
runtime/test source files are byte-identical between that parent and d811.
The candidate adds two inline `Option<RowTransactionGrant>` payloads to every
`Transaction`. Their complete layout is reserved even when absent. Both
`ConnectionState` and `SqlSession` own transactions by value, propagating that
growth into ordinary SQL execution. This is a concrete layout change and a
plausible stack-overflow cause, not a measured stack-size or backtrace proof.
The worker name does not distinguish async polling from blocking SQL execution.

The correction candidate boxes these two rare private grant payloads at their
existing actual constructor locations. It preserves exact grant fields,
original request/scope identity, native/WAL outcome custody and every check.
Ordinary transactions with no import grant allocate no grant. Neither the
runtime stack limit nor the Oracle tests, assertions or four-thread setting is
changed. Direct fixture traces retain test-thread, port, profile and actual
transaction size even if process abort discards libtest capture buffers.

The next smallest acceptance is the unchanged 50-case Oracle HR target through
the existing required PGwire job, followed by the native grant/WAL outcome
cases. Passing that target establishes regression acceptance for the corrected
head; it does not alone identify the original crashing frame or qualify HA.
If it aborts again, retain the fixture trace and a bounded native backtrace
before considering another correction. Do not increase the stack to hide it.

## Missing-owner refusal hidden by readiness poisoning

**Failed exact error-contract assertion.** Core job 132380251018 recorded
914 passes, two failures and eight ignored cases. In
`native_metadata_learner_cannot_vote_and_cached_legacy_store_cannot_adopt_enforced_owner`, the test
preserves an earlier actual missing-owner persistence refusal but its later
read-authority check no longer sees the expected missing-owner error.
That actual read error was not printed by the failed assertion; its exact text
must not be invented. Source inspection shows the new version-readiness check
preceded the existing actual physical-owner check, masking that refusal after
the rejected transition poisoned the gate.

Source correction `b154f7f6379d6b862d55ab84e61335855fef6621` restores the
physical-owner check first, then requires the unchanged ready-version gate
before any positive context. It does not clear poison or admit an ownerless
actor. The exact existing assertion remains, with the actual error included in
its diagnostic. The next acceptance is that unchanged case and its positive
protected-owner counterpart in the existing Core suite. **Correction untested.**

## Snapshot test bypasses the live append operation

**Failed positive fixture setup; no production poison conclusion established.**
`native_birth_snapshot_cache_and_reopen_preserve_known_bindings`
called private `install_snapshot` directly. The new authority invalidation
correctly refuses mutation without a live operation. The failed positive setup
reported `metadata authority mutation lacks live original operation or is
poisoned`; this disjunction does not demonstrate poisoned production state.
The same test passed on exact parent main f69.

Source correction `99b9c1d823f6c7d46d9fd2466a7a9014ed896542` submits both the
invalid and valid snapshots through the actual public append/snapshot receiver,
which owns that operation. Original birth/cache/reopen assertions remain. The
invalid snapshot must leave state/version unchanged; the valid snapshot must
acknowledge and advance the generation with no active operation left behind.
No production guard is relaxed and the test cannot mint an operation witness.
The next acceptance is the corrected existing case through Core, then protected
snapshot/append regressions. **Correction untested.**

## Evidence boundaries

The separate Rails run 43572675090 passed 20 tests/100 assertions with no failures,
errors or skips. That is useful ordinary-application compatibility evidence;
it does not supersede the failed Oracle target or validate distributed movement.
Native, SQL and remaining-workspace results must retain their own actual final
counts. Earlier 508 compiler failures and the failed Azure qualification remain
unchanged. Corrected-source execution, cold recovery, native operator/controller
integration, Joined quorum/release, and physical expansion remain open gates.
