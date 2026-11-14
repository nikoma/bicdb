# Passive capture for the four failed native 503 cases

This diagnostic-only successor starts from exact failed source
`c6a081a6026f4efd00d80f392c94b67f2f2f4e12`, version `1.0.503-beta`. The original
serial result remains **208 passed, 4 failed, 1 ignored** in 1197.50 seconds,
run `43529104485`, job `132242099377`. Original log SHA256:
`8af0ca1ea94ef88ae64a0ed4101d907d044072acd20ff8ce2c22d4149da09e54`.
No diagnostic runtime result exists yet. A later passing run cannot explain or
replace that failure; the separate 504 serial pass is not root-cause evidence.

The capture is compiled only for tests and selected only by these four existing
Rust test names, which are unchanged:

- `native_serving_observation_scope_actual_normal_serving_current_damage_refused_at_exit`
- `native_hot_recovery_finite_open_never_starts_process_worker`
- `native_hot_recovery_retained_original_store_replace_restore_revokes_held_reservation`
- `native_initial_data_actual_tls_drop_without_stop_ends_inner_owner_and_reopens_same_root`

There are no new protocol calls, retries, probes, readiness flags, quorums,
permissions, timeout extensions, assertion changes or native persistence changes.
The provider's original protocol body is factored into a private inner method
only to retain its exact returned `Result` before the test-only return observation.
Non-test compilation has no capture module/fields/helpers or emitted evidence.
This is not a production diagnostic feature or a qualification fix.

## Actual boundaries recorded

The actual provider retains a weak capture handle when constructed by the
selected test. This lets its real listener and worker threads record into the
same buffer without propagating a test identity into protocol fields or keeping
native owners alive. Lifecycle mode is captured with that weak handle at
construction, so a late worker from an older mode cannot be labeled as the next
mode. A provider created outside that original test context is not secretly
rebound; its missing observations remain a scope gap.

Metadata records contain actual acquisition/read-request/context IDs, caller,
range/epoch, term/read index and old/new/fresh voter sets. Each bounded-worker
queue intent and actual transport invocation/return records voter, attempt,
next index, queue and transport elapsed time, original/context/attempt deadlines
and actual slice. Queue intent is explicitly not wire dispatch. Returned
transport errors are observed before the collector discards them. Actual ACK
acceptance/rejection, match/conflict information and acceptance errors are
recorded only after the existing consensus guard has dropped. Terminal records
preserve the original result/error; no diagnostic result feeds the quorum.

Initial CONFIG records identify both the preparing host and the actual recipient,
configured network IO timeout, each phase's begin/return, elapsed time, remaining
deadlines and the exact error. The original host/member order and 5,000ms budgets
remain. They do not infer native absence or reset partial preparation.

The lifecycle test records each mode, append intent/full expected value/hash,
actual append result before its original unwrap, and whether a later Strong,
materialization, fault/drop branch or same-root reopen was reached. Append error
is explicitly uncertain write effect; each returned successful append remains
an ACK even if later observations are lost. Strong uses separate success and
presence fields so JSON null does not become absence. Diagnostic UUIDs are never
protocol business IDs. The original two-second SDK object, gateway, operations,
assertions, drop/reopen ordering and fault actions are retained.

## Memory, output and completeness limits

Active operations perform bounded-memory observations only: no file/fsync,
network request, user callback or worker join/drain is added. The sink uses a
nonblocking memory latch. Per case, it retains at most 4,096 records / 4 MiB;
32 KiB is a post-serialization per-record retention limit. Selected test fields
and payloads are small, fixed diagnostic DATA; this is not a general bound on
arbitrary caller-created JSON allocation. Errors use a bounded UTF-8 formatting
writer before string growth: at most eight source errors and 2,048 bytes per
error, with explicit text truncation, formatting and depth-limit flags. An error
within those limits retains its complete text and available IO kind/OS code.

A busy sink, cap, oversized record or serialization failure increments a gap
counter and never changes the original API result. The closed flag is rechecked
under the same record latch used for the final snapshot, preventing an append
after that snapshot. The outer guard is declared before test fixtures/guards;
it snapshots memory and releases the latch before emitting one
`NATIVE_PASSIVE_DIAGNOSTICS` JSON line after caller-owned scopes have dropped.
No output failure substitutes for the original test result.

The snapshot does **not** prove global quiescence. No extra joins or waits are
introduced; late background probes may escape the snapshot, even when all drop
counters are zero. Output therefore always declares
`global_capture_complete=false`, `background_quiescence_proven=false`, and
counters as snapshot-only. Buffer integrity and global completeness are separate.
This memory capture is not a durable external ACK ledger.

Raw SDK frames, internal APP retry/refresh attempts, native commit reconciliation
and host CPU/memory/IO samples remain explicit gaps. The existing SDK observer
contract requires durable callback success; this patch does not relabel a RAM
sink as a durable observer. Test output can be retained by a public-repository
workflow and is not presumed confidential. No generated TLS key/certificate or
database custody bytes are collected.

Formatting, source diff and unchanged-version checks are the only local checks.
No build, runtime test, new CI dispatch, source version bump or deployment is
part of this diagnostic batch. Availability, fault-control and no-loss claims
still require the intended boundaries to execute, with the original failures
preserved and incomplete capture reported explicitly.
