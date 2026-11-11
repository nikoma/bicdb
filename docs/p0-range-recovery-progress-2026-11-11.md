# Distributed-range P0 correctness and recovery progress

This source batch follows the failed Azure qualification. The original experiment
and its evidence remain pinned to 1.0.461-beta,
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`. Subsequent instrumented diagnostics
use 1.0.463-beta, `d49a391b3c6157f90f29e9f2f5214143c60f9f4b`. None contains
these engine changes. These findings do not grant production admission.

| Component | Source change / finding | Evidence and remaining gate |
| --- | --- | --- |
| Relocation scheduling | Implemented fair turns for existing and newly queued moves | Two integration regressions pass; real blocked I/O and complete convergence remain separate |
| Stable-membership range write ACK | Require durable certificate ACK quorum before success, checked after local WAL; insufficient replies return an unknown outcome | Five fsync-on integration regressions pass; reconfiguration and pre-WAL irreversible-error paths remain open |
| Identical immutable-event retry | Re-admit matching data with the exact current row version | Retry/reopen/waiter tests pass; another WAL command and native audit/change-capture effects may result |
| Metadata read authority | Fresh request-scoped current-term proof and old/new voter quorums | Component tests pass; no range-leader-to-metadata-leader acquisition service or strong row-read endpoint yet |
| TCP data RPC | Absolute network exchange deadline below TLS, parent cancellation and final expiry check | Plaintext and actual mTLS tests pass; DNS/locks/CPU/fsync, bootstrap and whole-tick bounds remain outside this proof |
| Strong reads | Missing end-to-end implementation and readiness proof | Healthy StrongGet target fails on baseline; legacy Get stays eventual |
| Relocated image and compacted range history | Confirmed healthy-quorum recovery failure after 1,025 commands | Target value is present, but installed range history is zero; no durable image-base fix in this batch |
| Data-quorum handoff during promotion/cleanup | Confirmed late-ACK visibility failure | Actual five-store metadata + fsync-on replica test: cleanup-first permits a healthy new quorum to report recovery complete while an acknowledged value is absent; old copies remain |
| Physical qualification | Not established | Local component checks and the existing/eventual Azure runtime do not qualify these engine changes |

The exact combined candidate was `035c233ef7f7eec51c22fe17373944e07e9f5f72`
(package version 1.0.463-beta before the merge-version bump). Its 12 integration
checks passed. The first full core unit run passed 502 tests, failed one and
ignored three: the release profile disables a unique-index apply-time backstop
which that test expects. The failure is retained. Enabling the documented
`BICDB_PAGED_UNIQUE_APPLY_PROBE=1` opt-in, without changing assertions, passed
503 tests with the same three ignored. Debug CI enables that backstop by default.
This is an explicit test configuration, not proof that default release runtime
uses the extra backstop. Versioned final-head CI remains a separate record.

The checks used one Cargo job, incremental off, core opt0/debug0, retained
optimized dependencies and two test threads. An active memory/swap/pressure
monitor guarded the task's process group. Raw commands, results and resource
samples are in the [combined evidence](evidence/p0-combined-validation-2026-11-11/summary.json).
No new optimized server binary, published release or production deployment is
established by these checks.

Reproductions and protocol boundaries:

- [Fair scheduling](relocation-scheduling-regression-2026-11-11.md), including the original deterministic starvation failure.
- [Certificate ACK cut](evidence/range-certificate-ack-cut-2026-11-11/README.md) and [narrow correction](evidence/range-certificate-ack-fix-2026-11-11/README.md).
- [Network deadlines](cluster-rpc-deadlines-2026-11-11.md), including bootstrap/connect/DNS limits and uncertain remote outcomes.
- [Strong-read proof design](explicit-strong-range-read-design.md) and [independent integration review](strong-get-integration-review-2026-11-11.md).
- [Actual relocated-image/history failure](evidence/relocation-image-history-2026-11-11/README.md) and [durable image-base design](range-image-base-protocol-design.md).
- [Cleanup-first handoff witness](evidence/relocation-handoff-witness-2026-11-11/README.md), including the successful recovery-before-cleanup control.
- [Concrete image-capture interfaces](range-image-capture-interface-review.md), still a proposal rather than an installed data-plane protocol.

The handoff witness retains the fourth-voter promotion guard: two responders
cannot recover during the four-voter phase. The failure arises after legitimate
source cleanup/removal, with one current voter unavailable. The new two-voter
quorum reports ready without the late ACK; another old replica retains its value
and the removed source retains the command catalog. This is a correctness failure,
not a claim of irretrievable data loss or an explanation of either Azure run.

The next engine batch must establish an image-bound certified range floor and a
safe data-quorum handoff, then integrate fresh authority, range recovery, local
visibility and final semantic validation into a healthy strong-read path. A
permanent unavailable response does not pass. Physical acceptance still requires
zero missing acknowledged commits, correct read consistency and routing, bounded
fault-safe convergence and sustained throughput gain after convergence.
