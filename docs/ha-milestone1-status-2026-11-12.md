# HA Milestone 1: revision-scoped status, November 12, 2026

**Production qualification remains incomplete.** Public main **1.0.485-beta**,
`e3fa39974d23c13d6212d5b94ab84437271c50bf`, merged the metadata Append
frontier repair in [PR 162](https://github.com/nikoma/bicdb/pull/162). The exact
tested candidate was `ce02303676fe5fe730ffd7fc822a3a27ee8189aa`. The merge has
the same tree. A source version is not a published binary or verified deployment.
This document/evidence update adds no runtime feature.

## Deployment modes

| Mode | Implemented and tested scope | Remaining qualification / deployment evidence |
| --- | --- | --- |
| Legacy file shipping; native single-primary synchronous/asynchronous HA | Existing copy/promotion and ordered streaming/election paths. Current automatic-failover configuration validates synchronous voting acknowledgments against election quorum requirements and excludes observer acknowledgments. Asynchronous and timeout-degraded operation retain a positive-loss-window contract. | No new physical RPO/RTO or current-release production qualification. The original audit's unsafe-K configuration concern was source-repaired; it is not still an absent validation feature. See [the audit correction](ha-horizontal-scaling-audit-2026-11-10.md#current-single-primary-configuration-correction--2026-11-12-utc). |
| Distributed virtual ranges | Metadata consensus, range quorum and automatic placement/repair have existing code and CI evidence. The reproduced bounded-Append safety defect is repaired on main. The larger native read/recovery/relocation candidate remains separate and unmerged. | The original Azure four-to-five trial failed. Fault-safe convergence, correct live reads throughout movement, stable five-node capacity gain and production admission remain unqualified. No verified new deployment. |
| Cell-scoped HA/encryption/fencing/recovery | Separate signed authority, lease, fencing and asynchronous-recovery contract; synthetic drill evidence is not a live recovery drill. | No new operational drill, measured physical recovery or production admission evidence. This native-range repair does not qualify Cell HA. |
| Application fleet placement/scaling | Separate worker placement, leases and reassignment tests. | Worker assignment is not database shard scaling. No new independent-machine sustained application-throughput or state-group HA qualification. |

The detailed [four-mode audit](ha-horizontal-scaling-audit-2026-11-10.md)
and [failed Azure report](evidence/azure-long-growth-failure-2026-11-11/README.md)
retain their historical revisions and measurements.

## Findings and exact evidence

Labels apply to the stated scope only. A passing control in a failed cohort
does not turn that cohort, the final candidate or a physical deployment green.

| Finding / check | Classification | Evidence and limit |
| --- | --- | --- |
| Bounded metadata Append could count an untouched divergent suffix | Failed on baseline; source-fixed and CI-tested on main | Genuine fsynced three-store election/message execution: baseline ACK 3 for matched index 1 permits a false topology commit; genuine majority recovery later replaces it. Fixed receiver ACKs 1 and cannot make that premature commit. [Original failing/fixed artifacts, twelve state checksums and offline verifier](evidence/metadata-append-frontier-2026-11-12/README.md); [source/report and controls](design/metadata-append-frontier-safety-2026-11-12.md). This is not a business-row-loss demonstration or an Azure causal finding. |
| Final versioned consensus candidate | Passed | At `ce023036`, [full run 43179263914](https://github.com/nikoma/bicdb/actions/runs/43179263914): Core library 536 passed / zero failed / four ignored plus passing integration invocations; SQL, PostgreSQL-wire and remaining-workspace jobs pass. [Rails run 43179264201](https://github.com/nikoma/bicdb/actions/runs/43179264201), all-target/isolated-consumer compilation, formatting, licensing and repository-boundary checks pass. Remaining workspace has 126 passing test-invocation summaries; that is not 126 individual tests. |
| Original Azure metadata supplies a causal witness of that defect | Inconclusive | All five original archive hashes and metadata-envelope checksums verify. The only equal positive committed-index pair agrees; sequential stopped captures have neither complete intermediate history nor request-time Append traces. [Read-only derivative](evidence/azure-original-metadata-correlation-2026-11-12/README.md). Absence of a final conflict cannot exclude an earlier false commit. |
| Original temporary unreadability of two ACK records | Inconclusive cause; failed live verification | Later routed reads matched both records. Retained committed-command contents and later visibility do not establish request-time strong-read correctness, the first restoring event or a complete live-read reconciliation. Stale routing, incomplete recovery/catch-up and verifier collapse remain distinct hypotheses; durable data loss is not established. |
| Original growth's 404 operation failures | Failed operations; incomplete classification | **404 is the count of failed operations, not an HTTP status.** Only 20 bounded error details survive: eight connection-reset, seven temporarily-unavailable, four recovery-not-complete, one no-reachable-member. The remaining 384 lack detailed responses. A full per-attempt gateway/range/generation classification cannot be reconstructed from this evidence. |
| Original 300-second convergence observation | Failed convergence; mechanism inconclusive | 68 status samples retained ongoing topology/relocation changes and repeated failed moves; no completed rebalance. Activity alone proves neither bounded forward progress nor oscillation. Cleanup-confounded stopped states cannot establish the runtime cause. No steady-five capacity comparison ran. |
| Native metadata heartbeat catch-up and mid-round membership retirement | Passed, focused unmerged candidate | `f1f7ebdf96b737c8f9c192b2e7831ee108abe6f6`, [run 43181969102](https://github.com/nikoma/bicdb/actions/runs/43181969102): three passed / zero failed / 860 filtered out. Actual TLS 17/65-barrier backlog and retirement controls; full matrix skipped. Not operator scheduling, physical failover or full-candidate qualification. |
| Native learner enrollment positive/cancellation/pending retry | Passed, focused unmerged candidate | Same source, [run 43181974286](https://github.com/nikoma/bicdb/actions/runs/43181974286): three passed / zero failed / 860 filtered out, actual TLS. Registered identity/birth lifecycle is not data Serving or completed expansion. |
| Current-quorum image substitution | Failed candidate; correction not yet qualified | `ba868e9ff6780e98d4d799f5cbd83bacf0fdb210`, [run 43183211113](https://github.com/nikoma/bicdb/actions/runs/43183211113): zero passed / six failed before substitution assertions because quiescent preflight ran under its own counted write lease. A source correction retains all guards; its integrated runtime coverage is still required. |
| Complete joint handoff, new-leader ACK and Strong read fixture | Not tested at newest correction | `a96d406596991057e6f4167b7185783f06f3c8fd`, [native run 43190505814](https://github.com/nikoma/bicdb/actions/runs/43190505814) and [substitution run 43190512897](https://github.com/nikoma/bicdb/actions/runs/43190512897) failed compilation at three new `Record`/`Arc<Record>` assertion mismatches. No runtime result follows. Corrected source `dc6a13d4c5823c39f16fdfb8e2f135f3d2987a6b` is under fresh focused CI; subsequent fixture corrections also need execution. |
| Operator-driven native four-to-five data expansion, all-owner cold recovery, post-joint NoData and cleanup | Not tested / implementation incomplete | Private lifecycle and proof components do not supply a public Init/Join/Serve chain, remote handoff, restart scheduler or completed cleanup. See the command delta and recovery designs below. |
| New physical RPO/RTO, latency or capacity qualification | Not tested | No new Azure resources or VM experiment. Original trial remains failed. Source/CI work does not reopen its expired spending window. |

## Smallest next tests and open blockers

| Invariant / blocker | Next deterministic evidence required |
| --- | --- |
| Strong-read authority and visibility after leadership movement | Actual acknowledged value immediately after transfer: latest value or bounded retryable unavailable. No successful false absence. Independently observe replica-ready, leader-ready and strong-read-ready; none is inferred solely from election or topology. |
| Stale gateway route | Hold an actual stale route across promotion, record its structured refusal, bounded refresh and final read against the same ACK ledger. Preserve request/response bytes, topology generation, range epoch, leader and applied/commit positions. |
| Source fails during image installation | Complete actual current-quorum replacement/resume while target is Installing. Cover nonzero target-native prefix and a higher replacement cut, original cancellation/authority, cold restart, retained archived stage ancestry and all inherited command obligations. No historical receipt may become fresh quorum authority. |
| Joint handoff and post-joint recovery | First execute the complete actual old/new-majority handoff with target as new leader, ACK2 and Strong read. Then exercise NEW-domain Prepare-only/no-effect recovery with fresh NEW quorum and next ACK. Preserve permanent promises and prior Commit obligations; see [post-joint audit](design/native-post-joint-no-data-audit.md). |
| One stalled move versus independent ranges | Stall actual disk/catch-up work, prove other ranges continue, persist bounded progress and explicit refusal reason; restart controller at promotion and cleanup cuts. Distinguish progress, blocked condition, oscillation and controller error from request-time traces. |
| All-owner restart and command lifecycle | Strictly reopen actual birth/namespace/store UUID, term/vote/log and exact original configuration. Drive registration, catch-up, promotion and data activation through operator commands. See [command-path delta](design/native-command-driven-enrollment-delta.md) and [cold-recovery design](design/native-learner-all-owner-cold-recovery-design.md). |
| Long-running availability | Resolve bounded permanent-slot/inventory and archived-history limits with crash-safe reader and persistence contracts. Small tests do not qualify an 8 GiB/long-running relocation workload by avoiding those limits. |
| Physical repeat and capacity | Reviewed, source/binary-pinned candidate; isolated four-to-five VMs; complete independent attempt/ACK/uncertain/failed/retry ledger; explicit pre/during/post-convergence windows; zero lost ACKs or false strong-read absence; bounded convergence. Only a stable five-node window at equal latency/error budget can establish capacity gain. Separate authorization is required before new spending beyond the expired experiment window. |
| Ordinary PostgreSQL application transparency | Implement and qualify gateway routing, same-view cross-range scans/joins and durable recoverable multi-range SQL decisions. [Minimum execution/recovery design](design/distributed-sql-minimum-execution-contract.md) is design only. Tenant locality remains an optional optimization, never a correctness prerequisite. |

GAP-1's structural repair remains merged source, not integrated physical crash
qualification. WAL/index/FTS, queues/audits, backup/restore and interrupted DDL
must retain their separate evidence gates. The production gate is unchanged:
zero lost acknowledged commits, correct read consistency and routing, bounded
fault-safe rebalance, and sustained throughput gain after complete convergence.
