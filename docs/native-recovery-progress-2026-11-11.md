# Native recovery engineering status — November 11, 2026

## Subsequent source-pinned findings

The original snapshot below is historical. Later component work is indexed in
[the evidence matrix](recovery-component-evidence-2026-11-11.md); counts from
separate revisions overlap and must not be summed. All later engine compositions
remain unmerged private prototypes with source package metadata 1.0.471-beta.
Publishing this documentation does not qualify or deploy those engines.

| Finding / requirement | Classification | Exact evidence and next deterministic gate |
| --- | --- | --- |
| Pure certification-gap refusal poisons persistence status | **Failed baseline; focused fix passed** | `9eace333` fails; `7d2f84a5` passes 22 tests with unchanged catalog bytes after refusal. Retain failure; integrate and qualify the eventual runtime separately. |
| First fence cannot describe 1025 real irreversible commands | **Failed baseline; focused fix passed** | `e68fa21f` fails before later recovery. `d2c20220` passes 21 controls including all hashes 2..1026 and 1025 cold ACK digests; the later separate sparse-decision row records source-tail recovery, with complete target repair still open. |
| Sparse committed decision permits source-tail recovery across a raw gap | **Focused test passed; integration incomplete** | `0d58359b` passes three distinct local tests: real current quorum completes source native/Ready1026 while behind raw/native remains0; all1025 original ACK digests match. Full target image installation, cold sparse reissue and automatic orchestration were not tested by that cohort. |
| Generic catch-up invalidates native history | **Failed baseline; native replacement focused cases passed** | `035fd22f` reports generic repair success while native becomes unknown. Separate `18723cf4` passes actual mTLS/embedded retained-command catch-up and new writes, with explicit replay/authority limits; its five-test cohort does not rerun the original exact before-Prepare2 probe. Cold/membership/compacted recovery remains separate. |
| Activated+Prepared healthy quorum cannot finish old completion | **Failed baseline; focused fix passed** | `e3572fc6` fails; unchanged late-follower test passes at `7b36642b` using the protected prior native decision. Separate `1cd28523` ACKs write 2 on 0+2 and late-activates R1 on 0+1 after node 2's data endpoint stops. |
| Native aborted slot prevents next write | **Failed** | `eafa6949` leaves raw Aborted1/native0 and cannot ACK a second call after restoring quorum. Next test requires actual canonical no-data abort history and real second ACK, without replacing a rollback with an empty transaction. |
| Lagging follower catches up to new R2 after late R1 activation | **Not tested** | The 1cd follower honestly remains R1, has no row 2 and refuses Ready. Add native follower Apply/catch-up and verify actual history and values. |
| Strong reads, bounded end-to-end routing and complete rebalance | **Not tested by these candidates** | Require real serving admission, current authority/read barrier, actual MVCC snapshot, stale-route refresh and controller convergence. |
| Local native observations and authenticated factual read quorum | **Focused component checks passed** | Separate [local9](evidence/native-read-observation-foundation-2026-11-11/README.md) and [RPC15](evidence/native-read-rpc-candidate-2026-11-11/README.md) cohorts. The latter includes six actual RPC tests and nine local tests, remains unadmitted and cannot authorize a successful row or absence. Counts overlap. |
| Native catalogue durability configuration | **Source defect fixed; 28 focused checks passed** | [574ea560 receipt](evidence/native-catalog-fsync-2026-11-11/README.md) rejects actual native fsync-off/foreign-root configurations and preserves declared legacy async. No physical lost-ACK or power-loss result. |
| All-owner cold resume before full image copy | **Failed recovery scenario** | [8707 baseline](evidence/native-interrupted-normal-image-baseline-2026-11-11/README.md): source ACK is intact; target Installing intent blocks ordinary data-quorum fencing, with the third endpoint unavailable. One passing characterization reproduces failure; it is not recovery success or an Azure cause finding. |
| Protected proposal permits same-source cold image resume | **Focused local repair passed; integrated recovery incomplete** | [4de/ec19 receipt](evidence/native-protected-image-resume-2026-11-11/README.md): final4 PASS and separate runtime-identical related3 PASS. Strict copied reopen resumes nativeR2/localB1, preserves ACK1 plus resolved unknown2 and remains frozen/Prepared. Next test must lose source0, freshly certify the existing decision with Installing target1 and behind voter2, recover voter2, and resume from source2 at a higher epoch. |
| Read voters identify the actual enrolled physical store | **Focused component checks passed** | [ee5 enrollment32](evidence/native-store-birth-enrollment-2026-11-11/README.md): actual immutable registry, real wrong-birth refusal, cold factory and actual TLS read facts. Next require writer enforcement from factory creation and all participant policies installed before first ACK; old unenforced histories cannot be blessed retrospectively. |
| Registered host obtains current whole metadata without data leadership | **Focused component checks passed** | [e6 host23](evidence/native-host-metadata-2026-11-11/README.md): actual authenticated requests, membership recheck and original request/deadline binding. Next compose actual startup, immutable enrollment and writer policy. This observation is not a range role, writer lease or strong-read permit. |
| Current operator connects the tested components | **Not implemented/tested by these receipts** | [Exact-source integration audit](design/native-operator-four-to-five-gap-audit-2026-11-11.md): shared Runtime/transport, native startup, real StrongGet, cross-process image transfer, joint-data handoff, controller convergence and cleanup remain operational gates. A later uncompiled composition does not inherit earlier test results. |
| Original Azure temporary invisibility / 300-second nonconvergence causes | **Inconclusive** | Preserve the original forensics and incomplete error corpus; private deterministic failures are not causal attribution. |
| Physical four-to-five expansion and sustained capacity gain | **Not tested on these candidates** | Build and identify the operator artifact before a new isolated campaign. Original failed Azure run remains failed. |

For full reproductions, raw results, failure boundaries, source reconstruction
and restricted raw-root custody hashes, use the linked cohort receipts rather
than this progress table. The protected activation tests are local/loopback
component evidence. No VM/power-loss RPO/RTO, stable-five-node throughput,
p50/p95/p99 or production admission follows.

## Original 72-test snapshot

**Milestone 1 is incomplete. Production admission and the failed Azure
qualification remain unchanged.**

Public `main` was observed at `b037f636ae88e519dc4478e292ed106427155faa`,
**1.0.472-beta**, at 18:23 UTC. That version's additive application-runtime/OIDC
change does not establish distributed recovery or scale-out qualification.
The experimental engine below is a separate, unmerged candidate at
`d52e73726edaedc90d1a5d134f205e99c7c46d0a`, with source metadata
**1.0.471-beta**. A newer documentation publication version is not a newer
qualification of that runtime.

[The portable 72-test receipt](evidence/native-recovery-compose-72-2026-11-11/README.md)
contains the complete engine patch, all Core source hashes, exact commands,
raw results, memory guards and an offline source reconstruction verifier.
The checks are focused local components, including actual filesystem/native-WAL
operations and loopback TLS; they are not independent-machine experiments.

| Boundary | Implementation / observed evidence | Remaining gate |
| --- | --- | --- |
| Replica physically materialized | Actual nonempty RF3 image, target-local B, history, namespace identity and cold reopen tested | Current-release power-loss and large-dataset qualification |
| `NativePrepared` | Actual finalized image proof atomically replaces only its install intent; physical pin and durable write refusal retained | Host/controller integration and operational restart campaign |
| Closed native-ready data quorum | Actual distinct current voters agreeing on R/H0/H; authenticated Prepare/Status tested | Broader failures, production wire lifecycle and membership transitions |
| Native serving-epoch record | Native-WAL write and strict cold reopen tested; physical capture epoch preserved | A serving record alone permits no leader or strong read |
| `NativeCompleted`, still frozen | Actual native-WAL + catalogue persistence; both injected persistence boundaries tested | Authenticated incoming Complete and activation protocol |
| Activated leader | Not implemented/tested by this receipt | Actual completion quorum, fresh authority, persisted activation, freeze release last |
| Strong-read ready | Not implemented/tested by this receipt | Current leader authority, actual read barrier and materialized MVCC snapshot |
| Automatic four-to-five expansion | Original failed Azure evidence retained | New pinned CLI/host integration, complete ledger and actual convergence |
| Sustained throughput improvement | Not demonstrated | Stable five-node post-convergence comparison at equal latency/error budget |

The 72 passes are a single exact-revision cohort. Earlier 34/38/40/44/55-test
receipts overlap and must not be added to create a fictitious independent total.
The candidate has not undergone public-main runtime CI, has no published runtime
binary and has not been deployed. Documentation CI validates the publication,
not the experimental engine patch.

## New deterministic integration blocker

A subsequent, separate test at `1978a0ea` constructs an actual native serving
record A while writes remain fenced, releases the old pins, obtains a fresh
recovery nonce, and installs/verifies/adopts/finalizes image B. Both images have
R1 and the same logical history. The namespace identity and old serving record
remain. The second readiness factory then fails with:

```
recovered serving record does not bind the actual same-voter physical/native domain
```

Classification: **failed availability/integration test in the unmerged
candidate**, not a current-main or Azure causal finding. Image B's bounded native
contents verification passed. A separate ordinary second-image MVCC read was
not measured. No irreversible data-loss conclusion follows.

The complete three-database roots, source-pinned failed attempts, sidecars and
findings were copied with byte parity and fsync to private custody:
`native-serving-image-witness-1978a0ea`, 118 files, 4,550,410 bytes, manifest
SHA-256 `d1e10ab900e82f49bf95f677ae143d98d9c4ffeaf71cd8fe716b54ffef2dbdf8`.
An earlier fixture accidentally collected a quorum at the wrong recipient; its
correct rejection is preserved separately. It did not establish the old-base defect.

The smallest next test is the same two-attempt case after a fix that retains A
as a historical epoch floor, refuses it as B's current serving grant, and requires
an actual fresh closed quorum before publishing B's grant. It must also reject
lower epochs and stale ordinary-origin admission. Deleting the old record or
inferring permission from an epoch counter is not an acceptable repair.

## Qualification boundaries that remain open

The NativePrepared, completed-frozen and strong-read states are distinct. A
metadata-recognized leader may still be physically behind. No successful strong
read may infer absence from that condition; it must establish its actual barrier
or return a bounded retryable-unavailable result. This candidate has not yet
implemented and qualified that complete serving contract.

The selected record tests do not establish secondary-index, FTS, queue or original
signed audit/sync/broker effect parity. The original effect-envelope gap remains
[issue #148](https://github.com/nikoma/bicdb/issues/148). General cross-range SQL,
joins, recoverable multi-range business transactions and tenant-locality policy
remain outside these storage-component results. Ordinary PostgreSQL application
scale-out remains unachieved.

The Azure resources were torn down after the original failed runs. This work
provisioned no new VMs. The original USD25/deadline authorization is unchanged;
a new campaign must wait for the actual candidate and a renewed budget/window.
No physical RPO/RTO, sustained capacity gain, or physical p50/p95/p99 is claimed.
