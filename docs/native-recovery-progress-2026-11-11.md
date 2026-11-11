# Native recovery engineering status — November 11, 2026

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
