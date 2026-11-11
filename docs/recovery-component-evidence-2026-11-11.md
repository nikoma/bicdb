# Native recovery component evidence — November 11, 2026

These are retained local tests of experimental source compositions. Publishing
this evidence does not merge their engine patches, deploy a binary or qualify
HA. The source version of this documentation update is separate from every
historical tested version below.

| Bundle | Exact experimental source | Retained result | Boundary |
| --- | --- | --- | --- |
| [Fresh recovery receiver foundation](evidence/fresh-recovery-receiver-2026-11-11/README.md) | `bcddd35a` equivalent to pinned `9979638e` plus retained patch; package 1.0.466-beta | 44 distinct focused tests, 45 executions | Fresh roles, fences, quorum observations, native local physical proof and mutation ownership; later historical Apply/RPC collection excluded |
| [Native catalogue adoption and historical replay](evidence/native-catalogue-historical-recovery-2026-11-11/README.md) | `9e37d812369de61bb9489ce5415d73d47fdcef35`; package 1.0.471-beta | 38 distinct focused tests | Actual image catalogue adoption and fresh local WAL replay after checkpoint/reopen; recovery still blocks ordinary new writes |
| [Coherent participant source composition](evidence/recovery-participant-compose-471-2026-11-11/README.md) | `5d70669eec9fcea8f9963894ef73d243ef9d208f`; package 1.0.471-beta | 55 distinct focused tests | New recipient RPC collector and recovery-to-installer lease compile; neither new path was exercised in this bundle |

The bundles overlap; their counts must not be added. Each retains commands,
resource samples, warnings, prior failed attempts and source bytes or a complete
patch against a pinned public source. The 44-test bundle includes its exact
source archive; the 55-test verifier can independently reconstruct its source
from public base `b0e2498842be7b01e355bb8da1cc249ff16be056` without modifying the
supplied repository. The source archive includes existing public TLS test
fixtures, byte-for-byte checked against that public base; it contains no live
member keys or cloud credentials. Verification checks retained evidence and does
not rerun Rust or independently attest execution.

Most metadata participants use registered in-process transports. The metadata
provider groups include actual loopback mTLS tests; that does not turn the
historical Apply scenarios into authenticated remote receiver tests. Those
scenarios use actual fsynced native stores, quorum observations and local WAL
callbacks. A declared new metadata epoch/leader in a fixture is not an observed
automatic election. Later source or runtime experiments need separate receipts;
they do not retroactively extend these results.

No bundle establishes physical scale-out, sustained throughput gain, hardware
RPO/RTO or production p50/p95/p99. Actual RecoveryComplete, ordinary new writes
after recovery, serving configuration activation, ordinary remote native Apply,
complete audit/sync/broker effects, StrongGet and fault-safe four-to-five-node
relocation remain outside these claims. General distributed SQL and atomic
multi-range transactions are not supplied by this component evidence. The
[HA/scaling audit](ha-horizontal-scaling-audit-2026-11-10.md) and
[Azure results](azure-scale-out-results-2026-11-11.md) remain the operational
qualification record.

Recheck the self-contained bundles from the repository root:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/fresh-recovery-receiver-2026-11-11/verify.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/fresh-recovery-receiver-2026-11-11/test_verify.py
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/recovery-participant-compose-471-2026-11-11/verify.py --repo .
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/recovery-participant-compose-471-2026-11-11/test_verify.py
```

The catalogue/replay bundle retains its exact manifest, compressed source
snapshots, public-base patch, prior-failure reverse patch and reconstruction
receipt. Its README describes the source reconstruction and the original fixture
budget failure; both the failure and correction remain preserved.
