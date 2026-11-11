# Local native preparation and authenticated receiver components

Experimental source `d440c58ebc962f1e62374d86faecbfcc051e2bbf`, package metadata
**1.0.471-beta**, passed **40 distinct focused local tests**. This is a separate
composition and receipt from the earlier 44-, 38- and 55-test bundles; overlapping
counts must not be added. Its engine patch is retained for reconstruction, not
applied to the published main runtime by this documentation change.

| Group | Passed | Actual scope |
| --- | ---: | --- |
| Local native physical proof/preparation | 13 | Actual physical prefix/history/local visibility, owned read pin, five preparation cases including two persistence-ambiguity cuts, exact-session re-fsync and close/reopen |
| Recovery state | 5 | Durable refusal/tail preservation, retired nonce, actual rename/directory-sync ambiguity and range-scoped catalogue identity |
| Current source/fence quorum | 7 | Actual local metadata/native stores, source history/configuration guards and cancellation |
| Authenticated historical receiver | 3 | Actual loopback mTLS data and metadata RPCs, recipient-owned fresh role/fence/logical decision/current-marker quorum, row/WAL replay, duplicate/stale/one-marker/unwired refusal and latch freedom |
| Owned recovery-to-installer lease | 3 | Actual local owned transfer, refusal on drop/stale scope and independently fresh same-store return observation; component scope |
| Image catalogue adoption | 5 | Real native image/catalogue coupling, preserved refusal, retry, reopen and persistence ambiguity |
| Historical native Apply | 2 | Actual three-service current certificate quorum and post-real-WAL unknown/checkpoint/reopen/fresh-local-WAL replay with original command identity |
| Legacy catalogue compatibility | 2 | Prior v1/v2 migration/checksum contracts |

The new local `NativePrepared` record uses catalogue format 7 and retains an
actual immutable native point under owned physical protection. It remains a
refusal boundary: **it is not a NativeReady quorum, RecoveryComplete, leader
permit, serving configuration activation or StrongGet**. Uncertainty requires an
actual exact-session durable re-sync and a new physical observation; caller
numbers and an expired authority cannot renew the proof.

The authenticated receiver cases execute the collector that was only compiled
in the earlier 55-test receipt. Initial native births have the actual fixture TLS
identities before any rows. Original ACKs use three genuine fsynced embedded
services. The recovery network has actual mTLS listeners/providers on n1/n2 and a
closed n0 endpoint. Metadata leader n2 differs from data leader n1. Node n0's DB
remains retained locally; this is not a whole-host-loss experiment. The metadata
election/anchor and new data epoch/leader are declared fixture preconditions,
not an automatic-election measurement. A held outgoing real peer probe proves
recipient service and DB latches are available during collection. Results remain
row/native-prefix scoped, without complete audit/sync/broker effects.

Every group ran against the same clean source and compiled executable. The first
stderr records a forced compilation from `/tmp/bicdb-native-catalogue-adoption`.
Rust is 1.96.0; release dependencies, core opt-level 0/debug 0, one build job,
incremental disabled and two test threads were used. These are not performance
settings. All commands, result summaries, compiler warnings and RAM/swap/pressure/
disk samples are retained. Minimum sampled available RAM was 22,258,626,560 bytes;
there was no resource pause or interruption. The reported test-executable hash is
retained in `claims.json`; the mutable shared executable was not archived and its
hash was not independently recomputed for this export. It is not a release binary.

No complete nonempty current-voter repair, NativeReadyQQ, RecoveryComplete,
ordinary new write after recovery, ordinary native replica Apply, serving
configuration activation, StrongGet, complete effects recovery, physical
scale-out, production admission or deployment is established. No physical RPO/RTO,
capacity gain or production latency percentile is measured. Future completion
work on a different source tree is explicitly outside this receipt.

Run `PYTHONDONTWRITEBYTECODE=1 python3 verify.py` for exact manifest/result/resource
checks, and add `--repo /path/to/bicdb` to reconstruct the complete engine patch
from public base `b0e2498842be7b01e355bb8da1cc249ff16be056` in a temporary directory
and compare every captured core source file. The supplied repository is not
modified. `PYTHONDONTWRITEBYTECODE=1 python3 test_verify.py` checks verifier guards.
These validate retained evidence; they do not rerun Rust or independently attest
execution, network packet origin or production qualification.
