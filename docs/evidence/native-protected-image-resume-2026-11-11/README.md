# Protected current-image resume: exact-source local evidence

The prototype recovered three interrupted image-install states to **NativePrepared/Ready, still frozen**, using a separate fresh Installing quorum and complete source history. This is local protocol/storage evidence. It does not qualify an operator, a replacement source, higher data epochs, changed membership, power-loss recovery, Activation, StrongGet, or production HA.

The source was private `1.0.471-beta`. Final source `4de59df7e36672061a37964bec821988f44975fc` passed four focused tests. Three related regressions passed at `ec19eb3d599bc4735e67e392cff6cc0797a229b3`; the later delta changes only three test files, listed in `claims.json`. Seven distinct passing tests are reported across these **two exact scopes**, not as a seven-test final-head suite. No broad suite was run for this cohort. A source version or documentation publication does not produce an operator binary or deploy the prototype.

| Scope | Actual result |
|---|---|
| Final 4de: clear completed, before row copy | Fresh Installing protocol recovered target R2 with actual target-local MVCC bound B1 |
| Final 4de: BEGIN completed, before history staging | Same positive result without relying on a completed staging/audit receipt |
| Final 4de: catalogue intent persisted, before native Installing marker | Same positive result; actual old native point remained visible but ordinary admission still refused |
| Final 4de: native proposal persisted, before catalogue intent | Orphan remained historical only; ordinary native point, UID and history survived strict reopen |
| Related ec19: global format upgrade | A protected range cannot upgrade the whole catalogue over another range's legacy unprotected intent |
| Related ec19: two nonempty RF3 repair regressions | Same-store repair/adoption and atomic image-to-Prepared transition remained fenced |

Each interrupted-install test starts with a genuine ACK1 and an uncertain command2, creates actual sparse current certification, completes source native history to R2, and stops at the named target boundary. All owners are **orderly dropped**, frozen/pin ownership drains, and the stopped directory is copied and fsynced. Strict reopen reconstructs native birth/schema/UID and metadata state. This is not SIGKILL, abrupt power loss, or an independent-machine experiment. The `tls-replication` feature was compiled; these tests use embedded actual services and simulated peer transport, without exercising mTLS.

After reopen, the ordinary target fence/Ready path still refuses. A purpose-specific observation reports the actual Installing state and protected preimage; it cannot become an ordinary vote or Ready grant. Two distinct current voters and independently fresh metadata support the mixed quorum; duplicate replies are rejected. Complete actual source history must cover all preserved native, pending, raw and sparse obligations. A new attempt copies/verifies data, publishes a native receipt, adopts the catalogue and atomically replaces only its exact image intent with NativePrepared. The original target UID and source history are preserved. Each original ACK1 value reconciles, and command2 resolves without being called a new application ACK. R2 is never used as a database MVCC sequence: the actual target visibility bound is B1. The range remains frozen after RuntimeReady reattestation.

The protected native page-WAL proposal precedes BEGIN, with an exact pointer in the catalogue intent. Its historical source-quorum acceptance is not fresh authority or proof of target compatibility. The new local reader fence and catalogue format protect older readers; an 8 MiB complete protected-record limit refuses before BEGIN rather than truncating. The orphan test injects a pre-catalogue persistence failure, preserves conservative live refusal, and proves that no intent was published on disk. An orphan record alone neither invalidates the native point nor grants authority.

All prior attempts remain here. Three compiler attempts failed before any test; `150-second-cold` accidentally repeated the unchanged `611-first-cold` source and is not a new revision. The first actual ec19 positive passed. A later ec19 group passed three cold cases but its orphan fixture failed because strict reopen had not yet observed the actual schema digest. The final test-only correction observes and compares that real fingerprint; it does not assign a digest or relax the engine. The original failure and both stopped roots remain in custody.

`actual-results.json` preserves outer **COLD_BEFORE_EXPLICIT_RESUME** observations separately from the nested **NATIVE_PREPARED_READY_STILL_FROZEN** result. This avoids reading pre-resume refusal fields as the final state. The mutable libtest executable's captured hash is in `claims.json`; it is not a release binary. Eighteen stopped roots plus logs have private durable custody: 997 files, 17,594,774 bytes, manifest `0607e176907059dfdf602bc75351db7e0bdee12411f3cccb354b1b27c764d822`. Source/destination hashes match and files/directories were fsynced. No engine was opened for custody. Successful pre-existing related regressions delete their roots as designed; no archive is claimed for those two tests.

Verify retained records and reconstruct the exact two core source trees from public `main` revision `f3a063902d2fe316c29264fe20549c2fdd270242`:

```sh
python3 docs/evidence/native-protected-image-resume-2026-11-11/verify.py --repo .
python3 docs/evidence/native-protected-image-resume-2026-11-11/test_verify.py
```

These commands validate evidence and source identity; they do not rerun Rust or certify durability. The exact executed Cargo commands, active RAM/swap/PSI/disk samples, compiler diagnostics and raw test outputs are included. The resource guard caused no pauses or interruptions. Source patches include the private runtime prototype for reconstruction; publication of this evidence does not merge those runtime changes into main.

The earlier unsupported ordinary cold-resume characterization remains a distinct baseline at `8707ffa42fc0b20ff30e0dd5ebd8989e20207a0d`. These positives exercise the same source, data epoch and voter configuration. Source death, replacement source under a higher epoch, actual wire/controller orchestration, joint old/new data quorums, and abrupt-crash testing remain open.
