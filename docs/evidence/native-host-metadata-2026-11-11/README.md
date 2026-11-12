# Authenticated whole-metadata observation: local evidence

Exact private prototype source `e6a4d6368fb961b6da0d13394e4bc0902b7d6361` (`1.0.471-beta`) passed **23 distinct tests / 24 executions**: seven host-specific tests and seventeen provider controls overlap on one held-probe test. Six host tests use real loopback mTLS; the seventh uses three actual durable metadata stores. These observations provide current metadata to an authenticated Active member, including a follower or unassigned learner. They do not grant a metadata vote, range leadership, recovery admission, native writer permission, or StrongGet.

The two final runs and exact test IDs are in `actual-summary.json`. Actual cases cover leader self-listener acquisition, a follower, an unassigned learner, missing/spoofed member identities, request relabeling, cancellation/delayed decoding under the original budget, and membership changes committed while a quorum probe is held. Existing provider cases exercise fresh quorum/deadline, fanout limits, silent peers, role scoping and real mTLS acquisition. Final libtest identity was captured as SHA-256 `fe0cf9ac85edf9502607ca58ad253277ff771248b435dea24cad813a19214f53`, 137,874,976 bytes; the shared path is mutable and this was not an operator binary.

The earlier source `a838b1b58593f0dabcf4215df28b09d5ce713354` passed five host and sixteen provider controls. Its no-default-features check failed with six unguarded TLS-constructor references; the diagnostics remain included. Corrected source `79593853e9a25122fc59c64355a0d9dca8fb43d8` passed that check. The later e6a delta adds only test coverage; the earlier compile-check result is explicitly scoped to 795, not relabeled as execution at e6a. No broad engine suite was run for this cohort.

A bounded independent source review found no concrete blocker in this acquisition path. The read-only findings and exact API boundaries are in `source-review.json`. The transport seals the actual peer, original request, cluster, destination, start instant, deadline and cancellation token in a non-deserializable reply. An opaque observation can be constructed only from that reply; the response DTO alone cannot produce one. The provider validates actual membership/incarnation/pins both before beginning and immediately before finishing a fresh metadata quorum, using the existing bounded context/worker pools. A learner may request metadata but is not added to the voter set. Host admission deliberately tolerates data-range epoch changes while still requiring a fresh metadata authority round; no generic range/role conversion is exposed.

This review is a source assessment, not an additional dynamic test or a safety theorem. Host observation returns whole metadata, not a durable lease or application read permission. Later writer/read admission must independently validate the needed authority and lifecycle state. Numeric metadata destinations avoid unbounded DNS resolution in this path; filesystem/CPU scheduling and outer host process failure are not physical deadline qualifications.

No fixture DB roots or independent ACK ledger were retained for this host cohort. Raw logs, exact commands, sampled available RAM/swap/PSI/disk and source reconstruction are retained. These are loopback tests within one controller process, not independent Azure VMs or a production deployment. No native writer policy, StrongGet, operator qualification or published binary is claimed.

```sh
python3 docs/evidence/native-host-metadata-2026-11-11/verify.py --repo .
python3 docs/evidence/native-host-metadata-2026-11-11/test_verify.py
```

Verification hashes and checks the retained records and reconstructs exact final/check source inputs from public main revision `f3a063902d2fe316c29264fe20549c2fdd270242`. It does not rerun Rust. Publishing this documentation does not merge the enclosed private runtime patch into main.
