# Current-source native HA and structural recovery checks

Source: **1.0.460-beta**, commit
`c173fa783fb8323fe02c41fb7eda96f130bc29c3`. These are freshly executed,
source-scoped regression results on 2026-11-11 UTC. They are not release-level
physical qualification, a deployment, or a passing four-to-five-node scale-out
experiment. No runtime source or application assertion was changed for this run.

## What changed since the first audit

Merged PR132 (`92b8456d`, source version 1.0.457-beta) repairs the previously
reported native HA voter/acknowledgment risk. Source `HaServerConfig::validate`
rejects a nonzero synchronous K below `floor(N/2)` for an N-voter automatic
failover group, validates both the current and future-primary settings, and
rejects invalid or duplicate identities. Explicit K=0 remains asynchronous.
`SyncQuorum::restrict_to_voters` excludes allowed observer replicas from commit
confirmation and writer leases while retaining their replication positions.
Observer election/epoch requests are refused. Native HA handshake **v4** refuses
older peers that could restore the unsafe counting behavior.

This repairs implementation and configuration validation. It does not qualify
all network/storage faults or guarantee that an asynchronously acknowledged
commit survives an election. Timeout policy `degrade` still permits asynchronous
acknowledgments. All group members must use the same static voter membership;
changing it requires coordinated stop/reconfiguration rather than a native HA
membership-change protocol. Distributed virtual-range membership is a separate
implementation and is not covered by these native-HA results.

## Results

All **78 test functions passed**, with **zero failures, ignores or filtered
tests**: 8 HA durable-state, 7 HA crash, 32 live-replication, 2 core GAP1,
19 page GAP1 and 10 embedded-HA tests. The two builds and all three test
commands exited 0. Minimum sampled available RAM was 20,469,243,904 bytes
(about 19.1 GiB); no monitor pause was needed. The reusable build cache is
recorded by byte count in `results.json` (4,361,378,097 bytes, about 4.1 GiB). No superseded owned
output required deletion. Production services were not stopped or changed.
Test duration is not application RTO or latency.

## Test scope

| Cargo target | Executed scope | Persistence/failure boundary |
|---|---|---|
| core `ha_state_durability` | Votes, fences, drain/promotion, file-shipping apply/promotion, bootstrap install, catalogs and planner statistics | Both embedded and `server_paged`, fsync enabled; deterministic failures at write/sync/rename/directory-sync |
| core `ha_state_crash` | Acknowledged votes/fences/drain/promotion/catalog state, interrupted ship refuses ordinary promotion | Task-owned child aborts and emulated volatile unsynced directory entries; external child ledgers; **not** hard VM/host/device power loss |
| core `live_replication` | Ordered stream, retention, promotion, snapshot install, stale writer refusal, observer quorum exclusion | Both embedded and paged cases; common fixture disables fsync; algorithmic/protocol regression evidence |
| core `gap1_page_only_batches` | Mid-split recovery retains acknowledged rows; oversized index/FTS backfills complete through structural cuts | Paged store with small pool, fsync disabled; injected failures/reopen, not device durability |
| page `gap1_no_steal` | Eviction/drain/root-split, aborted/pending oversized work, xid reuse/free-page reuse and half-mutated-operation fencing | Page-level cuts/reopen, fsync disabled; verifies logical closure/visibility, not physical power-loss survival |
| pgwire `embedded_ha` | Real loopback replication, voter validation, observer stream/authority refusal, failover/rejoin/journal identity | fsync enabled through host configuration; embedded hosts and graceful shutdown/reopen; no independent physical hosts or process SIGKILL claim |

Run `python3 docs/evidence/native-ha-current-2026-11-11/verify.py` to verify
the retained file hashes and the complete raw test-result counts. This verifier
does not manufacture a physical qualification result.

Exact commands, exit codes, test summaries, executable SHA-256/byte identities,
source/configuration hashes, raw logs, resource samples and reproduction runner
are retained in this directory. The results file distinguishes target counts
from individual crash cases. No simulated fault is relabeled as a physical
power cut. Expected injected `durability.write_failed` diagnostics remain in
raw logs; successful assertions do not erase those events.

## Reproduction

Use the exact source in a fresh worktree. Preserve other agents' binaries and
services. The runner samples available RAM, swap, memory pressure and disk free
space every two seconds; it pauses only its own process group below 10 GiB RAM
headroom and resumes above 12 GiB. Disk free below 5 GiB terminates only its own
process group. It uses two Cargo jobs, disables incremental compilation and
omits test/dev debug symbols. These limits are build safeguards, not production
performance settings. The retained `run_monitored.py` is the exact runner used here. It takes a label
before the command and uses the fixed task-owned `/tmp/bicdb-ha-current-evidence`
and `/tmp/bicdb-ha-qualification-target` directories. Use fresh output/cache
paths for an independent rerun; do not overwrite retained evidence. The commands
below reproduce the Cargo scope without requiring that wrapper.

```sh
export CARGO_TARGET_DIR=/tmp/bicdb-ha-qualification-target
export CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
cargo build --locked -p bicdb-core --example ha_state_child
cargo test --locked -p bicdb-core \
  --test ha_state_durability --test ha_state_crash \
  --test gap1_page_only_batches --test live_replication -- --test-threads=2
cargo test --locked -p bicdb-page --test gap1_no_steal -- --test-threads=2
cargo test --locked -p bicdb-pgwire --test embedded_ha -- --test-threads=1
```

The build starts from defaults for each selected package; no Cargo feature is
removed to obtain these results. Build/test profiles are unoptimized with debug
symbols disabled. Test executables are not a release server binary. A later
source-version metadata bump or integration commit does not change the source
revision actually exercised here. Reusing this evidence must retain that scope.

## Gates still open

- Exact optimized integrated server binary, not just source-pinned test binaries,
  requires independent acknowledged-commit ledgers and repeated real VM resets.
- Independent machines, mTLS partition/blackhole trials and data-node/metadata
  leader loss during all relocation phases need their own evidence.
- No current-artifact physical RPO/RTO, application p50/p95/p99, sustained
  throughput gain, scale-out, full FTS corpus, queue/audit relocation, backup/
  restore or deployment qualification is supplied by this test batch.
- This is not a complete workspace/application acceptance run, five-voter live
  failure campaign or proof of general cross-range SQL transactions. Five-voter
  acknowledgment policy is configuration-tested; live loopback failover uses a
  three-voter group.
- Historical GAP1 and interrupted-DDL failure artifacts remain valid for their
  original sources. These focused tests do not replace a new interrupted-DDL
  hard-reset campaign or erase earlier failures.

A passing unit/integration test and a correct refusal are valuable evidence;
neither establishes the production recovery-time objective. Full qualification
must bind the exact binary, effective configuration, topology, fault mechanisms,
workload and externally acknowledged outcomes.
