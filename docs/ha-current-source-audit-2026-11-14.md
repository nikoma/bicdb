# HA and horizontal scaling: current-source addendum, November 14, 2026

**Production qualification remains incomplete.** The original Azure scale-out
experiment remains failed. A new single-primary streaming election defect is
recorded in [issue190](https://github.com/nikoma/bicdb/issues/190); snapshot ballot
rollback is recorded separately in [issue191](https://github.com/nikoma/bicdb/issues/191).

Inspected delivered main is **1.0.511-beta**,
`74f17b912cec7f2fe093d10dcf1618a43b35f454`, tree
`38fdbe27eed492a617ed6b176de301f5965967f5`. Its tree equals tested source
`dd61ccce453531fe4f1898810316e0dc36fbeea2`. Its
[required CI](https://github.com/nikoma/bicdb/actions/runs/43590107125) and
[stock Rails PostgreSQL-adapter run](https://github.com/nikoma/bicdb/actions/runs/43590107140)
passed. Its separately built optimized CLI identifies as `bicdb 1.0.511-beta`:
428,212,584 bytes, SHA-256
`a1bf4ebbc16df9e4408d668fa6ca41e571788811eec976a9015d2fcbb1dcf296`.
Source delivery, compilation, application acceptance, physical qualification and
deployment are separate evidence states.

The unmerged **1.0.512-beta** range-recovery candidate is
`b7791177fc94fa8d1ea2072f6b4f62bb659000b6`, tree
`e05d4b742ec49af5c06b75a65eaf4df1685df4df`. Its
[required run](https://github.com/nikoma/bicdb/actions/runs/43601117766) is ongoing
at this checkpoint. All-target/consumer compilation, PGwire's 511 passing tests
and [Rails20 runs/100 assertions](https://github.com/nikoma/bicdb/actions/runs/43601117775)
have completed; no passing native multi-node result is claimed. The CI
checkout `50a1359db62f58d7d88f2889fbb06d6fb53cc5dd` has the exact candidate tree.
This is an immutable source/result checkpoint, not a live CI dashboard.

## Evidence levels by deployment mode

Implemented means the specified code path exists. CI-tested means an identified
scenario actually executed; a compiled API or a test definition is insufficient.
Physical evidence states its host/guest and revision limits. Qualification means
the complete release-specific production gate passed. Deployed means an
operational instance and its configuration were independently verified.

| Mode/profile | Current implementation | Test evidence and limit | Physical evidence | Production-qualified | Verified deployment |
| --- | --- | --- | --- | --- | --- |
| 1: legacy single-primary file shipping | Ship/status/promote, with explicit fencing and redirection | Retained durability/crash regressions; no new physical run here | No current-main RPO/RTO campaign | Not established | Not established |
| 1: native single-primary synchronous/asynchronous streaming | Ordered streaming, voting-only ACKs, elections/lease fencing; zero-loss claim blocked by issue190 | Configuration/election/bootstrap/kill regressions exist and required main511 CI passed; partial higher-epoch catch-up followed by another election is not covered | Historical limited failover results do not qualify511 | No; source safety blocker plus incomplete qualification | Not established |
| 2: generic/legacy distributed range controller | Metadata consensus, per-range quorum, automatic placement and relocation | Retained functional and shared-host tests; ordinary SQL integration remains incomplete | Original461 Azure run showed some automatic movement; scale-out qualification failed | No | Not established |
| 2: native distributed data/recovery path | Genuine physical owners, OLD/image/NEW/decision components and local continuation; automatic operator closure incomplete | Candidate b779 compiles; previous4ef native run failed307/23/one ignored; new runtime results pending | No new pinned four-to-five native campaign | No | Not established |
| 3: Cell-scoped HA | Signed/encrypted Cell authorities, lease/fencing and authorized recovery; Phase5 asynchronous positive-RPO contract | Authority tests and synthetic signed drill fixtures; an admission verifier is not an operational drill | No current release-level recovery drill established | Not established | Not established |
| 4: application fleet | Worker placement, leases/reassignment and single-writer state groups | Retained multi-process loopback and lifecycle scenarios; application workers do not create distributed SQL capacity | Independent-machine throughput gain not established | Not established | Not established |

The complete [maturity audit](ha-horizontal-scaling-audit-2026-11-10.md) retains
the configuration and subsystem boundaries. The
[original Azure forensics](azure-original-run-forensics-2026-11-11.md) preserves
exact failed evidence: **404 was a count of failed logical operations, not an
HTTP status code**. Its two unverified ACK reads were later matched, but the
original responses/routes/read-time watermarks and causal visibility-restoration
event were not retained. Their exact causes remain inconclusive. The 300-second
convergence check failed; bounded useful progress versus churn was inconclusive.
The Azure observations concern distinct VM guests. Independent physical-host or
device-power-loss resilience was not established.

## Streaming election: configuration fix and a separate open defect

Main511 rejects an automatic five-voter configuration with nonzero synchronous
K=1. `HaServerConfig::validate` checks the current and every future-primary role;
K must reach the configured majority and only voting peers count toward ACKs
and the liveness lease. K=0 and timeout degradation deliberately permit
asynchronous acknowledged-loss risk. The original arbitrary-K configuration
finding is therefore closed at source, not physically qualified.

The separate issue190 counterexample still applies to a valid five-voter/K2,
timeout-fail configuration. A heartbeat calls `record_primary` before applying
its bounded batch; that function immediately raises persisted `data_epoch`.
`consider_vote` compares `(data_epoch, applied_seq)` lexicographically. A new
primary at sequence4096 can consequently label a standby with its newer epoch
after only512 commits, and that standby can later outrank an old-epoch voter
holding4096 acknowledged commits. Read catch-up readiness is not consulted by
elections. Two sequential primary failures expose the unsafe schedule.

This is a **confirmed source counterexample; runtime reproduction and physical
data-loss testing are not performed**. The issue contains the exact schedule,
pinned source links and next deterministic tests. A safe correction must bind
voting history to actual accepted contiguous durable history, handle old
mislabeled persisted state conservatively, preserve legitimate divergent-history
ordering, and reject unsafe old/new protocol mixing. Merely moving the epoch
assignment after a partial batch, comparing sequence alone, or taking the maximum
of all announced epoch starts does not establish that invariant.

Relevant source: `crates/bicdb-pgwire/src/high_availability.rs`,
`crates/bicdb-core/src/db/ha_node.rs`, `ha_bootstrap.rs`, and `switchover.rs`.
Existing regressions are in `crates/bicdb-core/tests/live_replication.rs`,
`crates/bicdb-pgwire/tests/embedded_ha.rs`, and
`crates/bicdb-cli/tests/ha_failover_cli.rs`. Passing those existing tests does
not resolve an uncovered failure schedule. This issue is independent of the
virtual-range and Cell protocols.

Issue191 identifies a separate source-level snapshot race. Staging captures the
target's ballot, but a peer can persist a later vote before shutdown. Installation
does not compare the latest target HA state before replacing it, allowing that
later ballot to disappear. The deterministic stage→vote→reopen schedule and
required recovery controls are retained in the issue. No runtime double-vote or
data-loss experiment was performed. Protecting this path requires install-time
authority/data checks; blindly merging ballots can still overwrite data after a
promotion. A refusal of changed state preserves safety but does not establish
automatic bounded recovery. Old/new installer compatibility must account for
bootstrap installation occurring before the ordinary format-reader check.

## Native range components and the missing automatic operator

Candidate b779 adds genuine protocol implementations beyond delivered511.
The older [operator-closure review](design/native-operator-scale-out-closure-2026-11-13.json)
must be read with these source-specific corrections:

| Boundary | Actual candidate b779 state |
| --- | --- |
| Native placement/capacity loop | Missing. CLI/operator maintenance still drives CONFIG enrollment and initial-registry refresh. |
| Source Start/Describe | Authenticated implementation exists. Initiating callers are fixtures. |
| Target image work | Genuine bounded operations and retained owners exist; process maintenance does not schedule their execution. |
| Local joint/Serving continuation | Substantial actual-service implementation exists; its host adapter/driver remains fixture-only. |
| Remote phases | Materialize, Observe, PREPARE and native DECISION exist. Remote catalogue decision, Serving preparation, activation, Open and ALL-release are not connected. |
| Retirement and cleanup | Export-reference retirement exists. Complete OLD retirement, physical deletion and terminal convergence remain missing. |
| Serving and restart | Local Registered-to-Joined publication exists. Automatic operator publication/status and crash-resumable orchestration remain missing. |

Concrete source/caller evidence:

- `crates/bicdb-cli/src/native_metadata_command.rs:553` runs the500ms maintenance
  loop, explicitly limited to CONFIG enrollment.
  `crates/bicdb-core/src/distribution_native_host/operator.rs:280` calls
  `maintain_joins`;
  `crates/bicdb-core/src/distribution_native_host/provisional_listener/registration/maintenance.rs:20`
  ends at metadata promotion.
  These files are byte-identical between delivered511 and candidate b779.
- `crates/bicdb-core/src/distribution_native_host/joined_data_handoff.rs:31`
  explicitly excludes a production caller and cold restart. Its only
  `NativeJoinedDataHandoffHosts` implementation is the `Five` fixture in
  `fifth_data_path_tests.rs:241`. Local service/State/OLD owners cannot become
  remote capabilities through a TCP proxy.
- `crates/bicdb-core/src/distribution_transport/native_source_discovery.rs:43`
  implements Start/Describe; initiating calls are in the discovery/retirement
  fixtures. `crates/bicdb-core/src/distribution_range_handoff/joint/remote_new.rs:62`
  ends at `PublishDecision`, explicitly
  excluding catalogue vote or Serving.
- `crates/bicdb-core/src/distribution_range_image/remote_source.rs:1019`
  retires typed OLD custody, with its initiating caller
  in `hot_policy_tests.rs`. Registry admission still has an eight-session cap
  and no session-removal producer. `AwaitingCleanup`/`Cleaned` definitions have
  no producer that advances a relocation through those stages.
- Generic balanced supervision cannot bypass the native contract:
  `crates/bicdb-core/src/distribution/native_joint_handoff.rs:468`
  refuses protected membership/cleanup mutation;
  `crates/bicdb-core/src/distribution_transport/initial_data_listener.rs:94` refuses generic
  snapshot/import/cleanup operations.

Thus a newly joined native metadata node is not automatically a materialized,
Serving data node. The next remote protocol dependency is recipient-owned
catalogue-decision publication/reconciliation over the same retained native
DECISION, followed by genuine remote preparation/activation/Open/release. A
worker stopping safely CLOSED after image installation is useful partial work;
it does not demonstrate automatic fault-safe scale-out.

## Application and qualification gates

Ordinary PostgreSQL applications still lack a demonstrated general transparent
cross-range SQL executor, joins, global snapshot and recoverable atomic
multi-range transaction coordinator. The stable namespace/record-key hash is not
a tenant-locality declaration. Locality must remain an optional optimization;
it cannot substitute for correct cross-range transactions. Stock Rails/local
SQL passes do not qualify distributed SQL.

The original Azure run, local component controls, simulated/synthetic fixtures,
CI compilation and existing APIs are not production qualification. Current
physical RPO/RTO and sustained p50/p95/p99 under fault-safe rebalance remain
unestablished. No sustained capacity gain is demonstrated without a stable,
fully converged five-node comparison at the same workload and latency/error
budget. The production gate remains zero lost acknowledged commits, correct
read consistency/routing, bounded fault-safe rebalance and measured sustained
throughput gain after convergence.
