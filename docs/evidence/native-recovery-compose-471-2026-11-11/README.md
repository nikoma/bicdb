# Current-main native recovery composition: focused evidence

Exact private candidate `da378dd9baa2a788fd2638adeb777a32ed7ab5d6`, package
metadata 1.0.471-beta, contains verified main `b0e2498842be7b01e355bb8da1cc249ff16be056`
and its table/routine fixes. **56 distinct focused Rust tests passed** on this
unchanged source and executable. This is unpublished integration work, not a
release, merged feature, broad regression result, production binary or deployment.

| Group | Passed | Scope |
| --- | ---: | --- |
| Actual recovery/source quorum | 7 | Five real receiver fencing scenarios plus full actual genesis history verification/current-voter capture and poisoned-cancellation retry |
| Local physical readiness | 8 | Actual local rows/WAL/visibility domain, history mismatch, catalogue/path/open and owned-pin refusal |
| Actual RF3 service origin | 1 | Three actual fsynced services give real Prepare/Commit/Certify ACKs; origin row/nativeR1; remote CQ1/nativeR0 and bare Apply refusal |
| Target namespace/floor/restart | 3 | Same-attempt recreated UID refusal, installed R1 irreversible floor across invalidation/reopen, and original interrupted retry |
| Actual recovery state | 5 | Durable refusal/retired sessions, injected post-rename fsync ambiguity, unchanged-scope re-fsync, unrelated-range/provenance stability |
| Existing event routing | 6 | Actual local TCP silent-route/discovery retries and independent healthy route |
| Recovery runtime | 1 | Actual Normal E2 to exact Handoff E2 session-owner refinement and stale/changed-core refusal |
| Metadata provider | 16 | Fresh-role scopes, bounded/cancelled/quorum probes, and actual local mTLS metadata-role cases |
| Mutation gates | 9 | Owned freezes, controlled writers, source pins, cancellation and lifetime exclusion |

The initial routing filter was misspelled and matched ZERO tests. Its zero-test
output is retained and excluded. The corrected filter ran the actual six tests on
the same unchanged source. No test count or guarantee comes from the zero match.

The first job forced compilation from the intended worktree (actual Cargo path
retained) and took 81.021 seconds including compilation and seven test executions.
The remaining jobs reused that executable on unchanged source. All Rust jobs
returned zero without resource interruption. `summary.json` records exact minimum
RAM, test names and executable hash/size; resource samples retain available RAM,
swap, PSI and disk headroom. Core test optimization/debug settings are zero;
release dependencies do not make these optimized production benchmarks.

Actual data-quorum calls in the RF3 origin fixture invoke real local durable data
services through an explicit trusted in-process caller transport, never canned
ACKs or a supplied certificate list. This does NOT prove authenticated remote data
RPCs. The metadata-provider group separately exercises actual loopback mTLS; it
does not qualify the data-plane source transfer or membership activation.

The source-history factory test is an actual RF3-configured EMPTY genesis source:
a current fenced majority includes an unknown physical floor, and a verified real
source permits current-voter capture without pretending that the unknown voter
was ready. It does not yet test a nonempty current-quorum compatible repair cut,
an authenticated divergent-history schedule, or repair completion. Nonempty R1
RF3 origin/CQ observations are demonstrated by the separate characterization only.

The target tests preserve the actual installed R1 floor/history after physical
invalidation, and refuse reusing an attempt against a recreated namespace. They do
not grant strong-read readiness. Remote native Apply remains refused: successful
origin ACK/nativeR1 alongside remote CQ1/nativeR0 is explicitly observed, not hidden
or called successful follower catch-up. Full historical participant RPCs/native
Apply, cold intent/session rebinding, configuration activation/ancestry, ordinary
replica materialization and StrongGet remain open.

The separate R1025 image result at `8a7ddf6e` remains historical evidence at its own
source. It was not rerun here and is not counted among these 56 tests. Its byte
coverage must not be attributed wholesale to this newer composition.

No full audit/sync/broker effect guarantee, secondary-index/FTS application parity,
SQL joins or multi-range business atomicity, physical RPO/RTO/percentiles, sustained
five-node throughput, power-loss behavior or cloud deployment is established.
The original failed Azure evidence remains unchanged.
