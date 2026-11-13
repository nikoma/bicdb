# Automatic placement: retiring replica and authenticated initiator

Source baseline751fc28f4 (actualmain492 composed) passed its original ten
registered CLOSED-fence tests. Those tests use a manual Pending allocation
whose retiring source equals the current range leader, and send from that
leader. They do not cover the full automatic planner contract.

`build_rebalance_plan` (`distribution.rs`) chooses `PlannedReplicaMove.source`
by replica load or repair need. `DistributionStore::apply_rebalance_plan` copies
that node into `RangeRelocation.source`: it is the RETIRING replica. It may be
a nonleader. Native joint scope separately calls it `old_source`; the actual
image donor is selected by real OLD closure/cut evidence and can differ.

The strict existing `MetadataRecoveryRoleRequest::validate_configuration`
allows CommittedHandoff initiation by the actual current range leader OR the
actual metadata leader, with exact session/nonce/target binding. The registered
CLOSED-fence capability additionally requires plan.source==initiator==range
leader. That extra assumption conflicts with valid planned retirement and
metadata-leader orchestration. It has not yet been changed by this test batch.

Two REQUIRED-success regressions reuse the actual four INITIAL hosts/TLS,
genuine fifth enrollment/owned MStore/promotion/Registered receiver, original
four writer guards, two complete source APP ACKs, actual native consensus
Pending commit, full per-attempt intent/results and custody. The original two-
fence required assertion and all original 2s/4s/5s API budgets remain.

* The automatic-plan case invokes the real planner for two moves on the actual
  committed membership; it preserves selected ranges, source/target/reason,
  expected epochs and plan identity. At least one real selected retiree MUST
  differ from the range leader. It does not replace that plan with leader-only
  moves. The fixture's committed heartbeat clock supplies planner liveness
  hints, explicitly not physical timing or authority. A test-only Pending
  adapter commits the planner-selected candidates through actual metadata
  consensus. This is not a production automatic-controller test.
* The metadata-leader case uses the actual node0 TLS client/identity as both
  the witnessed initiator and the actual fence sender, while the range leader
  and retiree remain node1/node2. It does not spoof an initiator ID.

Each actual fence must succeed while its original caller/role remain live.
Errors remain required failures, not accepted safety successes. Unsuccessful
fences must leave the target unchanged; source ACK/full rows and original
writer identity checks execute before the final required gate. The original
ten-case mode remains the default; no production path or authority predicate
is changed by this source. No image, rows, NEW quorum, target APP/Strong,
controller convergence, node replacement, physical RPO/RTO or qualification
claim follows from these focused ingress regressions.
