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


## Executed required failures and corrective successor

Exact80aabf0fa5f25d8da652825e1c7927d67ce77934, run43356111432,
Core131658467851: **0 passed / 2 failed**, 994 filtered, 4.56s; compile/consumer
and formatting passed. Both cases reach the original required two-fence gate,
with four original writer guards and two full source ACKs retained per fixture.

Actual planner output selects range2 with retiree n0, leader/initiator n2,
then range1 with retiree/leader/initiator n1. Range2 is refused by the extra
CLOSED-plan check while original caller/role have 4926/1424ms remaining; target
is unchanged. The range1 fence succeeds. The metadata-leader case uses actual
initiator n0 and retires leaders n1/n2; both fences are refused by that same
check, with caller/role 4932/1430ms and 4931/1429ms remaining; target unchanged.
These are semantic refusals, not timeout/setup failures or evidence of ACK loss.
The two fixtures' four ACKs are not one cluster history. Raw Core SHA-256:
`005ba370da2bc55f6a1ae3b91910a782dc480f36fa076d17f14f12cecdf6f57e`.
Private artifact14006341679 was exported and hash-verified, no extraction or
engine replay; its435073 bytes hash to
`6bdebf5c8f0f580200ed7eee5d78ba065f2a02cf0bbd07ee9453bfc7a376cc00`.

The corrective production successor keeps the retiring-replica requirement,
but proves that node is an actual old VOTER. It no longer equates retirement
with RPC initiation or image donation. Eligible initiation remains enforced
by the existing actual sealed CommittedHandoff role producer: authenticated
metadata leader OR range leader, exact configuration/session/nonce/recipient.
The same OWN-MQ/protected MStore/DB/open/Store/hook/native/no-domain checks,
refusal-only persistence and original callers/roles are unchanged. Source=None
remains outside this first replacement lane. No data or serving grant is added.

An additional negative first reaches the genuine valid capability after BOTH
original fences, then sends the same scope DATA from actual n3 TLS client.
n3 is neither the metadata leader nor range leader, so the real receiver must
reject its fresh role request with the exact existing identity/purpose/config
error and unchanged complete target state. This tests that removing the faulty
equality does not admit every authenticated voter. The corrected coherent
cohort contains thirteen tests: ten original, two placement and this negative.
All require fresh execution; the original80a failed record remains unchanged.
