# Retained completion continuation while a later origin is Pending

This source change follows required-test leaf
`de143dab28a34a2cea6726c4cd1f9574a5709e4b` and bounded-custody leaf
`0175a151117d35e5220de519e7476d0c96da6d57`, both on frozen
`ff8f10fdbf6a244ca6dda0c436e3ab8e46b0f4ed`. It is a proposed repair, not a
passing test or qualification result. No local engine build, CI dispatch, VM,
benchmark, or production deployment was performed for this source leaf.

## Actual failure shape and retained obligations

The required test retains original three-node TLS owners. One actual target is
still native2/NativeCompletedFrozen after the first recovery. The source has
acknowledged actual commands through R >= 4, and its surviving peer has the
same complete native head. A second actual pre-WAL Unknown leaves the source
at native R/Pending(R+1), the surviving peer at native R+1, and the old frozen
target still at native2. Original request/phase budgets and the final 20-second
availability gate remain unchanged.

The old source decision at two, its physical opens, uncompleted recipient,
mutation ownership, already acknowledged commands through R, and exact newer
Pending(R+1) are independent constraints. Cached phase completion cannot retire
those constraints. Nor does this require every old session to survive forever:
a genuinely complete new-session quorum/native/activation/read proof can
supersede a retained session after recovering all applicable obligations.

## Separate proof lanes

`KnownPendingNativeReplicaPrefix` is sealed, non-Clone and non-serializable. It
retains the actual DB and Store open, private native prefix and exact durable
Pending. Its constructor requires current catalogue custody, fsync/root policy,
original installed writer, actual native ancestry/namespace/schema validation,
exact next same-epoch committed command, exact V2 origin scope and native image
generations, no materialized/recovery-attempt Pending, and actual release of the
original local transaction scope. It has no public SourceLineage getter, row
capture permit, serving receipt or Ready conversion.

The complete native protected-slot inventory must match the original Store
mirror. This first continuation lane requires every covered old slot to have
its exact immutable Applied history, and the only higher slot to be the exact
next committed Pending. Missing slots, competing variants, abort promises,
no-effect records, resolved-abort receipts, other higher tails or different
Store/DB opens fail closed. No slots are deleted or synthesized. Other valid
history shapes remain unsupported by this narrow lane.

The donor can replay only commands already materialized at or below R. Every
step still requires a real retained raw Applied command, current role and
original OWN/Slot/writer, actual remote recertification including the target,
a real certificate, actual native apply, canonical remote ancestry and checks
after the RPC. No DB/service guard or mutation permit crosses network I/O. The
exact source Pending is revalidated after each local use.

A surviving peer may already be at R+1. It is counted as physical progress only
when its actual authenticated H(R+1) equals canonical Applied(exact actual
Pending command) extending the source's immutable H(R). This does not claim
local materialization of R+1, release its Pending or authorize a strong read.
Remote tails beyond that exact command remain refused.

`HistoricalPendingNativeActivationFacts` is a separate read-only projection,
not a current serving/activation receipt or Active-fence quorum. Under the
actual native journal it verifies the old actual catalogue activation against
both current and immutable native activation/serving records, actual canonical
ancestry, fresh observer configuration/member credentials/schema/restore
identity, native birth/open and the protected Pending constraints above. It
projects only the existing historical ActivatedAnchor reply. The ordinary
current-source, current-serving, read-readiness and Active-fence getters retain
their existing Pending refusals.

The worker continues actual old debt while readiness is false. Once the old
target has actually completed/activated two and caught up through the materialized
R, the next genuine recovery can resolve R+1. Prior-session custody is retained
through every fallible successor operation and retired only after its actual
new native/activation/read-quorum publication. No status boolean grants access.

## Evidence classification and next checks

| Finding or check | Classification for this leaf |
| --- | --- |
| Original source scheduling can stop both old debt and new recovery after a second Unknown | Source-inspected; required red not yet executed |
| Actual partial-debt fixture reaches its required preconditions | Not tested in this session |
| Required 20-second recovery/convergence gate after this repair | Not tested |
| Current-serving getters and Active-fence constructors are unchanged | Source-inspected |
| Formatting, source-version identity and whitespace | Run separately and reported with the frozen commit |
| Physical HA, Azure scale-out, production admission | Not tested; original failed Azure qualification remains failed |

The smallest executable check is the existing required selector
`native_hot_peer_second_unknown_while_prior_completed_replica_debt_requires_full_recovery`,
first at `de143dab` to retain a genuine red only if all actual prerequisites
were reached, then at this repair with the same owners, workload and budgets.
The original repeated-Unknown and canceled-successor-custody selectors must
also run unchanged. A red prerequisite is inconclusive about the final gate,
not permission to remove the prerequisite or extend the budget.

Next deterministic negative cuts are removal/conflict of either immutable
activation/serving archive, retained Pending scope/generation mismatch, an
original transaction scope still held, replacement of the actual Store slot,
a higher protected tail, an abort promise and a mismatched remote H(R+1).
Each must refuse this continuation without erasing debt or granting a read;
a later valid repair may recover through a separately complete quorum protocol.
