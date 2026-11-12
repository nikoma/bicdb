# Native read foundation: separate delivery preparation

This branch prepares one foundation delivery, separate from the full native
Milestone 1 and four-to-five operator goal. It is not a tested release, native
operator binary, deployment, physical qualification or completion of that goal.
Root owns the final PR rewrite, version selection, full CI, main merge and push.
No version is reserved by this document.

## Exact source selection

| Input | Revision | Role |
| --- | --- | --- |
| Foundation base | `14a793e8c6c2179b1585562c25f4e41b7ca956d3` | Root composition containing main481, `25a` foundation and subsequent cold/open fixes, fifth-factory/cancellation/committed-leader publication, and no-default-feature gates |
| One test compile correction | `be1345d3cfeed7b416a9e60af6547633b9b30114`, cherry `dc24ae4321e9ccc3c8067b250df2a80d11d7d291` | Remove `drop(tx)` after consuming `Transaction::commit(self)` in the new factory control; no runtime guard changes |
| Optional focused CI workflow | `de5c48acda2e9486182589758df133f889c51b3a`, cherry `bb30eef214b81fa20615848dfe859de5d065a009` | Manual nonempty Core substring selection with source/list/run artifacts and explicit zero-selection refusal; normal PR/push and empty-filter dispatch keep the broad suites |
| Isolated consumer compile | `94c0add89cd7e66743556988671ebdbd4bb7df84`, cherry `2e30d0175a1cc4619a65c28ad7e31bcc0f3e6550` | Check `bicdb-workflow` separately so workspace TLS feature unification cannot hide a no-default consumer defect |

The working source retains version1.0.481-beta and data protocol24 from the
base. No later Abort, joint-data handoff, learner-birth RPC25 or their ancestry
is imported. Earlier component and CI evidence remains tied to its original
exact revisions; it is not a passing result for this new branch.

## Scope and dependency gate

The selected source supplies native identity/durability and metadata ownership
fences, current native read observations and consuming Strong point-read
components, enforced writer enrollment, same-voter recovery/image components,
actual-open checks, and provisional factory/bootstrap surfaces. Their explicit
mode, lineage and actual-local-visibility checks remain intact. A source API or
local component test is not operator integration. Existing legacy contracts and
historical evidence are preserved.

**The delayed pre-binding metadata Append ACK provenance correction is still a
required dependency, not included or validated here.** A reply produced before
native metadata binding must not later become a countable current vote because
its recipient now has another bound identity. The coherent correction must tie
countable replies to the actual receiver birth/incarnation/protected metadata
StoreID and the applicable committed domain. Preserve authenticated destination,
original request/deadline and actual-open checks. Infra owns that correction;
Root must integrate its narrow dependency closure and coordinate any wire fence
before freezing the final candidate. Do not pull unrelated Abort/joint/learner
RPC features merely to acquire that fix.

Automatic native writer/prepare-only-abort recovery, full broker/audit/sync
effects, cross-process native image handoff, old/new DATA-quorum membership
activation, proof-authorized cleanup and the automatic host/controller path
remain outside this delivery. Existing finite command/history bounds and
unsupported recovery states remain explicit refusal, not broad qualification.
No full Milestone 1, five-independent-VM expansion, zero-loss scale-out,
convergence or stable capacity claim follows.

## Required next checks and delivery

1. Integrate the coherent metadata reply provenance correction. Run the genuine
   held-prebinding-reply negative and healthy native metadata append/election
   controls on that exact combined source; keep the original defect evidence.
2. Freeze the implementation batch and first compile all workspace test targets.
   Check the isolated workflow consumer and no-default-feature Core build too.
   Then run focused native read/Strong, born2 enrollment/ordinary Apply,
   actual-open/actor, metadata initialization, fifth-factory/cancellation and
   cold-image/reopen controls relevant to this selected source. Record full test
   names, exact source, selected counts and failures; a zero match is not a pass.
3. Run required broad Core, PostgreSQL-wire, SQL/Oracle where applicable,
   remaining workspace/application and repository checks. Keep application runs
   collecting useful evidence. Do not substitute a focused workflow for the
   final broad gates or add overlapping cohort counts.
4. Root verifies current remote main, selects the next unused source version,
   uses the repository bump script, checks version identity and validates the
   final versioned head. Root then merges/pushes and verifies remote main before
   deleting merged temporary branches. No source bump implies a binary,
   release, deployment or operational test.

Preparation performed no Cargo invocation, database open, process test or VM
operation. Formatting/diff and source-selection checks are reported separately
from the required compilation and dynamic checks above.
