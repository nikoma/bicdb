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
| Delayed pre-binding regression | `91ca9fc6dbef0ed5f72fd66b721099c1a59760bc`, cherry `e2e79bbe12562ff7ba0efc75462e04ccca9677bc` | Preserve the genuine delayed learner ACK/promotion refusal control before applying its correction |
| Owned metadata ACK correction | `f0015998def081449f8fdde4776dc58f6e7ee713`, cherry `34f0ad55be22dd8a09997692e9ac5d78f6f64d8b` | Bind a countable reply to protected actual receiver birth/StoreID or the original authenticated exchange and current committed identity |
| Persistence-producer correction | `64cb9f8e8eafcb69a19331401f6c0c4440d21e92`, cherry `6be6008baad63f85b97e6362448be9416fcbfc3e` | Actual fsynced persistence belongs inside the sealed response producer; preserve learned-retirement refusal |
| Retired-member refusal | `a778b09926a8b853008114c20ce4048306dbe1cd`, cherry `4c31585fc715965e0bdf2115e25b373ccc8093c9` | Reject retained real ACKs after committed retirement without changing replication progress |

The working source retains version1.0.481-beta. It jumps from base protocol24
to protocol27 solely for native metadata ACK provenance; protocols25/26 feature
code is not imported. No later Abort, joint-data handoff, learner-birth RPC25
or their ancestry is imported. Earlier component and CI evidence remains tied
to its original exact revisions; it is not a passing result for this branch.

## Scope and dependency gate

The selected source supplies native identity/durability and metadata ownership
fences, current native read observations and consuming Strong point-read
components, enforced writer enrollment, same-voter recovery/image components,
actual-open checks, and provisional factory/bootstrap surfaces. Their explicit
mode, lineage and actual-local-visibility checks remain intact. A source API or
local component test is not operator integration. Existing legacy contracts and
historical evidence are preserved.

**The delayed pre-binding metadata Append ACK provenance dependency is now
composed, but remains uncompiled and dynamically unvalidated on this branch.**
A reply produced before binding cannot be retagged with a later native identity.
The producer owns actual persistence and protected receiver birth/StoreID;
remote provenance is sealed by the original authenticated exchange. Counting
checks current committed birth/incarnation, pins, Active lifecycle and response
body before changing `match_index`. Decoded DTOs cannot construct that skipped
local provenance. Legacy absent fields remain omitted; earlier native birth
records without abort1 are accepted without retrofitting that capability.

Only protocol-number and transport-module context conflicts were resolved:
protocol27 names this ACK fence, and only the new `metadata_append` module was
added. No newer `native_abort` or `learner_birth` transport modules were pulled
in. See [the scoped ACK design](native-metadata-owned-append-ack.md) for the
producer boundary. Actual test results on another branch do not validate this
independent dependency composition.

Automatic native writer/prepare-only-abort recovery, full broker/audit/sync
effects, cross-process native image handoff, old/new DATA-quorum membership
activation, proof-authorized cleanup and the automatic host/controller path
remain outside this delivery. Existing finite command/history bounds and
unsupported recovery states remain explicit refusal, not broad qualification.
No full Milestone 1, five-independent-VM expansion, zero-loss scale-out,
convergence or stable capacity claim follows.

## Required next checks and delivery

1. Compile and run the newly composed metadata reply provenance correction:
   genuine held-prebinding-reply and decoded-DTO negatives, healthy owned/TLS
   append, current retirement, and native append/election controls on the exact
   final source. Keep the original defect evidence; source composition alone
   does not close this validation gate.
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
