# Native read foundation: separate delivery preparation

This branch prepares one foundation delivery, separate from the full native
Milestone 1 and four-to-five operator goal. It is not a tested release, native
operator binary, deployment, physical qualification or completion of that goal.
Root owns the final PR rewrite, version selection, full CI, main merge and push.
No version is reserved by this document.

## Versioned delivery composition

The working delivery is **1.0.489-beta**. Its first composition used
prospective 488 source `34ba460941ddd011534b74d6054d59839bc9c84d` at
`3cd044d94fafcd37dd91cd0a93db1d88cd631091`; that base was superseded after
its workspace export regression failed. The current composition
`7d742557bad4433d743d80616cd5604a03c061c4`
merges corrected prospective 488 source
`d8fc122db55300fd019fffe95321403c30ef45be`. Its primary-only recovery exports
avoid unrelated secondary-index decoding and reject foreground mutations.
The merge was clean, including `db.rs` and `spatial_paged.rs`; it retains the
read/native mutation fences and newer decimal/index/invariant changes.
Verified main remains 487 at this checkpoint. Actual 488 main and the next
unused source patch must be verified before merging this delivery. This new
composition requires its own final affected checks; results for the older
base are not transferred to it.

All twenty-nine packages, internal requirements, tracked lockfiles and the
current-source compatibility document are synchronized. Full final-source
regression and stock Rails checks are required; older native231 or broad480
passes do not qualify this newly composed versioned source. Full Abort/joint
and public operator data serving are excluded as before.

## Current validation checkpoint (November 12)

Versioned source `2f09872918406347f4dcdbfed1215b8d3c3be6bd` passed Core,
SQL, PostgreSQL-wire, all-target/isolated-consumer compilation and formatting
in [PR CI 43232044386](https://github.com/nikoma/bicdb/actions/runs/43232044386),
and [stock Rails 43232044327](https://github.com/nikoma/bicdb/actions/runs/43232044327).
The remaining-workspace job subsequently **failed** its
`export_does_not_load_unrelated_secondary_indexes` assertion: corrupted unrelated
index JSON was decoded during primary-only export. This exact failure was
retained; it is the already identified prospective488 base defect, not a passing
release check.
Core library coverage is 846 passed, zero failed, nine ignored; integration
invocations are not added to this count. These results belong to the superseded
488-base composition, not `7d742557b` or an eventual main merge. In particular,
a passing Core/SQL result cannot waive the earlier base's failed workspace
export test. The corrected-base final delivery must pass the relevant suites.


The preparation notes below retain their original source-specific history.
Later read candidate `b08556c3f8ef5422833164d7efef90954846d93c` includes
verified main487 and the fail-closed missing-catalog custody correction.
Its `native_` batch passed 231 tests, zero failed, two ignored in
[run 43222639169](https://github.com/nikoma/bicdb/actions/runs/43222639169).
The paired acknowledged-value/missing-catalog acceptance and thirteen enrollment
controls also passed on implementation-equivalent source `25710b785`.
See [the custody correction and exact evidence](native-read-catalogue-custody-source-status.md).

Earlier source `480db3b52820919b0c18a48e15cccda38fe7c1c1` passed the broad
Core, SQL, PostgreSQL-wire, remaining workspace and stock Rails suites. These
older results do not validate later code or the final versioned delivery.
Final current-main integration, next unused version, broad release checks,
PR157 replacement and verified main merge remain required. No released binary,
physical experiment or completed Milestone 1 follows from these CI results.

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

The original independent source at809 retained version1.0.481-beta. It jumps from base protocol24
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

**The delayed pre-binding metadata Append ACK provenance dependency is composed.**
Root reports exact `29f9e80c572638b8ff4fcefa01f70e8448535be9` focused CI
`43169260087`: 222 passed, zero failed and two ignored, with compilation and
isolated consumer checks passed; the full test matrix was skipped. Those
results do not validate the later matched-frontier changes below.
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

## Subsequent matched-frontier correction: source only

The metadata Append defect is independent of native enrollment: an untouched
follower suffix must not be reported as matching a bounded leader request or
committed merely because it exists locally. This branch separately adds:

- `f0b06118cf254e14c3f5ab4531f267aff71ff1e4`, exact test cherry of
  `36e1fddcbe0c26245f0f9d2ad7b795158314c561`: three real fsynced stores and
  elections, divergent suffix, bounded ACK, real majority recovery, empty
  heartbeat, committed snapshot with zero entry payloads and strict reopen.
- `5dc886e098b0f9229c91ac7cdc42948ec6757e6a`, exact cherry of
  `c295916d0030f3fbd37962150cfd1f65dc70e752`: opt-in retention of this
  metadata-only fault state's files/reports in focused CI artifacts. No TLS
  credentials or application ACK ledger is produced by this control.
- `5d690a235680c8e88fd422a332878a6f75bc77f3`, adaptation of
  `2e2ddd9136efa7b181532888c66606003f8a12d6`: advance only through contiguous
  entries actually covered by the validated request. Empty requests use the
  verified previous entry; a validated snapshot supplies its covered frontier.
  Follower commit is capped at `min(leader_commit, matched_frontier)`.

The native owned-persistence producer receives this handler-derived frontier
and sets the reply's `match_index` **before** computing its provenance digest.
It still owns the actual fsynced write and applies all existing actual-open,
birth, StoreID, current lifecycle and pin checks. No public scalar setter or
new authority/protocol/version is added; protocol27 remains unchanged.

This adapted stack has not compiled or executed locally. The prior 29f result
and Root's separate public-main FSM baseline/fix are not tests of this new
native composition. The new regression name is
`metadata_append_actual_bounded_prefix_ack_cannot_commit_divergent_tail`;
a `native_` substring run does **not** select it. Run it explicitly, then the
related native owned/TLS ACK and snapshot/election controls on the frozen head.

## Finite heartbeat continuation: independent source adaptation

This branch adds only the following bounded-startup continuation to the read
foundation at `92d5b30fa`. These cherry-picks do not import their original full
prototype ancestry:

| Input | Independent cherry | Scope |
| --- | --- | --- |
| Original 17-barrier regression `8be0d3b05` | `ffac1823552f943b8047410cfb9e1a07c86b9b86` | Actual factories, common C, owned metadata stores, genuine TLS votes and withheld Append delivery; exercise the real finite heartbeat and recovery entry point |
| Accepted-peer progress `c95d89ebf` | `80403d306f079b02946aa739b1b95dc7f293485f` | Construct the next request from the leader's accepted per-peer match index plus one; retain three rounds and sixteen entries per request |
| 65-barrier controls `5b19455ec` | `18d2bbf86aa63b10389d89c0db0d88140432f8c7` | Genuine bounded progress across calls, no current-term read anchor before65, wrong-peer/unaccepted replies, real negative reply, actual new-term and committed-C resets |

Only the heartbeat test-module declaration was resolved. No Abort, joint-data,
learner-birth RPC or learner-enrollment transition module is added. Protocol27,
native birth fields and reader flags remain unchanged. The owned native Append
ACK producer still seals the handler-derived matched frontier before its digest;
its actual birth, protected StoreID, current identity, pin and body checks are
unchanged. The cursor consumes only that existing validated `match_index`, never
an ACK count, transport completion or conflict hint. Leadership, open and
committed-configuration resets remain in force. A same-term negative reply
removes only that peer's cached progress conservatively.

Root separately reports that full-prototype source `d9fdffd32`, CI `43174437808`,
reached the intended 17-barrier failure: both followers remained at16 after two
finite heartbeat calls, and the new recovery barrier lacked a durable quorum.
That is an earlier prototype result, not a test of this independent adaptation.
No passing result for either adapted regression is claimed here.

The exact regression names are:

- `native_initial_metadata_actual_tls_barrier_backlog_advances_beyond_first_batch`
- `native_initial_metadata_actual_tls_65_barriers_resume_across_finite_heartbeat_calls`

A `native_initial_metadata_actual_tls_` filter selects both. The 65-barrier source
requires the first finite call to reach48 on each peer with commit0 and HostMQ
refusal; after validated progress, a second call must commit65. It also checks
real new-term/configuration resets and unchanged application WAL without any
installed writer hook. Public evidence roots receive only sanitized metadata
traces; generated test TLS keys and native files remain in a separate private
fixture root. These are one-host component controls, not scheduler, writer,
Strong-serving, physical failover or Azure qualification.

## Mid-round retirement: independent source adaptation

Root's separate full-prototype `2c73af354` native cohort passed both bounded
heartbeat tests, but its enrollment retirement path reached a strict request
builder refusal after earlier peer ACKs committed C. That run does not validate
this read-only delivery composition or the following correction.

The narrow adaptation adds test `0295210ab` as
`f3b2d8bee7d05f01b0368dba82d043877d3adfc5` and fix `d07e817ee` as
`b0d181a0f05b30f79f146711f4e11e3c029fab55`. Before each request, heartbeat checks
current leadership and current Active membership under the same metadata guard
used for construction. A retired/missing peer from the captured earlier list
is skipped. The builder remains strict, all other construction errors still
propagate, and cancellation checks remain in the original bounded lock path.
No network call retains that guard.

`native_initial_metadata_actual_tls_midround_retirement_skips_stale_peer` uses
four actual owned native metadata stores/TLS listeners and a real planned fifth
identity registered only as an unbound nonvoting learner. Real early ACKs from
peers1/2 commit retirement before captured peer4 is reached. It requires the
heartbeat and all four real replicas to finish at the committed metadata index,
while directly building for retired peer4 still refuses. Actual application WAL
must remain unchanged and no writer hook is installed. It uses no learner-birth
RPC, learner-enrollment transition, Abort or joint-data producer. Its metadata
retirement is not data-replica movement or physical scale-out qualification.
This new regression and the correction remain unexecuted on this branch.

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

## Independent metadata candidate composition

See [the source-only integration audit](native-read-metadata-485-integration.md)
for mergec48c804d6 of preserved809 with PR162 candidatece023036, exact conflict
resolution, read/routing invariants, local configuration override review and
remote validation filters. That candidate inherits version1.0.485-beta; no new
version, PR157 update, executed test or public delivery is claimed here.

## Corrected-base final validation in progress

At `a7ecd9450b9b78c438cd365aed005a95f756a001` (corrected488 base),
[run 43243818809](https://github.com/nikoma/bicdb/actions/runs/43243818809)
passed the Core job: library846 passed, zero failed, nine ignored in174.23s,
plus integration invocations; all-target and isolated-consumer checks passed.
There are128 invocation summaries, not128 unique tests. SQL, PostgreSQL-wire,
remaining workspace and stock Rails43243823778 are still running. These results
cover this exact source, not an unobserved main merge. Final requirements remain.
The earlier2f failed workspace raw-log digest is recorded in retained CI custody;
its failed assertion has not been waived.
