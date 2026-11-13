# Remote native images: phase lifetimes and capacity gates

Read-only audit of `d5ef90ecb11c593d91301369f5b44ad2fe3bf395`.
No implementation, build, benchmark or new runtime result accompanies this note.
It refines the lifetime section of the earlier
[remote session contract](native-remote-image-session-contract.md), inspected at
`3ee40c19b44affa0a481a66b278ded3af817e910`; that document's transport and kernel
producers remain proposals. Paths below are relative to `crates/bicdb-core/src/`.
The goal remains automatic, online, fault-safe transfer of the full 8 GiB dataset
from four to five independent machines. A small frozen component transfer cannot
qualify that goal. P0 read correctness and bounded recovery remain prerequisites.

## Actual lifetime boundaries

There is **no universal four-second handoff capture lifetime**. Historical
closed-cut facts, actual physical ownership and current permission are distinct.

| Actual source | Lifetime and authority boundary at the audited revision |
| --- | --- |
| `distribution_range_handoff/joint/closed.rs::NativeJointOldObservation` | `require_current` checks actual service, protected OLD closure, permanent promise, Frozen and pin. `require_issuance_current` additionally checks the original role/plan deadline. Stored closure is not a renewed role. |
| `joint/plan.rs::NativeJointHandoffPlan` | `require_live` bounds original issuance. `require_role` can validate a distinct fresh actual role for the EXACT same committed plan/session/nonce/configuration, with nonregressing metadata anchors. It does not revive the original role. |
| `joint/old_quorum.rs::NativeJointOldRound` | Actual OLD observations and history coverage establish a sealed closed cut, including every observed irreversible obligation. `begin_capture` consumes that cut once and checks actual retained owners. A maximum native counter is insufficient. |
| `distribution_range_image.rs::RangeImageCapture` | Owns actual source DB, Frozen, read pin, lineage, R, source-local B, catalog bindings and cursors. It stores no general original business cancellation/deadline. `begin_internal`'s at-most-300-second token only bounds its wait. Export accepts a caller token each step and validates physical ownership before/after work. |
| `distribution_range_image/source_cut.rs::require_retained_history_quorum` | CommittedHandoff does not retain the original role timer here. Initial CurrentVoterRepair has no retained replacement/resume coverage; those specialized coverage-bearing cuts DO recheck their retained current quorum deadline. Do not transfer that restriction or authority between purposes. |
| `distribution_range_consensus/image_install_fence.rs::NativeImageInstallRole` | Every target step consumes an actual live purpose-specific role, checked against the exact durable intent. CommittedHandoff remains distinct from CurrentVoterRepair. The intent stores restrictive identities/session/cut, not a renewable role. |
| `distribution_range_image_install.rs` | Installer holds actual target freeze/fence/safety/catalogue and staged history. Verification owns the completed LOCAL source capture. Local source borrows and actual owners cannot be deserialized from a seal. Partial clear/copy progress is in memory; existing exact completed-image recovery does not establish general partial cold continuation. |

Consequently a handoff is not proven impossible solely by a four-second MQ bound,
but the current local capture also does not enforce a retained original business
deadline across successive export calls. A remote session must add that ownership
contract. Replacement-history cuts that retain a short-lived QQ remain bounded
by it; a 300-second capture wait cannot extend it. No long-image completion bound
has been established by these source facts.

The existing fresh preparation/publication/open/release phases in
`distribution_range_handoff/joint/bridge/` separate historical facts from fresh
per-recipient HostMQ, local post-QQ authority and the ORIGINAL caller token.
They require the full participant set and mandatory leader, exact open/Store,
writer, Frozen/pin or actual released-generation facts, and protected decisions.
These are local opaque orchestration patterns, not remote image permissions.

## First implementable cross-process phase protocol

The first vertical is explicitly **offline/frozen for the selected range**.
Other-range writes may continue; do not require the entire database application
sequence to remain unchanged. Source B is read visibility, target B is produced
locally by actual installation; neither is a supplied range floor.

1. Use command-driven registration/enrollment of the genuine fifth learner and a
   committed pending plan. Authenticated OLD-close handlers independently obtain
   actual roles and retain their OWN local closure/freeze/pin. Source and target
   independently consume request-correlated TLS OLD observations and full
   commitment coverage. No caller-provided voter list/hash becomes a quorum.
2. Before accepting transfer, protect an exact accepted ImageCut fact on an OLD
   DATA quorum: scope/configuration/member identities, accepted R/H0/H, actual
   local closure/receipt bindings, and complete irreversible commitments through
   bounded authenticated pages. The current global joint decision is produced
   after NEW materialization and cannot supply this pre-transfer crash fact.
   The new stored fact remains audit/refusal data, not serialized live authority.
3. Source and target registries own the actual session capabilities and ORIGINAL
   business cancellation/deadline. A source session owns the real capture and
   pin; a target session owns its actual Store/open, intent, freeze, staged proof
   and installer. Every advancement derives a separate short-lived phase token
   bounded by that same business parent; fresh per-recipient MQ and actual DATA
   coverage revalidate the exact unchanged plan/cut/owners and original promises.
   Old tokens stay expired. A new caller with a longer budget cannot continue
   the old same-operation session past its original deadline.
4. Add actual TLS source HistoryNext/RowsNext/Finish observations, sealed to the
   original request/start/deadline, challenge, peers/births/incarnations/pins,
   session/attempt/cursor, scope and content commitments. A target's metadata
   intent must not pretend it is the OLD data leader. Source read authorization
   needs its own actual current pending-plan authority plus actual closed-cut
   and physical ownership, not a fake NormalRecovery role or MQ alone.
5. Introduce a private remote-source kernel input alongside the existing LOCAL
   capture borrow. Its sole producer consumes actual authenticated observations,
   complete audited immutable history and the exact target context. It cannot be
   constructed from a decoded DTO, receipt hash, counter or transferable local
   Frozen permit. Keep current typed target row grants and full local checks.
6. Target stages/audits FULL history before clear, then bounded clear/copy,
   independent content verification, native adoption and catalogue finalization.
   Each mutation retains actual local identity, schema/UID, history and all
   catalogue/Commit obligations. No network wait under a DB/service/journal
   guard. Lost reply means uncertain until exact stored result is recovered;
   post-fsync expiry is not an ACK. A successful status is not new permission.

Source currently caches only one previous retry step. Target currently lacks a
general durable partial-copy cursor. The implementation must either protect exact
step/result checkpoints transactionally or explicitly restart bounded clear/copy
under a NEW authorized attempt. It cannot claim durable cursor resumption from
the present in-memory fields. Duplicates must return exact original content/audit.

Row export permits 8 MiB batches/records and a 9 MiB encoded step; history export
bounds a page to 256 entries/272 KiB. Actual transport configurations can have
4 MiB frames. Fragment the COMPLETE serialized envelope, including an individual
large record, using exact record/step/ordinal/whole-hash bindings and bounded
assembly. Check escaped bytes, missing/reordered/conflicting fragments and quota
before mutation. Never truncate history or silently lower the value contract.
A native ordinary command is bounded to 2 MiB; a large-record transport test must
use a genuinely supported source producer or a negotiated fragment size BELOW a
real smaller record. An invented 8 MiB ordinary ACK is not a valid fixture.

## Crash, abandonment and online obligations

Session expiry/cancellation closes advancement; permanent OLD promises survive.
Pending-plan removal or coordinator death cannot authorize unfencing, cleanup or
discarding a preexisting higher Commit/CQ. Conditional abandonment needs its own
durable quorum-backed no-exposure/decision protocol or must finish the same plan.
That producer is not present merely because metadata can remove a relocation.

Source restart loses actual pins; target restart loses actual installer owners.
Reading cut/intent/receipt JSON cannot recreate them. A distinct authorized cold
recovery purpose must consume actual protected accepted facts and fresh current
MQ/DATA evidence, preserve all prior promises/obligations, and establish NEW
physical owners. This is separate from extending an expired original operation.
An ended session may expose bounded historical status, never advancement authority.

Source substitution must cover the accepted old cut, target's stored native point,
received/audited prefix and ALL irreversible obligations, under an equal/higher
actual source history. CurrentVoterRepair replacement coverage does NOT grant a
learner CurrentVoter authority. Its existing four inherited-point and 128-archive
bounds are availability limits, not permission to prune ancestry. If replacement
changes source-local image metadata/root, compatible logical history alone cannot
continue the old byte cursor: require exact image equivalence or a NEW attempt
with protected old obligations and full bounded recopy. Target Unknown stays
Unknown until actual adoption; no invented zero or native floor.

Online 8 GiB copy requires another producer: a real fixed MVCC base view and GC
pin WITHOUT keeping the whole range Frozen, durable full-payload/canonical-NoData
tail retention, purpose-specific learner catch-up, and a short final OLD closure
covering every irreversible decision. Prove base R/H to final cut R/H, then
target-local materialization at that final cut. Current capture validates Frozen
and may scan at newer global snapshots; simply releasing Frozen is unsound.
`distribution_data/native_joint_handoff.rs::observe_native_joint_new_target`
requires finalized image R equal to the OLD cut; it does not compose base image
plus a later tail. Do not weaken that equality to simulate online copying.

## Capacity and validation work: independent hard gates

| Exact source boundary | Consequence; no performance measurement inferred |
| --- | --- |
| `db/range_image_native/abort_history/slot_journal.rs`: `MAX_SLOTS=4096` per range, `MAX_SLOT_BYTES=3 MiB`, `MAX_TOTAL_SLOT_BYTES=64 MiB` per DB | Protected monotone records retain FULL `RangeWriteCommand` payload, writer-start/everCommit/no-effect/promise. Publication checks count and aggregate encoded bytes before its final native puts. No corresponding resolved-slot GC producer was found. Raw compaction does not reclaim this inventory. |
| `distribution_range_consensus.rs::compact_resolved`, `RETAIN_APPLIED_COMMANDS_PER_RANGE=1024` | Compacts old resolved raw entries, preserving unresolved entries. This is not a global entry/byte bound or protected abort-history checkpoint. Ready explicitly refuses a retained scan above 1024; historical certification has its own 1024 bound. |
| `native_abort.rs::read_bounded_abort_catalogue`, `image_install_state.rs` guards, `catalogue_custody.rs` | Actual native catalogue readers/custody cap at 64 MiB. Common `RangeWriteStore::persist_with` clones/serializes/writes the whole envelope without an aggregate 64 MiB write preflight. Reader/writer budget alignment needs an actual regression; do not assume refusal necessarily occurs before the oversized publication. |
| `db/range_image_native/history.rs::put_immutable_history_entry` and export; `history_stage.rs` | Each Applied/canonical-Aborted link is immutable. Full 0..R history is exported/staged; page bounds are not total retention/transfer bounds. No general history checkpoint/GC producer is established here. |
| `distribution_range_recovery/image_history.rs`, `native_ready.rs` | Complete fenced obligations cap at 2049 and Ready at 2048. Refusal beyond bounds remains explicit; numeric compaction cannot silently omit a promised checksum. |

For an initially empty enforced native database loaded through the ordinary
abort-enabled writer path, cumulative retained full commands reach the protected
64 MiB budget well before storing 8 GiB of new user payload. This is a source
capacity blocker for that path, not a claim that every historical database is
physically unable to contain 8 GiB. Switching to an unqualified older domain,
raising caps or dropping slots by native/resolved counters does not close it.
Other mirrored catalogue payloads/JSON overhead may hit bounds earlier.

`db/native_recovery_ready.rs::require_owned_materialization_current` invokes
`range_image_native.rs::validate_source_range_image_lineage`: no user-row scan,
but actual base/prefix/joint checks, ALL namespace identities/catalogue hashing,
reader/schema/generation checks. Native admission revalidates READY against ALL
initial genesis receipts. `native_abort_slot_inventory` reads/validates EVERY
retained slot including full command bytes; callers can repeat lineage and
catalogue checks per retained decision. Catalogue persistence hashes/serializes
payload-bearing state. History export also revalidates lineage before/after each
page. Work scales with those structures and call multiplicity, not only a small
protected root. Protocol byte/count bounds do not prove completion inside a
four-second phase or with 8 GiB. Scoped reuse must retain identical final current
checks; long phases need actual bounded work/checkpoint protocols, not renewed
old authority or unmeasured latency assertions.

Sound future compaction needs protected resolved-slot checkpoints/tombstones
covering exact Applied/Aborted history and local data visibility, actual relevant
membership/quorum and all prior refusal facts. Retain EVERY unresolved/unknown,
everCommit, AbortPromise/no-effect, CurrentJoint/OLD closure, recovery/image,
reader-pin and backup/restore obligation. Never infer absence from counters.
Checkpoint publication/readback must precede payload reclamation; crash on either
side must retain the old fact or the verified replacement, never neither. Existing
single-envelope raw compaction is not this protocol. Design/implementation and
compatibility fences for that checkpoint are OPEN; none are introduced here.

## Next deterministic acceptance matrix

These are required future tests, not results claimed by this audit.

| Gate | Smallest genuine control and required evidence |
| --- | --- |
| Capacity refusal first | Real born2 service/ordinary writer reaches count and encoded-byte boundaries; capture exact protected/raw sizes. Refuse before new irreversible DATA/CQ/ACK, or report the earlier uncertain cut accurately. Preserve prior ACK ledger and exact Strong values across refusal, stop and strict reopen. Exercise aggregate catalogue budget independently of slot budget. |
| Checkpoint safety before scale | Actual resolved Applied/Aborted slots plus unresolved Commit/unknown/promise, joint and backup/recovery obligations. Wrong membership/prefix/visibility/missing archive refuses. Crash before checkpoint, after fsync/before reply, during reclamation and after restart; old ACKs and exact unknown decisions survive. No fixture counter rewrite. |
| First remote adoption | Command-driven fifth learner; real independent TLS processes, nonempty multi-page range with external fsynced ACK ledger, e.g. 16 MiB below current cumulative limits. Force original phase expiry BETWEEN pages; old token remains expired, fresh own MQ/DATA phase continues only under original live business session. Full image/history, distinct source/target B, target UID and unavailable-before-activation assertions. |
| Phase refusal/retry | Wrong actual peer/birth/open/Store slot/writer/plan, expired original caller with longer new caller, cancellation, lost/fsynced reply, missing OLD voter, uncovered higher CQ and history fork. Full-envelope fragment/retry equality; stale status grants nothing. |
| Cold and source death | Stop actual processes before/after accepted cut, partial audit, after clear, partial rows and pre/post adoption. Verify durable facts; no revived pin/Frozen. Separate learner replacement producer and equal/higher prefix coverage; keep lower-R OLD repair and missing NEW member repair gates explicit. |
| Downstream completion | Actual remote NEW materialized quorum and quorum-durable joint decision, metadata finalization, all mandatory final replicas/leader preparation/open/release, fresh policies, target-leader next ACK and Strong old/new ACK reads, stale-old-writer refusal, proof-authorized cleanup. Then automatic controller restart/convergence and online/fault schedules over the FULL 8 GiB, before independent-VM qualification. |

No existing metadata-control or blank/registered receiver allowlist is widened by
this note. Future immutable receiver mode must explicitly authorize ONLY reviewed
session operations; no legacy snapshot/APP/Strong fallback. Local opaque proofs,
metadata MQ, a source seal or a routing hint cannot substitute for remote DATA
authority. This audit changes no capability status, admission policy or version.
