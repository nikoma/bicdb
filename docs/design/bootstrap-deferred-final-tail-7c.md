# Deferred final-tail standby bootstrap protocol (design, not implemented)

This proposal is pinned to source `7c77418656a41ec6e25c7601319e061c328f48e1`, tree `df63298f61b0a4cdd56f0caabc694d888c38dbda` (the unmerged 520 candidate). It replaces the earlier proposal's live stage-to-restart write fence with a final authorization cut after the original database has actually closed. The earlier 2ce source proposal remains historical evidence; no implementation, compiler result, crash experiment, mixed-version result, or production qualification is claimed here.

The required behavior is both ordinary absent-target and ordinary existing-target standby bootstrap. A live existing standby may continue its existing permitted writes, replication apply and election activity while a candidate is staged. Stage completion is verification and association DATA, never install permission. Finish must capture the latest original state while the SAME original root and Page owners are actually closed. Native/distributed use additionally requires a genuine acknowledged-prefix coverage producer at that final cut; this proposal does not create one from paths, counters or serialized stage records.

## Source facts and changes required

| Current producer, pinned to the source above | Existing behavior | Required change |
| --- | --- | --- |
| [Core startup](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-core/src/db.rs#L10823) | Finish runs before FORMAT checking; a false return precedes root creation and DirectoryLock. | Acquire/check the shared namespace protocol before Finish, before FORMAT effects and before root creation. Retain it until the actual normal root claim/enrollment succeeds. |
| [Closed root creation/enrollment](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-core/src/db.rs#L10854) | Actual DirectoryLock, physical gate, Page claim and FORMAT enrollment exist. | Enroll a persistent local root identity under these actual owners; missing identity on an enrolled root refuses. No per-open UUID substitution. |
| [Stage](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-core/src/db/ha_bootstrap.rs#L97) | A live old root is read under backup/physical SH; source HA is read, then replaced using one early old HA read. READY is `ready\n`. | Preserve immutable verified SOURCE HA separately; persist the original-root/candidate association. Defer OLD-derived HA synthesis until Finish. |
| [Finish owners](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-core/src/db/ha_bootstrap.rs#L311) | Each call acquires current path owners; the physical scope is SH. Same-call checks do not prove an earlier target/predecessor association. | Acquire only roots matched to the durable intent; use a distinct closed-owner plus EX finalization factory without retaining this SH. |
| [Restore tail](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-core/src/backup.rs#L841) | Both codecs retain restore custody through payload/MAC verification, native-copy authorization and the tail. | Mint candidate enrollment and immutable source descriptor from these actual verified owners, not from a decoded caller DTO. Preserve pending-restore exclusion until that producer completes. |
| [Pg receive](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-pgwire/src/high_availability.rs#L1018) | A real `server.read_db()` is reduced to a path before archive receipt; staging precedes shutdown request. | Capture the original persistent root binding from that real live owner before receive; require the same binding at Stage. Drop DB guards before stage/restart. Requesting shutdown is not actual closure. |
| [CLI reseed](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-cli/src/main.rs#L6567) | An existing target's actual DirectoryLock is held while the path-only Stage function runs. | Pass/borrow this real closed claim for enrollment/association; avoid a recursive acquisition. The absent lane must use the same namespace reservation. |
| [Actual consistent snapshot](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-core/src/db.rs#L10569) | Commit admission is excluded and WAL made durable through the actual applied cut before archive creation. | Preserve this source producer; its local backup cut is not by itself proof that a final target's required acknowledged history is covered. |

The new names below are proposed private interfaces and disk records, not APIs that already exist. The existing public `stage_standby_bootstrap` / `finish_standby_bootstrap` names and both backup codec entry points remain supported. There is no blanket existing-root refusal or absent-only replacement of the API.

## Local identity and namespace enrollment before any installable record

A local `PhysicalRootIdentity` is a persistent allocation ID with an enrollment version. It identifies the physical database root across close/reopen and renames, not its data lineage, process or authority. Mint it only under an actual fresh-root creation plus claim, or an explicit closed legacy-root enrollment after all normal ownership checks. The current [incarnation UUID](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-core/src/db.rs#L11027) is allocated per open and cannot serve this purpose. Optional native-origin state and collection namespace IDs are not universal substitutes.

Current-artifact normal open performs closed legacy enrollment before it permits mutation/ACK, so a later live Stage can read a real enrolled identity. A running old artifact without this enrollment cannot be silently migrated by a path reader: stop/enroll/reopen with the new artifact is a real precondition. A flag-active missing/malformed root identity, or an identity missing while an install is pending, refuses rather than reminting. This is a profile migration, not a generic unknown-to-fresh recovery lane.

A stable parent-owned namespace slot binds a canonical, no-follow parent and exact target component to `namespace_id` and monotone install generation. It must survive the target's absence and both renames. Its never-unlinked lock inode is synchronization; durable slot identity and intent are the cold association. An inode, path string, random per-open ID, or current DirectoryLock alone is insufficient. Persistent IDs are trusted crash-fault correlation, not cryptographic authorization against a malicious local writer copying control files.

Every current-artifact Core creation/open, destructive restore/reseed, and relevant standalone Page creation must check the same slot before changing that namespace. In particular [PageStore::open](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-page/src/manager.rs#L858) currently creates parents before opening its physical gate; admission must precede that effect. Core and Page need a shared low-level namespace exclusion/refusal primitive without a Page-to-Core dependency. Core alone interprets the full bootstrap intent; a standalone Page opener can refuse a pending final install without acquiring any Core authority. Canonical aliases, symlinks/reparse points and parent substitution must not allocate another slot for the same controlled namespace.

Stage takes a short real namespace association guard and retains a reservation through candidate creation/verification. Existing original Core owners may keep serving; Stage does not acquire their DirectoryLock or attempt SH-to-EX upgrade. Normal reopen while only StageIntent exists may proceed only as the SAME original root identity, with no namespace rebinding. The absent reservation prevents creating an unrelated target. Stage's candidate namespace is also reserved against ordinary Core/Page creation.

Persist `StageReservation {version, namespace_id, generation, install_id, target_component, expected_original_root_id_or_absent, reserved_candidate_component, allocated_candidate_root_id, verified_archive_header_id_and_manifest_digest}` with file and parent durability before candidate creation/writes. The archive header comes from the actual verifier; it is not proof that all payloads have already verified. Reservation permits only its private restore producer to create the candidate; it is neither STAGED nor installable. An unrelated existing candidate component refuses rather than being force-cleared. Completed SI must bind that reservation's digest and full verified source result. A new install cannot overwrite an earlier reservation or erase its evidence solely because a timeout occurred.

Before any candidate writes, use exclusive directory creation, an actual retained directory-handle observation, and its real root/Page/backup claims. Reuse the real association pattern introduced for Cell rotation, not `None == None` physical birth or `create_dir_all` as permission. A losing claim never authorizes cleanup of another tree. The candidate gets a fresh local identity under SAME verified restore custody. A power loss before that identity is durably enrolled must not turn an unmarked closed directory into an adopted candidate; the protocol must preserve/refuse an ambiguous object, or supply a proven durable creation-reservation recovery producer before claiming automatic progress through that cut.

Physical root identities and namespace/install control records are local and nonportable. Both archive codecs/repository manifests, exclusions, force-clear paths and materialized-copy authorization must strip source physical identity and remint candidate identity while retaining the verified SOURCE DATA lineage. A source backup root ID can never authorize replacement of a target. Generic force restore cannot erase an active namespace/final intent; reset/import over a root must allocate a new physical identity or be an explicit same-owner data transition recorded by this protocol.

## Stage: immutable verified source and original target binding

The private producer shape is:

```text
begin_stage(actual NamespaceGuard,
            Original = AbsentReservation | ActualLiveRootBinding | BorrowedClosedRootClaim)
    -> StageReservation
verify_and_restore(StageReservation, actual verified archive owner)
    -> StagedCandidate { immutable StageSource, immutable StageIntent }
```

`ActualLiveRootBinding` is minted from the real enrolled `BicDb` owner before Pg receives the archive; it carries exact root/namespace identity and a weak lifetime/correlation binding, not a detached write grant or a strong DB Arc retained through restart. The trusted public path-only Stage can select the actual enrolled original at its invocation under the namespace/physical read guard. It cannot bless an arbitrary unprofiled live root or infer the earlier Pg owner's identity from the path. CLI borrows its actual closed claim. No API receives a caller-supplied `is_original`, identity UUID, or source backup ID as authorization.

Persist a bounded immutable StageSource before transforming HA fields. It references the authenticated backup ID, full verified manifest digest and actual source cut, and preserves the exact SOURCE HA bytes or explicit absence, their digest, source FORMAT/storage profile and relevant verified lineage/native-copy descriptors. Source HA is DATA, never target vote/role authority. Do not reread the already synthesized candidate tail as SOURCE during retry. No ordinary opening of a stage with copied SourcePrimary state is permitted: candidate admission remains pending throughout this interval.

Persist StageIntent and a distinct versioned `STAGED_V2` marker only after restore verification and candidate enrollment:

```text
StageIntent = {
  version, namespace_id, generation, install_id, exact namespace components,
  original: Absent | Existing(original_root_id),
  candidate_root_id, source_descriptor_digest, verified_payload_manifest_digest,
  replacement_lane: OrdinaryStandbyReplacement | NativeRequiredPrefix,
  parent_stage_reservation_digest
}
```

The lane name is not native permission. NativeRequiredPrefix cannot reach final mutation without the actual coverage producer below. StageIntent has no latest-OLD HA, install authorization or source-derived target identity. An independently stored original target HA snapshot at staging can be diagnostic only. It cannot override a later ballot, stream apply, maintenance or ACK.

Stage-only live writes/elections remain governed by the old server's existing policy. It may catch up after Stage, accept a later vote or grow its durable head. The candidate becomes stale for a strict native lane if coverage does not reach the final required head; Stage success promises neither eventual installation nor zero ACK loss. Ordinary async/divergent reseed may intentionally replace history under its existing explicit policy; namespace matching does not upgrade that policy.

## Final: latest closed OLD cut and FinalIntent before effects

Finish acquires the namespace guard, resolves the SAME StageIntent, and obtains the actual matched original root, candidate and any already-associated predecessor owners. A present but different target or predecessor is a conflict even if it is closed. With an originally absent target, a later normal target is never retroactively adopted. With an existing target, a new real root at the same path is never the original. An earlier install's predecessor is not cleanup permission for this install.

Construct a distinct `ClosedBootstrapFinalizer` from SAME root/Page DirectoryLocks, backup owner and physical EX. The current `BootstrapTreeOwner` retains physical SH, so it cannot be reused by attempting an upgrade. The new factory acquires the real claims in audited order and enters EX without an outstanding SH; nested physical writes use [the SAME EX lexical borrow](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-page/src/physical_gate.rs#L369), never a fresh SH or detached thread. Do not acquire a live Core write guard or open a second `BicDb` underneath these claims.

While these actual claims remain held:

1. Check original/candidate identity, namespace reservation, verified source and exact stage payload/tail-free digest. Confirm restore completion and health; a missing pending marker alone is not a successful historical restore return.
2. Read the latest original HA state and complete relevant physical/durable head. Preserve its exact bytes or explicit absence independently of StageSource. Root identity plus HA equality does not prove unchanged APP data: same-epoch apply/commit may advance without changing HA. A read-only stable full-state fingerprint at this CLOSED cut may detect later mutation, but it is not a prefix-coverage certificate.
   The original-head observation must be read-only under the closed owner: exact relevant files, checked file lengths/checksums and data/WAL lineage are not represented by only a sequence floor. Do not call truncating startup recovery or a bad-tail-skipping parser as a validator. If ordinary startup recovery is needed first, it is a separate normal-owner operation before FI; after it closes, recapture the actual final head. The native branch must refuse until its genuine head/coverage minter exists. Lock bytes and mutable protocol-progress files must have explicit narrow exclusions; a broad directory digest is not an authority token.
3. Derive the exact candidate standby tail from immutable StageSource plus this latest OLD preimage, according to the audited ordinary HA rules. Preserve latest target voting/election obligations and quarantine; do not copy source Primary, votes, slots or a cleared verification state as local authority. Any proposal to change those rules requires separate semantic controls; a source record alone does not supply current-native authority.
4. For NativeRequiredPrefix, validate a genuine sealed final required-prefix/lineage proof against this actual closed original, actual verified candidate, required ACK obligations and current configuration. If that producer is absent or refuses, refuse BEFORE tail/rename/retirement. Keeping the predecessor after publishing stale active data does not itself preserve ACK semantics.
5. Persist immutable FinalIntent, its exact referenced OLD/source preimages, post-tail bytes/digests and `FINAL_PENDING` namespace fence durably BEFORE the first candidate HA write or namespace move. Its encoding names actual root/install identities, not live authority reconstruction. Every earlier error remains its original error; a new barrier proves new durability only.

```text
FinalIntent = {
  version, stage_intent_digest, namespace_id, generation, install_id,
  original_root_id_or_absent, candidate_root_id,
  exact_original_head_fingerprint, original_HA_preimage_digest,
  immutable_stage_source_digest,
  exact_allowed_post_tail_digest,
  original_and_candidate_profile_bindings,
  replacement_policy_binding,
  native_coverage_descriptor_digest_if_genuinely_issued
}
```

The private current finalizer, not this record, authorizes physical effects. Full preimages can be content-addressed immutable side files, hash-bound to FI, rather than three resident serialized copies. Stream hash/read exact bytes; do not impose an unrelated small cap on existing admissible HA/catalogue/history. The current 64 MiB backup header limit is an existing archive contract, not a newly justified universal HA-map bound. The implementation must define separate checked fixed-record bounds and streaming bounds derived from the accepted input formats, and explicitly preserve existing accepted-head capacity. Missing/corrupt/unknown-version/truncated side files refuse before effects.

Once FinalIntent is pending, all current-artifact normal Core/Page open/creation/destructive reset paths must finish the matching protocol or refuse before their own effects. FI does not hold a DB Arc, DirectoryLock or EX after the call returns. This durable early-open fence, not an indefinitely retained memory token, prevents a failed Final call from allowing new writes behind the saved final cut. A future opener which cannot acquire closed owners returns the existing busy/refusal behavior; it must not wait for a claim held by its own returned Stage object. All Stage/Final scopes drop before the caller requests or performs a restart.

## Tail reconciliation, rename journal and cold recovery

Store progress outside the moving roots under the same namespace slot. State references immutable SI/FI digests and a monotone checked generation; write, fsync file, atomic replace and fsync actual containing directory before dependent effects. Strict decoding rejects bad tails; no truncating/silent parser or inferred state from a missing filename. Stage-only status cannot bypass pending restore or FI.

Candidate HA before an interrupted tail can be exactly the saved SOURCE preimage or exactly the FI post-tail; default/absent is accepted only if that absence was explicitly in the saved permitted state. A mixed or different HA body is not repaired by guessing. Repeated synthesis always uses immutable StageSource and immutable OLD preimage; it never uses the previous post-tail as the next input. Other candidate payloads must remain exactly bound. A failed write/sync with visible post-tail bytes is Unknown, not old success. Under fresh SAME closed ownership, a NEW explicit barrier can durably establish the exact allowed post-tail and move progress forward while preserving the old error attribution.

| Durable intent / actual matched placement | Permitted next action |
| --- | --- |
| SI only, target original (or reserved absence), candidate exact | Normal SAME-original activity is allowed. Finish must create a new current FI; never use a staging-time OLD head. |
| FI pending, target original, stage candidate, predecessor absent | Validate exact final OLD/source/candidate bindings, tail and new barriers; persist MoveOld intent, no-replace old to predecessor, fsync parent. |
| FI pending, target absent, stage candidate, predecessor SAME original | Recover only this install's interrupted first rename. Preserve predecessor, verify its saved final head; no-replace candidate to target. |
| FI pending, original absent and target/predecessor absent | No-replace candidate to target. There is no predecessor cleanup permission. |
| Target SAME candidate, stage absent, predecessor exactly as FI specifies | Revalidate installed payload/tail; obtain a NEW parent durability result, then durable Committed. This does not relabel an earlier failed directory sync. |
| Any unmatched target, predecessor, candidate ID, unexpected duplicate, namespace generation or head/tail mismatch | No adoption, rename or deletion. Preserve evidence and unrelated roots; typed pending/conflict/quarantine. |

Keep original data and FI evidence through both renames and every parent-sync failure. Normal open becomes permissible only after candidate publication, a real successful parent barrier and durable Committed, plus any actual native readiness/proof obligations. Persist CleanupPending before removing ONLY the SAME original predecessor. Fsync parent after removal and write Done/tombstone. A new occupant at a previously absent/removed predecessor name is never cleanup authority; generation/tombstone prevents stale READY reuse. Completed-history garbage collection needs its own no-active-owner protocol; this design does not authorize unbounded tombstones or silent caps.

Cold recovery may reacquire real filesystem claims and prove a NEW durability result for exact SI/FI state. It cannot reacquire a prior Role/OWN/MQ/Frozen or native grant from JSON. The ordinary replacement lane may progress on its explicit closed-root policy. The native lane must re-observe/obtain the genuine required coverage and current phase authority through an existing lawful recovery producer. If no such cold producer exists, refuse before publishing and keep that availability gate open; do not convert the ordinary path into native recovery permission.

## Native required-prefix producer: available facts versus missing proof

Available: the real source consistent-backup cut excludes commits and writes the actual applied boundary durable; the verified restore owners authenticate the selected archive; actual target closed root/Page/EX can establish stable local files; current native handoff components retain real decision/prepared/catalogue owners in their separate process lifetime. None independently proves that a chosen source snapshot covers the final original target's required acknowledged prefix on the accepted branch.

Missing for this bootstrap path: an opaque minter taking actual source coverage/accepted lineage plus the SAME closed original target and required ACK obligations, with actual configuration/decision ancestry, capable of authorizing exact final replacement and predecessor retirement. A numeric source sequence >= target sequence, equal HA file, root allocation ID, full-directory hash, partial archive hash or an enum `NativeRequiredPrefix` cannot mint it. The current generic backup/Stage API supplies no such object. The read-only exact durable-head fingerprint API must also avoid reusing startup WAL recovery, which can truncate/change files; whole-tree fingerprinting at Final is only a mutation-detection input and does not solve branch coverage.

Therefore this design can specify the ordinary standby correctness protocol completely while retaining a concrete native implementation blocker. Full automatic native cold repair is not claimed. Real async/divergent policy remains distinct from synchronous zero-ACK-loss claims. If additional finalized tail semantics for native metadata are needed, they must consume actual phase-bound producers, not generic HA JSON.

## Pg/CLI integration and original request lifetime

Pg captures the actual target binding at `server.read_db()` before the archive is read, then releases that DB guard. It receives and hashes chunks as today, invokes Stage with that binding, releases every Stage scope and requests shutdown. [restart](https://github.com/nikoma/bicdb/blob/7c77418656a41ec6e25c7601319e061c328f48e1/crates/bicdb-pgwire/src/high_availability.rs#L70) only sets a flag/requests shutdown; actual host closure and separate next open provide Final's closed claims. No Stage result holds its own restart's EX/root claim or strong Core Arc. A canceled/expired request does not renew its effects; possible completed/Unknown Stage outcomes remain attributed to that original operation. A distinct next-open finalizer is a new actual filesystem/durability operation, not a revived network request or proof the original call returned Ok.

CLI preserves `ha reseed`'s existing closed-target meaning and message that next restart installs. It passes its actual claim instead of reacquiring itself; after Stage it drops that claim before a later open invokes Final. Originally absent targets use the namespace reservation and candidate real creation claim. Ordinary public path-based staging retains existing-target support through enrollment/association and a separate finish; it does not have to freeze all live writes during archive restore. Unprofiled live old binaries/mixed control planes require explicit stop/enrollment and are not silently supported.

## Reader/version and old-stage policy

The protocol must run before Core's existing pre-FORMAT Finish. Use NEW versioned Stage/Final/READY names and no legacy `BOOTSTRAP_READY` containing JSON. A FORMAT capability can make later payload access refuse but cannot retroactively protect an earlier old installer.

Pinned [main511 old Finish](https://github.com/nikoma/bicdb/blob/74f17b912cec7f2fe093d10dcf1618a43b35f454/crates/bicdb-core/src/db/ha_bootstrap.rs#L162) uses legacy READY presence without validating its bytes and removes staging if that marker is absent (178–181). Pinned current7c validates `ready\n` but still removes a captured staging tree without its expected marker (214–217). Thus an old binary can delete new staged work or create an absent target while ignoring the new parent slot before FORMAT checking. This is NOT a uniform old-reader fail-closed promise. New current-artifact recovery detects changed/missing IDs and refuses; rollback/open during incomplete installation and mixed old/new bootstrap remain unsupported until actual old-binary experiments establish a narrower contract. No old binary was executed in this design.

Legacy unassociated READY/stage/predecessor state is not silently upgraded to install authority. Preserve it and require an explicit actual-owner recovery/restage which verifies the selected source and original target. Do not delete it as incomplete merely to pass startup. Preserve public function names while making ambiguous legacy state a precise error; successful ordinary existing-root behavior must be covered by real new-profile tests.

## Required genuine acceptance and crash controls (all not tested for this proposal)

1. Ordinary existing live Pg target: Stage, real later vote/election-term change and same-epoch durable import/APP with full Record ACK, close, Final. Final tail must use the latest OLD state, and ordinary history policy must be explicit. Native-uncovered required ACK must refuse BEFORE tail/rename; retaining only a stale active view plus old ciphertext is not a pass.
2. Absent target Stage, actual normal Core creation/ACK/close at a competing name and distinct cold finish: no adoption. Existing target replaced by a different actual enrolled Core root is the parallel negative. Actual unrelated predecessor/closed new occupant is never deleted.
3. Both codecs and repository restore: verified SOURCE immutable, candidate physical identity freshly allocated, latest target ballot retained, source Primary/votes never imported, complete data/index/catalogue/row assertions retained. Wrong source/manifest/root/namespace and missing enrolled identity refuse; no fake marker/JSON grant.
4. Faults and actual child-process crashes before/after reservation, root enrollment, SI/STAGED, FI/preimage publication, tail replace/file+dir sync, each rename and parent sync, Committed, marker unlink, predecessor removal and Done. Require exact permitted placement/bytes and preserved original API return; distinct fresh successful barriers cannot rewrite old errors as success.
5. FI failure then actual normal Core and standalone Page open/write attempts: both must finish or refuse before effects. Stage-only SAME-original reopen remains lawful. No lock/Arc cycle through Pg shutdown, no SH-to-EX self-deadlock, claims drop before restart/join.
6. Keep all original namespace/full-payload/error assertions. A new early slot fence changes the old pause-after-Finish-false schedule: use an actual nonblocking admission/refusal observation and release the original hook before joining, not a stager waiting on a claim held by the paused opener. Map the original race to the precise new refusal cut rather than skipping it or weakening its payload assertion.
7. Interrupted tail from saved SOURCE, exact post-tail after failed sync, different/mixed body, repeated latest OLD validation and same-name predecessor replacement after cleanup failure. No `Absent`/standby marker/visible-byte-only success shortcuts.
8. Current-artifact cold success for both absent and existing targets, malformed/torn/unknown journal, orphan legacy state, force-restore/reset during FI and stale generation replay. Mixed old-reader tests only if a compatibility promise is later proposed.

## Four-state status and smallest coherent implementation batch

- **Passed (source inspection):** actual Pg live owner, CLI closed owner, restore custody/tail, pre-FORMAT ordering, physical EX borrow and missing persistent association are pinned. Deferred finalization avoids a universal live write fence and the strong-Arc/restart cycle of the older proposal.
- **Failed (current source contract):** there is no durable original target/predecessor association or final latest-OLD intent/open fence in current7c. Same-call namespace race tests passing does not establish cross-call/cold association.
- **Inconclusive:** portable namespace/creation-reservation crash progress, exact read-only final head observation and complete shared Core/Page/restore opener coverage until their concrete low-level APIs/lock order are implemented and reviewed. Unknown/corrupt identity refuses; it is never evidence of fresh birth.
- **Not tested:** every proposed Stage/Final protocol behavior, crash and integration control above. Existing7c component passes are evidence only for current implementation, not for this proposal.
- **Open:** native final acknowledged-prefix/lineage coverage, phase-bound cold authority reconstruction, controller discovery/restart, final retirement/GC and mixed-version qualification. There is no automatic native or full self-healing claim.

Implement as one coherent ordinary-bootstrap correctness batch: persistent local enrollment + shared early namespace admission, same-owner verified candidate/source descriptor for both codecs, SI-only staging, actual closed Final owner/EX, immutable FI/open fence, strict source/post-tail reconciliation and one-way journal, both genuine caller integrations and the full absent/existing/crash controls. A parser, absent-only lane or HA-file equality check alone is not this fix. The native coverage producer is a separate explicit prerequisite before enabling native replacement; keep its lane refused, not silently mapped to ordinary policy.
