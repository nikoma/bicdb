# Joint handoff: independent fresh-phase review

Read-only review of root `021ddfc130590256bd607d376ac7581ba1f568b9` and the
proposed successor invariants. No new implementation, Cargo or dynamic result
is asserted here. The expired-round CI evidence remains its original source
cohort. Cold learner source `ac563d472` is preserved independently.

## Required authority boundary

An original prepared/live-MQ round must remain expired or canceled. A new
operation may retain its actual durable local facts, but needs a DISTINCT
opaque phase authority from independently fresh per-node HostMQ and actual
current service/DB/store/freeze/native checks. Neither a mutable deadline on an
old receipt nor a decoded audit projection can implement this boundary.

The caller's original parent deadline/cancellation must bound the new phase.
Every current participant must have its own actual transport requester identity,
current fixed configuration, birth/incarnation/pins/schema/restore and local
open. The new leader is mandatory. Preserve the full original participant set;
the three-copy acceptance case cannot silently shrink to a convenient majority.
A post-collection publication MQ needs its own independent context and term/index
not behind any newly collected authority. It must not compare only old audit
labels to claim this ordering. A new caller operation does not make an old
canceled operation or its observations live again.

## Existing source seams and recoverability blockers

1. **Original live authority is embedded in local preparation validation.**
   `distribution_data/native_joint_handoff/bridge.rs` retains
   `ActualServiceNativeJointPreparedEvidence.committed`; its `require_current`
   reaches `NativeJointServingPreparationRecord::require_binding`, which checks
   that observation's live MQ. A fresh-phase fact validator must use an actual
   new observation and revalidate ALL same-open physical/catalogue fields. Merely
   omitting the old live check is not a fact producer.

2. **Preparation records are immutable historical facts.**
   `db/range_image_native/serving_configuration/joint_handoff/bridge.rs`
   `same_fact` deliberately ignores metadata audit labels/Xid/checksum only for
   idempotent comparison, then reuses the original stored record. Pointer and
   immutable archive must still match. A fresh phase should preserve those bytes
   rather than rewrite original metadata context labels. The actual original
   native preparation SHA and full fixed decision remain the physical binding.

3. **Partial target catalogue activation can change its preparation digest.**
   `TargetImageCatalogHistoryObligations` hashes actual progress (including
   current epoch), entry identity/state and complete certified/sparse/installing
   provenance. It does NOT hash image intent or joint `serving_activation`.
   OLD survivors already advance their epoch to the final epoch during close
   (`joint_handoff_state.rs`); target image adoption starts at capture epoch.
   `joint_handoff_state/activation.rs` changes that target epoch at activation.
   Consequently a naive fresh `observe_native_joint_bridge_catalogue` can fail
   the original prepared `catalogue_sha256` on an otherwise legitimate already-
   activated target. Validate the exact monotone activation/intent retirement
   using actual typed native+catalogue records. Do not skip arbitrary digest,
   tail, sparse/current-certification, backup, nonce or image-base changes.
   The actual token exposes its prior epoch; do not invent it from old epoch.

4. **A protected native bridge is an anchor, not current Ready.**
   `bridge/activation.rs::require_actual_point` intentionally allows a current
   native prefix beyond the original cut, checking the actual immutable history
   and same physical base/generations. A native bridge receipt therefore cannot
   by itself prove exact current R/H/B, no higher CQ/pending tail, or current
   read/write readiness. For still-frozen preparation, preserve the stronger
   exact point checks in preparation `require_binding`. A later activated-anchor
   branch must retain its narrower historical meaning.

5. **Already released ownership cannot be recounted as Frozen.**
   `joint/runtime.rs` retains `Released(original activation receipt)` after drop.
   `NativeJointImageReleaseOwner` retains `frozen=None`; the Recovery lease's
   actual Frozen has been consumed and its obsolete in-memory session retired.
   Old receipt cancellation remains original, so those old retry methods stay
   expired. A fresh phase needs a distinct actual already-activated/released
   observation or must report that cut unsupported. It cannot call `as_frozen`
   on a consumed recovery lease, install a replacement freeze from a DTO, or
   silently replace the old receipt's lifetime.

6. **Actual serving-generation binding must survive retry.**
   `db/native_joint_activation.rs` binds the actual DB-open weak lifetime,
   health, store-open UUID and `RangeServingGeneration`. Arm requires an actual
   unfrozen/read stamp. A later freeze invalidates the old generation even if
   that later owner eventually drops. Fresh metadata alone cannot revive an old
   generation or arm stale activation after such a recovery transition.

7. **Cold restart is a separate ownership producer.**
   Existing prepared/native audit records survive, but same-open admissions and
   retained actual freezes do not. New phase support must not infer a new Frozen
   owner from their JSON/paths or treat an ended DB open as current. Until an
   actual restart/refusal/freeze producer is composed, classify that cut OPEN.

## Smallest adversarial acceptance cases

Use the actual enforced four-metadata/RF3-plus-spare fixture and target n3 as new
leader. Preserve its original failed-round assertions and artifacts separately.

- Let original preparation MQ expire. Assert its original validators STILL fail.
  Collect actual fresh per-node observations for the same full three-member set,
  then finish native publication/open/release and the genuine target ACK/read.
  Assert every reused preparation/publication audit byte/digest is unchanged.
- Cancel the original business parent after the first new observation. Later
  observations or a new uncancelable internal child must not finish that phase.
- Obtain an actual metadata configuration/identity/schema/restore change between
  participants. A known conflicting record poisons/refuses the phase; it cannot
  be discarded as a peer failure and then outvoted by the other two.
- Hold an actual controlled writer, change an actual DB inner open while keeping
  the outer Arc, or end one participant. Refuse exact currentness before any
  new countable grant. A missing third copy must remain visible.
- Publish one actual native bridge, expire the phase, and retry via the new
  authority. Keep the existing native bytes and immutable archive unchanged;
  absent/mismatching pointer or archive refuses, even with matching counters.
- Activate the actual image target catalogue, then expire. Permit only the
  exact proven epoch/intent retirement transition; an unrelated raw/CQ/sparse
  obligation or image/base change must still refuse.
- Inject an error AFTER actual frozen-owner drop but BEFORE arm, then expire.
  Old release methods must remain expired. A supported fresh already-released
  path must match actual activation/open/generation and original stored facts.
  A new intervening freeze/drop must invalidate the old arm, not renew it.
- Let a genuinely released current quorum ACK R+1 while another member retains
  the original cut. Its historical R anchor may remain valid, but cannot label
  that behind member Ready at R+1, erase CQ obligations or return false absence.
  Either implement an honest narrower activated-anchor phase or report this
  greater-prefix cut unsupported; current recovery/catch-up is a separate gate.

Only the first unchanged-frozen same-open path is the immediate successor.
Partial publication, partial catalogue activation, partial release, new writes,
metadata membership changes, cold restart, physical faults, source cleanup,
post-joint NoData and automatic operator scheduling must retain explicit scope.
No source-level guard pass is a completed recovery or an HA/deployment result.
