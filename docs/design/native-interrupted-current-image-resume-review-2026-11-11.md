# Interrupted current-voter image recovery: source review

Reviewed private source `bc3953904659856be93a96d0efdddfd6cde1dbb5` on November 11, 2026. This is a source assessment and a proposed deterministic characterization, not a newly executed failure test, a current-main defect attribution, or qualification evidence.

The scenario loses **all** DB, Runtime, capture, installer and metadata-provider owners after the target clear finishes and before full row copying. Source n0 retains actual native R1026. Target n1 retains its actual Sparse1026 decision, image intent, native Installing marker and audited inactive history; it has no published installed-image base. Data member n2 remains unavailable, while metadata consensus can still obtain a majority. A strict open changes DB-open identities without changing the physical births or namespace UIDs.

## Finding

The inspected APIs do not establish an integrated cold-resume path for this scenario. They correctly preserve refusal rather than turn an incomplete image into readiness. Two dependencies need an actual characterization:

1. `RangeRecoveryRuntime::fence_epoch` obtains fresh metadata authority, then calls `ClusterDataNodeService::preflight_native_recovery` and `RangeWriteStore::preflight_recovery_fence`. The latter calls `require_no_image_install`. Target n1 therefore cannot provide an ordinary fresh fence observation while its durable intent remains. With n2 unavailable, n0 alone cannot finish a fresh data-quorum round. Metadata majority alone does not supply that data proof.
2. A newly constructed Runtime has no sessions. `transfer_image_install` first requires `source_capture_freeze`, which accepts only an existing exact-session Runtime freeze. The ordinary session-creation path is the fence endpoint above. No separate cold install-resume session producer was found.

The lower installer has a useful but narrower seam: `begin_native_image_install` can replace an incomplete attempt and rebind the saved target DB-open identity after validating its exact existing intent, actual birth, UIDs and fresh role. It still requires `TentativeNativeRangeImageSourceAuthorization`, borrowed from a live actual capture. `CertifiedRecoveryImageCut::from_verified_source_history` requires actual fresh fenced-quorum source-history agreement. It has no JSON or numeric constructor. Once all original owners are lost, the persisted intent cannot recreate that authority.

`recover_completed` is distinct: it requires a live opaque completed source and an actual published native installed-image receipt, then rescans the image. Before full copy, no such target receipt exists. It cannot convert partial installation into a completed one.

The persisted `ImageInstallIntent` contains the physical target birth and namespace UIDs, source node/incarnation/R/native-receipt/catalog commitments, current and intended configurations and credentials, restore/schema domain, session, nonce and attempt. It is explicitly a **refusal record**. It does not persist an opaque current-data-quorum proof. The transfer preflight also binds ephemeral DB-open identity and native/catalogue state, but is memory-only. Inactive history or serialized seal fields must not be promoted to a fresh authority merely because their checksums validate.

## Existing test scope

`partial_native_attempt_reopen_stays_unready_and_new_attempt_rejects_late_chunks` reopens the target and obtains another actual **committed-handoff** capture through `Fixture::recapture`. Its permanent handoff promises provide a different source-cut path. It does not demonstrate all-owner loss during a **Normal CurrentVoterRepair** with only n0 and the Installing n1 available. The completed-window tests exercise the later published-receipt boundary. Neither should be cited as proof of this earlier cold-resume case.

## Smallest next actual test

Extend the copied compacted continuation with a test-only stop immediately after complete history staging/audit and target clear, before copying rows. Preserve the actual intent, stage, Sparse1026 checksum, target birth/UIDs, local visibility sequences and all 1,025 external ACK digests. Drop every owner and verify weak DB references drain; copy and fsync the stopped tree before strict reopening. Never open the original retained source.

Using actual reopened metadata stores and independently fresh witnesses, try ordinary fence acquisition on n0 and n1 with n2 data delivery unavailable. Retain the first refusal and actual quorum result. Check that n0's full native1026 history and acknowledged values remain, target1 is unknown/unready, the exact sparse obligation remains, and ordinary protected operations cannot treat the partial image as ready. Also record the new Runtime's missing-session refusal and the absent published installed-base boundary. Do not fabricate a SourceCut, clear the intent, create genesis, reuse stale DB-open tokens or invent an Applied label to progress.

If this confirms the dependency, the next design should be a **separate, exact interrupted-install admission** which revalidates the actual persisted session/intent, target birth/UIDs, all native/catalogue/sparse obligations and source lineage under fresh metadata and quiescent ownership. It must enable refusal-only quorum observation and an owned resume session without making ordinary fence, readiness or serving guards permissive. This review does not implement or qualify that mechanism.

## Source locations

- `crates/bicdb-core/src/distribution_range_recovery/runtime.rs`: `fence_epoch`, `frozen`, `source_capture_freeze`, `transfer_image_install`.
- `crates/bicdb-core/src/distribution_data.rs`: `preflight_native_recovery`.
- `crates/bicdb-core/src/distribution_range_consensus/recovery_state.rs`: `preflight_recovery_fence`.
- `crates/bicdb-core/src/distribution_range_consensus/image_install_state.rs`: `ImageInstallIntent`, `require_no_image_install`.
- `crates/bicdb-core/src/distribution_range_consensus/image_install_fence.rs`: `begin_native_image_install`, `require_native_install_recovery_preflight`, `same_install_lineage_for_new_attempt`.
- `crates/bicdb-core/src/distribution_range_image/source_cut.rs`: `CertifiedRecoveryImageCut::from_verified_source_history`.
- `crates/bicdb-core/src/distribution_range_image_install.rs`: `begin`, `recover_completed`, partial/completed-window tests.

No data loss, new application ACK, StrongGet, transparent SQL, VM crash or physical qualification is established by this review.
