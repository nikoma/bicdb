# Protected proposal before interrupted current-image installation

This is a proposed protocol, reviewed against private source `bc3953904659856be93a96d0efdddfd6cde1dbb5`. It adds no runtime implementation. The actual two-command characterization is separately frozen at `52c09955`; its execution is pending when this proposal is written. Existing intent JSON remains refusal-only.

An accepted compatibility receipt written only after full history staging/audit is too late: a process can stop immediately after durable BEGIN, before that receipt exists. The minimum complete design needs a **protected proposal and preserved preinstall point before publishing BEGIN**, followed by fresh, purpose-specific interrupted-install admission. The later audit can refine compatibility; it cannot be the first fact needed to resume.

## Opaque producers and ordering

Proposed API names below describe contracts, not available capabilities.

| Producer | Required actual inputs | Result and limit |
| --- | --- | --- |
| `capture.historical_repair_acceptance()` | Actual `CertifiedRecoveryImageCut` produced from `VerifiedRecoverySourceHistory`, its actual fenced data quorum, live native source lineage and owned capture pin | Opaque historical source-QQ acceptance fact. It grants no current authority and asserts no target compatibility or completed image. |
| `db.persist_native_current_image_proposal(...)` | Historical acceptance above, actual native admission, drained owned target freeze/control lease, current native safety and full catalogue/sparse/pending obligations, exact fresh recipient role, actual local UID/schema/birth domain | Native-WAL `ProtectedCurrentImageProposalReceipt`. No invalidation, Installing marker, row change, floor advancement or serving grant. |
| `store.begin_current_image_install(..., &proposal)` | Exact actual proposal receipt, current role and same target/native/store bindings | Durable intent referencing the exact protected proposal ID/hash, then native Installing marker. Never an intent reconstructed from a proposal DTO. |
| `runtime.acquire_installing_fence(...)` | Fresh authenticated initiator/recipient metadata witness, exact durable intent and protected proposal, current native/catalogue obligations, fresh challenge, actual quiescent ownership | Opaque **InstallingFence** observation and refusal-only resume ownership. It reports preserved lower bounds separately from dirty/unknown current physical state. It cannot become ordinary Ready or serving authority. |
| `resume_round.finish(...)` | Distinct actual current-voter majority of ordinary and exact InstallingFence observations, independently current source native lineage/full history, matching protected source cut | Fresh purpose-bound resumed cut and owned resume admission under this round's original deadline. No numeric or DTO constructor. |

Required order under native-journal serialization and owned counted mutation protection:

1. Validate the live typed source authorization, recipient role, target birth/UIDs and all actual irreversible obligations. Acquire network authority **before** DB/service latches. Protect reader compatibility before publishing new trusted fields.
2. Write and fsync the immutable protected proposal, its complete historical audit manifest and the preserved preinstall point in native page WAL. Read back the actual committed record and mint its opaque receipt.
3. Publish/fsync the catalogue intent containing that receipt's exact ID/hash. Revalidate the pointer and complete scope before returning a fence.
4. Publish the native Installing marker bound to both the final intent hash and proposal. Only then stage, audit, clear or copy through the existing controlled installer.

Avoid a hash cycle: the proposal binds a canonical *intent-core scope* excluding its own pointer. The catalogue intent adds the protected-proposal pointer and gets its final hash. The Installing marker binds that final hash and the proposal pointer. Each link is validated exactly.

A native proposal with no referencing intent is an orphan historical record. It must leave the ordinary native point, prefix history and source generation unchanged; merely finding it cannot mark a range Installing or block a later valid native point. An intent with a missing, mismatched, incomplete or corrupt proposal is quarantined. It must never fall back to the old intent codec or infer an empty preimage.

## What the protected proposal binds

The source half records the actual historical acceptance of a particular native cut: cluster/bootstrap, logical range span, source node/member incarnation and stable birth identity, R and portable H0/H(R), actual native receipt commitment, original configuration/voter credentials, schema/readers/restore domain, recovery session/nonce and original metadata/data-quorum provenance. The current `CertifiedRecoveryImageCut` retains source facts but not the complete original quorum audit after its factory consumes `VerifiedRecoverySourceHistory`; that factory is the point to produce this historical acceptance from actual opaque inputs.

The audit must identify exactly the actual distinct authenticated voters and their original metadata contexts/frontiers and complete obligation commitments. A record written by that trusted producer is historical local acceptance, not a portable signature manufactured from a list of voter IDs. Do not describe embedded test identities as mTLS evidence. Native checksums detect damaged records; they do not establish authenticity against an attacker who controls the host filesystem.

The target half binds the actual stable native birth, cluster/bootstrap/member incarnation, restore identity, exact local namespace UIDs, source-schema/portable-reader contract, session and attempt. It preserves the actual preinstall native history point and its immutable receipt/generation, actual applied and materialized-pending lower bounds, full pending command identity, and the complete raw CQ/Applied/current-certification/permanent-sparse obligations. Retain the required history generations/journals while an intent/proposal references them. Current dirty rows are not the preinstall native proof.

Record the target's observed local MVCC sequence and actual native visibility receipt separately from R and page-WAL XID. An observed DB counter is not itself a visibility guarantee. The final imported image still needs the existing actual target-local visibility receipt, full content/UID/schema verification and staged native-history publication.

The proposal is not a completed row-image certificate: before row export finishes there may be no final content root. Fresh resumed source attestation must inspect the actual source native lineage and full history, and full target verification remains mandatory. Historical source acceptance is also **not** proof that the target's local history is compatible. Before any destructive operation, stream/audit source history against every current and protected preimage obligation. A later protected compatibility receipt can record that audit, without changing those distinctions.

Complete audit data can exceed a single record: the native record helper currently caps records at 8 MiB, and a full multi-voter obligation set can exceed that. Use bounded immutable chunks plus a fully checked manifest root, or reject before BEGIN; do not silently truncate. Only a completely published manifest can yield a proposal receipt. Bound record/attempt counts and retain referenced records; garbage collection needs an explicit safe reference rule, not elapsed time.

## Fresh InstallingFence and resume

On strict reopen, first validate the actual protected record, intent pointer, native birth, restore lineage and local UIDs. A historical loader may mint `HistoricalCurrentImageProposal`, but cannot return a fresh quorum proof. Preserve original recorded DB-open provenance unchanged, and bind every new opaque observation to the **actual new** DB open. Do not require the original ephemeral open to survive.

The new endpoint must preserve generic `require_no_image_install` behavior. It is admitted only for the exact referenced interrupted installation and independently fresh metadata role. It acquires/drains a refusal-only freeze without requiring a new source cut first; otherwise it retains the existing source-cut/session cycle. Normal receiver/source operations cannot consume that resume freeze as a ready session. The returned observation separately includes:

- actual current dirty/unknown native observation;
- preserved protected preinstall history/floors, actual current native safety, and all current raw/sparse/pending commitments;
- exact intent/proposal/attempt/session, actual birth/UID/restore/current-open and current authenticated actor/recipient;
- fresh metadata context, term/read frontier, recipient challenge and bounded original parent deadline.

No field labels the target at the proposed source R. The current source must supply actual source-native proof and full source history covering **all** observed and protected obligations. A new majority checks distinct current identities and exact semantic configuration. Only its sealed result may authorize an exact resumed source cut and a new counted install lease. A same-cut retry may use a fresh attempt and DB-open binding; it must retain the parent proposal, old floor/history and all sparse promises, reject late old chunks, and persist the new pointer before any new rows. A new attempt cannot recalculate an unknown dirty preimage as zero.

Never renew the original deadline of a running acquisition. A later acquisition has its own business UUID, nonce/challenge and original deadline and performs a new fresh quorum; stored historical timestamps/deadlines are not leases. Every subsequent bounded local phase still requires a live exact role. Expiry, ambiguous persistence or incompatible current history returns refusal.

## Epochs, leadership and membership

Metadata Raft leadership/term and range leadership/epoch are different domains. A later metadata leader, ordinary heartbeat/load updates or changed local process-open identity need not invalidate a historical proposal. Use fresh metadata progress not behind the proposal's historical frontier, exact restore lineage, and semantic range/configuration checks that ignore only documented non-authoritative load estimates. Never compare a metadata index with R or a local DB sequence.

The smallest initial implementation may support only unchanged data-voter configuration and range-leader epoch. It must state that limitation explicitly. A higher **range** epoch or another range leader cannot be handled by overwriting the old proposal's range/configuration fields to match the new role. Preserve the original historical domain. A separate reauthorization producer must consume actual fresh current data-quorum fencing, retain every prior irreversible/preimage/sparse obligation, prove source-history inclusion at the old protected R/H, and establish permanent rejection of old-epoch writers before issuing a new cut. Resetting the current nonce's marker map is not proof that older promises disappeared.

If the voter set changes, a majority of the new configuration can miss an old ACK majority. Fresh metadata membership alone is insufficient. Require actual qualified old/new joint-data-quorum transition evidence, or a durable qualified handoff/activation proof binding both configurations and the old history commitment, plus fresh current quorum/source history. Until such a producer exists, refuse the transition. New member incarnations, unproven credential transitions, changed restore lineage or recreated target namespace UIDs likewise require their own authorized transition; no old proposal may silently retarget them.

## Crash matrix and tests

The default two-command baseline `52c09955` must run unchanged before implementation. A characterization PASS means its reported cold recovery scenario is FAILED/unsupported. Future tests must cover each boundary independently:

| Last durable boundary | Required restart behavior |
| --- | --- |
| Proposal only, no intent | Ordinary native point unchanged; orphan never grants mutation or readiness. |
| Intent, before Installing marker | Exact pointer validates; special fresh refusal observation/resume works without inventing stage completion. |
| Installing marker, before stage or mid-stage | Unknown/unready; protected preimage retained; restart bounded full compatibility staging. |
| Staged/audited, before clear | Fresh admission and exact scope required; stored audit is not fresh authority. |
| Clear or partial copy | Actual baseline boundary: preserve all old obligations, obtain fresh InstallingFence quorum, restart/resume bounded copy without false Ready. |
| Native published image, before catalogue adoption | Existing full actual-image revalidation and exact receipt recovery still required. |
| Adoption/owner-return/Prepared cuts | Preserve the existing typed finalization and atomic exact-intent-to-Prepared path; no transient generic admission. |

Include missing/wrong proposal pointers, torn manifests, post-rename persistence ambiguity, wrong birth/UID/restore, stale opens, late old attempts, source-history forks, higher-epoch and changed-membership refusals, and expired original deadlines. Keep R1/R2 and compacted R1026 cases, including actual target MVCC reads and external ACK reconciliation. None of these component tests alone qualifies production HA, StrongGet, transparent SQL or physical crash recovery.

## Existing code hooks

- `db/range_image_native.rs`: reserved `native_key`, `read_native_range_image_record`, `write_native_range_image_record`, `MAX_NATIVE_RECEIPT_BYTES` (8 MiB). The generic writer is a storage primitive: a new producer must explicitly own the native journal and counted mutation permission rather than assume the helper supplies authority.
- `BicDb::in_paged_transaction`: actual native page-WAL transaction/XID and transactional receipt publication. Page XID is not a range or MVCC read bound.
- `db/range_image_native/serving_configuration/activation_decision.rs`: useful pattern for a sealed actual quorum producing a protected native record, immutable historical archive, actual page-XID/checksum and strict readback. Its activation decision is not an image-proposal substitute.
- `distribution_range_recovery/image_compatibility.rs`: `VerifiedRecoverySourceHistory` still has the actual initial authority and verified source lineage; use its factory boundary for historical acceptance.
- `distribution_range_consensus/image_install_state.rs` and `image_install_fence.rs`: exact new pointer/intent binding, durable refusal and same-lineage restart checks. New pointer trust requires fail-closed old-reader/catalogue protection; existing readers must not silently drop it.
- `db/range_image_native/history_stage.rs`: bounded inactive history, same-page progress and publication validation. A later compatibility receipt can reuse actual audited stage proof.

All names introduced here are proposed. No protected proposal, InstallingFence exception, cold resume producer or new qualification result is delivered by this document.
