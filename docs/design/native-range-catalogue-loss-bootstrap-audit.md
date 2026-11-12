# Missing native range catalogue: read-only audit and minimum repair contract

Audit source: isolated read delivery `480db3b52820919b0c18a48e15cccda38fe7c1c1`,
not the full Abort/Joint prototype. No runtime fix or Cargo was run for this
note. Driver's genuine ACK / all-owner stopped copy / remove-ONLY-catalogue
baseline is independent and must establish the first actual failure boundary.
Source reachability alone is not a false-ready, lost-ACK or data-loss result.

Execution update: the [retained paired witness](../evidence/native-read-catalogue-loss-2026-11-12/README.md)
at source 3d0041e09 confirmed successful Strong absence after the selected
catalog-only deletion, following an intact full-value control. This note remains
the repair contract; a source implementation and acceptance controls now exist
but their runtime validation is pending. The original Azure cause and ordinary
main487 single-host/backup impact are not established by that witness.

## Existing source boundaries

| Source symbol | Actual behavior at the audited source |
| --- | --- |
| `RangeWriteStore::open`, `distribution_range_consensus.rs` | NotFound constructs empty state. The enforced-writer rejection of old format/marker is conditional on `existing_catalog`. A missing file therefore becomes native format12/Some1 in memory. |
| `SealedHostWriterEnrollment::from_actual`, `distribution_native_writer.rs` | Consumes real HostMQ and actual native DB, but binds only ephemeral store-open ID plus birth/facts; it does not attest original catalogue load provenance. |
| `BicDb::install_enrolled_native_coordinator`, `db/range_image_native/writer_enrollment.rs` | Loads actual native current writer-policy receipt. Previous/current physical birth must match, but previous store-open ID is intentionally replaced on reopen. There is no durable catalogue identity/load-origin check. |
| `ClusterDataNodeService::open_with_schema_runtime`, `distribution_data.rs` | Captures actual storage policy before creating learner/range/digest state, opens range store, then startup replay. This is an early actual-DB refusal seam. |
| `RangeWriteStore::persist_with` | Calls native writer-host guard BEFORE directory creation/write, and rechecks afterward. A blank open has no installed weak policy, so ordinary raw writes are currently refused. |
| `persist_recovery_catalog_with` | Uses `persist_with`; it does not provide a policy-free empty-catalogue bootstrap. |
| `persist_verified_open_catalog`, `distribution_range_consensus/native_writer.rs` | Exception to ordinary policy gating: writes a validated envelope directly. Only `open()` calls it, for migration or existing recovery/image refusals. Its purpose must remain unchanged validated-existing data, never a missing-file replacement. |
| `native_writer_enrollment_active`, `format.rs` | Rereads format metadata and calls `validate_supported`. A new mandatory local reader flag can make older generic store opens and warm policy guards refuse. |

At this exact source, a freshly synthesized native12 catalogue has no recovery
or image maps and `migrated=false`, so `open()` itself does NOT persist it.
Ordinary raw prepare/commit/certify/apply/abort ultimately require installed
native policy before persistence. Startup replay of the blank catalogue has
nothing to replay. These are useful existing defenses, not a sufficient proof
that every future path cannot accidentally write a blank before enrollment.
The explicit `persist_verified_open_catalog` exception must be covered by the
new invariant as well as ordinary/public raw APIs.

## Minimum missing-only refusal

The actual existing native WAL writer-policy receipt is an EverEnrolled
REFUSAL fact, not authority. Malformed/unreadable native policy is an error,
never absence. If that receipt exists, enrollment MUST require a catalogue
that THIS actual store open originally read and successfully verified. Missing
catalogue cannot be reconstructed from metadata, native R, row counts, absence
of a pending marker or a later newly created pathname.

Retain a private, immutable per-open origin such as `LoadedVerified` versus
`MissingAtOpen`. It is minted only by the original successful file read,
identity/checksum/format validation; not by `Path::exists()`. Bind root, actual
store-open identity and the decoded catalogue. Own writes MUST NOT mutate
MissingAtOpen into LoadedVerified. Before sealing/publishing policy, recheck
actual catalogue existence/identity/current validated bytes under its real
owner; a file removed after open is not protected by an old boolean.

Check the prior actual DB policy and missing condition BEFORE any catalogue
creation/migration/bootstrap write. Prefer an actual DB-aware early admission
at service open, with generic enforced missing opens refusing by default and a
separate genuine-fresh exception. On failure no range catalogue is written.
Repeat the failed open/seal twice and after all-owner reopen: path stays absent,
no new policy/hook or clean-ready proof appears. A fake blank file created by a
failed attempt must never make the next open eligible.

The check and policy publication must retain the SAME actual service/store
ownership and serialization boundary. An earlier separately opened catalogue
handle can be stale even though it once loaded a real file; compare actual
current file with that owner's in-memory envelope before reseal and do not
allow a second independent store cache to overwrite newer CQ state. Original
load provenance is necessary, not a replacement for exclusive current ownership.

Lock order also matters: existing `install_enrolled_native_coordinator` holds
its native journal guard from reading previous policy through hook publication.
Do not add a policy-history getter inside post-publication coordinator/service
validation that recursively acquires that same journal. Read/validate at the
actual existing guard seam, or pass a narrow opaque captured fact and recheck
using a genuine under-journal adapter. No MetaMutex→DB/network inversion should
be introduced by this repair.

This is a missing-file contract. A verified but old valid catalogue at the
same identity remains a DIFFERENT rollback case. Do not call this full catalogue
history integrity or native data recovery.

## Genuine first bootstrap and order

First bootstrap must remain possible only through the actual new enforced
factory/initialization owner, with real native birth/root/open and genuine
initial physical proof, no previous native writer-policy history, and the
exact expected empty catalogue. Neither a metadata declaration nor generic
`RangeWriteStore::open` can mint that permission. Merely observing no policy,
zero raw/native counters or no file is not a fresh-factory constructor.

The cleanest ordering is private fresh-factory bootstrap **before the first
service/range-store open**: write the exact empty native catalogue with fsync
and directory sync, reread/verify it, then open the real store from those bytes.
Thus even the first `SealedHostWriterEnrollment` has genuine LoadedVerified
provenance. Only afterward may first native writer-policy WAL publication occur.
No raw policy-free writer API is exposed by that bootstrap.

If initialization instead creates the file after store open, preserve its
MissingAtOpen origin and carry a DISTINCT sealed first-bootstrap receipt bound
to the exact actual creation attempt/open/catalogue/policy candidate. Do not
rename it LoadedVerified. Also avoid rechecking "previous policy now exists"
after publishing that SAME first policy and then incorrectly rejecting its own
hook: post-WAL validation must consume the exact real candidate/publication
receipt, not infer a generic preexisting catalogue from the pathname. Creating
before store open avoids this extra state machine.

| Actual crash/error cut | Required result |
| --- | --- |
| Initial empty-catalogue file/directory fsync fails | No policy WAL publication and no writer hook/ACK. Live original factory owner may retry exact empty attempt. Cold retry requires genuine protected/prepolicy bootstrap provenance; otherwise explicit quarantine, never generic missing→empty. |
| Catalogue durably written, before first policy WAL | Strict open reads/validates those existing empty bytes; actual no-policy/genuine initial scope plus NEW fresh HostMQ permits first enrollment. Do not rewrite/reset the catalogue. Unexpected nonempty prepolicy state refuses. |
| First policy durably published, before hook (including lost/error return) | Catalogue was already fsynced by order. It remains mandatory on ALL later attempts. Strict all-owner reopen may reseal this actual birth/open using that verified existing catalogue and fresh HostMQ; missing catalogue refuses permanently until separately authorized recovery supplies its real history. |

Positive controls: genuine first factory enrollment with catalogue initially
absent from the caller's view; exact durable empty-before-policy crash; actual
policy-before-hook crash; normal existing-catalogue cold reopen preserving all
CQ/Applied/refusal bytes; original policy/DB-open mismatch refusal. Inject
rename-success/dirsync-error and preserve actual ambiguity. No new ACK/Ready
is inferred from guard completion.

## Reader compatibility and persistent identity

The existing native-writer flag and format12 are already understood by the
buggy reader, so changing only new code is NOT an old-reader fence. Activate a
NEW mandatory node-local reader capability before the new policy contract can
expose counted writers. Older `format::validate_supported` must reject actual
stored bytes before opening/creating the missing catalogue; the warm raw guard
also rereads that format. Keep this flag out of portable schema capabilities.
A new policy record version/mandatory binding may add a second explicit refusal,
but do not rely on ignored optional JSON fields. Validate with real newly
produced bytes against the historical reader/codec and retain None/legacy byte
parity where the old contract is deliberately preserved. Do not silently
retrofit an old root lacking genuine fresh/bootstrap provenance.

A stable catalogue UUID is useful for rejecting a FOREIGN or recreated
catalogue and for deterministic initial crash continuation. It is NOT strictly
necessary to close the selected missing-ONLY-after-policy path if the earlier
load-origin/no-write ordering is complete. If introduced, use an independent
small native WAL Pending→Registered identity receipt bound actual birth/root,
with the same UUID/checksum in the catalogue, exact empty-before-policy ordering
and a new reader fence. Do not import Abort's decision/promise protocol merely
to obtain an identity primitive; do not reuse Installing13/Joint14/Abort15
semantics or infer those capabilities from a numeric format.

Pending may resume only the exact original never-exposed empty attempt; an
orphan cannot erase data. Registered missing/wrong catalogue always refuses and
cannot reenter first bootstrap. Old roots without the capability must keep an
explicit older contract or refuse; no retrospective "never committed" proof.

A UUID ALONE does not detect a valid SAME-UUID old catalogue copy. That needs
separate protected irreversible generation/hash/body anchors, updated with the
real CQ/refusal ordering and safe cross-file crash reconciliation (or an actual
native authoritative journal). Native materialization floors alone miss a CQ
written before native pending/materialization. Those rollback/prepolicy
ambiguous cuts remain OPEN. No minimal missing-file fix should claim to close
them or the full fault-safe milestone.
