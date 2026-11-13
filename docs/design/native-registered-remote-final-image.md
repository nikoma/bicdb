# Registered remote final physical image

This component is source only and has not been compiled or run. It builds on
runtime `22c8d2e62` plus the separate fixture correction `7ac4f2672`. Earlier
proposal, Installing and row results do not validate this component.

The original Registered image owner now supports `BeginVerification`, bounded
`VerifyTarget`, and `PublishFinalImage` through the actual
`advance_registered_remote_image` caller. Each request obtains fresh original
target Role/OWN and an authenticated Finished frame bound to the SAME issued
MF owner. It borrows the retained A lease and original hook, admission, native
journal, DB-open, Store, protected catalogue and namespace identities. It does
not construct a local source lineage, ordinary image fence or local OLD quorum.

Verification first requires actual complete audited stored history and exact
committed protected row progress, with no pending row transaction or unresolved
row outcome. It compares the complete source seal against the MF, history wire
chain/count, row wire chain/count/bytes/root and source snapshot bounds. The
serializable seal alone cannot create the verifier or a final receipt.

The retained opaque verifier captures target B exactly once, target image
generations, actual index definitions, schema and namespace bindings, and its
own generation. It independently reads target primary storage across every
collection and all selected-range records. The scanner counts raw rows,
including nonmatching ranges, toward a caller-specified bound from 1 to 16,384,
with 1,024-record/8 MiB export batches. It preserves a resume key even across
nonmatching rows. Each step requires actual durable and visible watermarks at
least target B. The hash includes the canonical catalogue and ordered complete
record payloads; count, byte count and hash must all equal the Finished seal.
Missing and extra target rows cannot be hidden by the retained import hash.
Progress is published only after original-point postchecks. Conflicting
contents invalidate that verifier; a fresh request cannot reset its generation.

Actual index definitions are recorded and rechecked throughout the scan and
final publication. Independent index/FTS/vector parity is not established by
the primary-row scan. The admission-write index verifier is not called while
the Registered admission-read scope is held. Memory-index assets, rebuilds and
recovery remain separate gates. Existing local spatial maintenance audits may
describe the earlier physical copy; these are distinct from business audits.

The retained final publication owns exact candidate bytes and the actual native
page xid before its first put. It writes distinct protected CURRENT and archive
records in one native transaction, retaining a successful commit return before
any postcheck or response. Fresh same-owner reconciliation requires exact bytes,
CURRENT/archive parity, actual committed page status, original identities,
unchanged target generations and definitions, actual complete row/history
headers, fresh source frame/Role/OWN, and healthy durability/visibility state.
An unknown outcome cannot select another xid, new attempt or replacement owner.
The local image reader fence is durably re-published before protected writes.

`NativeRegisteredRemoteFinalImageReceipt` is an opaque, non-Serde actual
protected receipt. The original host can borrow it through
`registered_remote_final_image_receipt(range)`. Its `require_current` method
requires the actual lexical image context; the borrow or pointer alone grants
no operation. It exposes the actual source manifest/seal, stored history
generation, target reservation, image generations and actual index definitions
for a later, separately implemented adoption producer. Its protected record
also binds target birth/open/Store/catalogue/namespace IDs, proposal/intent,
committed row header/xid, history header hash, verification generation, complete
source seal, target scan bounds, metadata provenance and final native xid.

This component intentionally writes no `install:base`, native integrity range
floor, range-log floor, catalogue adoption, Ready or Serving record. Existing
local anchor validation requires a local history-publication archive; it cannot
accept the distinct Registered history DATA by casting. A later Registered
adoption producer must validate the actual final receipt and original A, publish
the matching native history/anchor contract, and retain outcome custody through
that new invariant boundary. Remote NEW, durable decision, Open/release,
source-stream closure and OLD cleanup remain separate actual producers.

Source captures and OLD owners still retain their cap-eight bounds. This final
receipt neither drops the source read pin nor releases permanent OLD/Frozen
ownership. Cold reconstruction, unknown-row resolution, populated Assigned
second-range movement, controller integration and physical qualification remain
open. Bounded raw-row steps do not establish bounded filesystem I/O or physical
RTO; the existing service mutex and native journal still cover native fsync.

Four source selectors under `native_registered_fifth_remote_final_image_actual_`
exercise the real fifth receiver and original actual source frames:

- `independent_scan_and_native_receipt`
- `refuses_without_independent_target_scan`
- `missing_physical_row_is_not_hidden_by_import_hash`
- `post_wal_cancel_reconciles_same_xid`

These reuse the genuine local-OLD-seeded wire fixture, not a completed remote
NEW/decision/release campaign. The corruption control deliberately deletes an
actual target primary row in a page transaction, then requires independent scan
failure despite the unchanged import hash. The cancellation control revokes the
actual caller after native final commit, retains its original xid/bytes and
reconciles through a fresh bounded request. Evidence is saved before final
assertions and retains the original outer Manifest/frame journal. No test in
this batch is claimed as passed, hardware power loss or production qualification.
