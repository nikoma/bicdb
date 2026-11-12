# Full native catalogue custody integration (source only)

This successor starts at full prototype
`61eb4d92b5938349cf18a283e936eee45fa97e9d`. It imports the read-only custody
correction a266 (mapped c60f) and the paired acceptance successor 802897ee5
(mapped a75d, including the original actual fixture/custody files). Original
read-only branches, baseline failures and passing evidence remain unchanged.
No release/publication implementation from 61eb is modified here.

The separate read-only cohort 25710b785 passed the paired intact/missing
acceptance (run43221341175, one test, 4.23s) and 13 writer-enrollment controls
(run43221345069, 0.92s), with all-target compilation passing. Those results do
not execute this Abort15 composition. Only formatting, diff and version checks
are performed locally; this successor requires focused remote validation.

## One actual fresh owner

The full prototype already has a from-birth Abort1 capability and protected
native Pending-to-Registered catalogue identity. A blind import of the read
factory's empty12 would either conflict or let an unproven existing empty file
be adopted. Abort-aware genuinely new factories now skip empty12 entirely.
After the actual birth/preflight and both reader fences, before returning the
factory, they initialize the existing protected Pending StoreUUID and exact15
catalogue, then register that same UUID. Later genesis completion verifies the
Registered identity instead of trying to create another catalogue. Existing
non-Abort read-profile factories retain their private fresh empty12 path.
That path explicitly rejects an Abort-capable birth.

The Abort initializer now consumes a nonserializable/non-Clone
`NativeAbortCatalogueBootstrap`. Its private constructor borrows the actual
DB/birth, original protected Pending identity and actual native journal. The
consumer revalidates that exact native record/checksum/birth/open, Abort1 and
custody reader fences, fsync, no protected READY, no current/previous native
writer policy and no protected slot inventory. A decoded sidecar, zero counters,
a caller UUID or absence of a policy cannot construct this capability.
A Registered identity never returns to Pending or recreates a missing file.

Raw creation uses the same create_new/file-sync/directory-sync byte writer as
the read-profile bootstrap. Existing-file continuation is only an exact valid
Pending15 envelope with the original protected UUID and unchanged writer marker,
with every range/backup/recovery/image/sparse/installing/joint/abort-obligation
map empty. Protocol-None, even from an actual empty read-profile factory, refuses
without changing it. Nonempty or partial/corrupt files refuse; no CQ or promise
is cleared. Valid exact Pending15 is re-fsynced before native Registered. An
actual absent file may be created only within the original protected, never
exposed Pending scope. The native identity remains borrowed under the same
journal through raw publication and validation.

Strict Registered cold loading retains the actual protected StoreUUID check and
returns `NativeRangeCatalogueCustodyMissing` for actual NotFound at that earliest
boundary. It never falls through to an empty RangeWriteStore. The dedicated
read-only custody rule, original loaded-existing provenance, actual policy
validation and mandatory `native_catalogue_custody_v1` remain. Neither an old
private prototype nor a policy-free root acquires new birth capability.

## Inherited and additional controls

The intact/missing stopped-cut twin preserves its original ACK/hash/Strong
assertions. The missing copy must return only the dedicated custody refusal on
three repeated cold attempts, create no catalogue/hook and preserve protected
current/previous policy digests. Restoring the exact original file occurs only
after all refusals, solely to inspect unchanged native facts, not as automatic
repair or a subsequent successful read.

All four `native_catalogue_custody_` read controls remain. The repeated-open
control now drops the first actual live Store owner before cold raw opens;
Abort15's exclusive catalogue owner is preserved. Two new controls are additive:

- `native_catalogue_custody_abort15_pending_all_owner_continuation_keeps_original_uuid`
  executes the real first-bootstrap before-write, before-file-sync and
  after-durable cuts, drops every actual DB owner, strictly reopens, completes
  original initial schema/genesis through the real producer, and requires the
  original protected Pending UUID, birth and namespace to survive Registered
  and a further all-owner cold reopen. This initial, never-exposed schema
  creation is not a schema retrofit onto an admitted root.
- `native_catalogue_custody_abort15_pending_never_adopts_protocol_none_empty_catalogue`
  uses bytes from a genuine separate non-Abort empty12 factory. Installing those
  DATA bytes at the Pending target must not authorize their adoption; actual
  Pending checksum, file bytes, no-READY and no-policy facts stay unchanged.

Existing `native_abort_birth_registration_refuses_missing_or_foreign_range_catalogue_at_r0`
and the actual TLS policy-WAL-before-hook negative are retained. These are
source controls, not current passing execution claims. The original source
Native ACK/Strong custody witness is never rewritten as a full-prototype bug
execution; this full composition is a separate necessary gate.

Valid-old-copy/same-UUID rollback remains a separate protection/recovery
contract; this integration does not infer its closure from UUID equality. It
adds no NoData quorum, Joint grant, release, current metadata authority, new
ACK, Strong readiness, operator recovery or physical HA claim. Old unsupported
prototype/Pending roots lacking the required new reader fence remain quarantined.
