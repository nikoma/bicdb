# Registered remote target: durable Installing catalogue

This is an unmerged native-image component, not qualification of automatic
rebalance, copied data, Ready, Serving, cold recovery or physical scale-out.
It follows the protected proposal in `native-registered-remote-proposal.md`.

The original Registered host retains the actual source Manifest owner, original
receiver/hook/database-open/Store-slot identities, exact protected proposal and
actual runtime freeze rotated from handoff owner N to distinct install attempt
A. A new `NativeRegisteredRemoteInstallingPublication` retains both the exact
original and proposed catalogues before any catalogue mutation or file write.
No JSON pointer can construct this owner or the opaque resulting fence.

The dedicated service sink requires the actual native journal/admission/hook
scope, fresh recipient Role/OWN/MStore observation and actual authenticated
source frame. It verifies the native CURRENT/archive records, actual committed
page transaction, complete source manifest, canonical pre-pointer intent,
namespace identities and native/raw preimages. It persists a refusal intent;
it does not change user rows, native logical range floors, writer enrollment,
metadata membership or read readiness.

The separate catalogue format 17 persists `registered_proposal` rather than
reusing the current-voter-repair pointer. Old readers reject the unknown format.
The local Registered proposal reader flag is durably re-published on every
continuation. Format 17 requires every learner install intent to have its
Registered pointer, with matching A, positive native xid, handoff purpose and
mutually exclusive pointer types. Upgrading refuses other legacy learner
intents. It preserves actual absent optional joint/current maps from format 15;
any present joint state still receives its complete semantic validation.
Generic local image installers reject this profile and cannot take over its
retained Registered attempt.

Before actual catalogue replacement the sink installs the refusal in memory
and marks persistence uncertain. Atomic replacement uses file and directory
fsync at `registered_remote_install.catalogue`. An I/O error or canceled reply
retains A, proposal and candidate; it never restores the old memory or invents
a replacement attempt. A narrow fresh-request continuation validates that
memory is exactly this publication's original or candidate catalogue, allowing
this retained owner to reach its re-fsync sink despite uncertainty. Ordinary
writer/custody admission remains strict. Every retry repeats durable replacement
and exact envelope readback before publishing a verified fence.

The opaque fence provides readonly catalogue/database constraints for a later
separately scoped history/row producer. Those constraints alone cannot authorize
a transaction. The original B0 proposal scope is insufficient after copying
rows; a distinct actual mutation scope and complete source-history audit remain
necessary. Current implementation does not reconstruct an owner after process
loss, resolve unknown native WAL outcomes, copy rows, publish a final Image,
produce remote NEW/decision/Open, or remove retained source sessions.

New regression selectors exercise the genuine fifth target with actual proposal,
actual source frame, same A and original protected catalogue:

- `native_registered_fifth_remote_installing_actual_catalogue_same_owner_preserves_rows`
- `native_registered_fifth_remote_installing_actual_after_fsync_cancel_fresh_same_attempt`
- `native_registered_fifth_remote_installing_actual_directory_sync_error_retries_exact_catalogue`

The directory-sync control injects EIO at the existing real I/O seam after
rename, twice across fresh requests, then verifies exact-candidate re-fsync.
The cancellation control cancels the caller after successful file/directory
fsync. Semantic controls recompute outer checksums after pointer erasure,
attempt substitution and format downgrade, so checksum mismatch cannot mask a
missing semantic check. These are hosted component tests, not power-loss tests.
At initial source publication these new selectors have not yet been executed.
Historical failed cohorts and evidence remain unchanged.
