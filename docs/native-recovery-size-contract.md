# Interim native recovery size contract

This source-only P0 batch limits new **image-aware native** originating commands
before local or remote Prepare, Commit or Certify. The complete serialized JSON
`RangeWriteLogEntry`, including the longest state wrapper, must fit 2 MiB.
This is a command/entry limit, not a 2 MiB user-value promise: escaped strings,
collection/key identities, mutation metadata and multiple mutations consume it.
The check serializes borrowed input to a counting writer before cloning records
or allocating a command checksum buffer. Historical candidate selection and
entry export use the same limit.

Native TCP pre-admission additionally requires `max_frame_bytes >= 4 MiB` and
checks the exact normal Prepare/Commit/Certify/Apply/Abort envelope shapes for
all configured destination identities, using the maximum request-ID width.
This pass performs no I/O and consumes no request ID. Four MiB is conservative
space for bounded recovery scope/status envelopes; each recovery sender must
still validate its actual complete envelope against its transport frame limit.
It does not promise arbitrary-size topology or recovery metadata fits a frame.

Legacy command construction, existing ordinary client value limits and global
transport configuration validation are unchanged. New native admission returns
an explicit pre-decision error on excess size or insufficient transport frame
capacity. A failed pre-decision admission leaves no native pending record or
quorum command; a genuine subsequent healthy write must remain possible.
Existing acknowledged oversized commands are not reclassified as aborted and
must remain unavailable with a diagnosis if the bounded recovery path cannot
represent them.

The new focused test sources (not yet executed at this checkpoint) cover the
exact complete-entry boundary, the legacy
constructor distinction, validation-only envelope equivalence/frame refusal,
and an actual local three-service/fsync native-origin refusal before any
Prepare/Commit/Certify RPC or native pending state followed by a healthy ACK.
These tests are not physical HA, large-value or production qualification.

Streaming recovery for larger entries, the advertised multi-megabyte value
contract, aggregate retained-catalog bounds, ordinary native follower
materialization, complete audit/sync effects and fault-safe membership changes
remain separate gates. This limit closes a new-admission/recovery mismatch; it
does not satisfy a four-MiB value guarantee.
