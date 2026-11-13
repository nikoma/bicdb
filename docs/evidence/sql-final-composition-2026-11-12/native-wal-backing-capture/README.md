# Historical Native backing contracts and local WAL recovery

Exact implementation `ccde585361dcdbf65bd9da698a21ccba94941b09`, inherited
1.0.490-beta on the private combined branch. This is unmerged prerequisite work.
It neither authenticates a replica peer nor grants imported rename recovery.

Both retained-WAL exporters used the current catalog to describe earlier writes.
After a rename and name reuse, an original Standard write was exported with the
replacement TimeSeries backing's contract. Two unchanged positive assertions
reproduced that defect in batch and live exports. A real local-consumer retention
hold keeps that history across checkpoints without counting as any replica ACK.
The first fixtures correctly hit the mesh-rename restriction and the checkpoint
retention floor; those setup failures are retained separately.

An explicitly fenced Native commit now captures each touched born backing's
actual allocation UUID, namespace UUID and contract alongside the same durable
write decision. The first vector's planned dimension is included. Prepared row
WAL cannot cache stale allocation data. Legacy/unborn backings and earlier WAL
acquire no invented identity. Historical batch and live exports use those actual
commit contracts, even after rename, replacement, drop and restart. The separate
local `native_wal_backing_capture_v1` capability fences birth-aware older writers;
activation is explicit, future-only, durable and retryable. It is excluded from
portable semantic reader requirements.

The first exporter fix exposed another real failure: after restart, retained
writes could reappear in the newer same-name collection. Local recovery now
resolves captured writes through their exact birth/namespace pair before row
replay. It uses the surviving actual backing, rejects partially changed pairs,
and skips pairs whose backing was dropped. It does not fall back to names,
storage aliases, donor UUIDs or SQL OIDs. Legacy/unborn local WAL retains its
existing semantics and gains no authenticated portable provenance from this
change. A later corruption control showed that materialized paged row bodies
were discarded before identity validation. A compact deduplicated pair history
now preserves those checks without retaining materialized row bodies.

The final exact source passed 36 focused Core tests, zero failures and one
pre-existing ignored frozen-reader test, across six parent harnesses: fourteen
birth/WAL controls, six image controls, eight namespace controls, one legacy WAL
serialization check, four sparse WAL checks and three snapshot/schema reader
regressions. Core/SQL/wire all-target compilation and all twenty-nine inherited
package/version identities passed. Four actual write/sync EIO injections cover
both modes; no failed write escapes the acknowledged durable prefix. Uncertain
recovery allows only a complete row or absence. Four corrupted cold opens cover
changed birth vs changed namespace in both modes, followed by catalog restoration,
exact Native ACK custody and two stable opens. These in-process checks do not
replace independent process, EIO and KVM qualification.

Capture checks cover immutable allocation data, exact payload/vector/timestamp/
unknown metadata, first-vector dimension, same-name isolation and two reopens.
The opaque in-process capture is checksum-bound to the exported frame, discarded
by JSON serialization and unavailable on asynchronous sources. Changed payload,
source node, sequence, transaction, reader requirements or a stolen capture on a
different frame cannot retain that capture. It is deliberately not a stable
source-root attestation or a target import receipt.

All nine baseline/validation histories remain in their own folders. V1 and V2
advanced past exporter assertions and failed the real restart isolation check.
V3 passed 34 checks; V4 added four EIO cases and passed 35; V5's new corruption
control failed before the materialized-history fix; V6 passed all 36. The JSON
summary counts parent harness results only. Raw manifests validate original
bytes after gzip decompression; command lists and source revisions remain exact.

Still required: actual source-root identity scoped at backing allocation,
authenticated producers at BOTH protected Native image and legacy logical-import
boundaries, durable target/root/generation bindings and record provenance,
revocation/cursor handling, and the read-only SQL recovery resolver. The original
promoted undecided rename positive remains unchanged and open. Its last actual
failure at `68de99c6` is preserved in the sibling `native-collection-birth`
evidence; it was not rerun or claimed passing on this WAL source. Full combined
regression, independent fault/VM, original application/UI/restore, physical HA and
capacity, and approved performance qualification remain separate full-goal gates.
No production CLI, main delivery, release or deployment is implied. Experimental
production remains off and no new cloud spend was introduced.
