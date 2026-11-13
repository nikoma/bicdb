# Registered pending-row evidence across process loss

Source-only preservation batch, based on
`d6908095e0d347e563ef68801f49e52dba35452d`. No Cargo build, hosted CI or
runtime test has run on this batch. Earlier passing d690 same-open controls
remain evidence for d690; they do not validate these changed paths.

An actual Registered row intent now inserts a restrictive pending-index entry
in the SAME native page transaction as that intent, before logical row WAL.
The entry binds original database open, native birth checksum, range, original
attempt, source/history/intent/step identities, logical TxID, exact mutation hash
and actual intent page XID. These are DATA, with no recovery/mutation/Serving
authority. Committed exact readback is required before logical WAL proceeds.
The matching entry is deleted in the SAME page transaction as the protected
RowsHeader. Committed header readback, absence of that exact entry and all
existing context postchecks still precede removal of the live pending owner.
Unknown, pending-apply and unpublished progress retain the original debt.

The complete scan bounds admission to 4,096 entries, 16 KiB per entry and
16 MiB total encoded entries. Count overflow, malformed bytes, unknown fields,
wrong identities, uncommitted intent status, missing original intent or changed
birth fail closed. A scan error is never interpreted as an empty index.
Admission scan and publication run under the actual page write guard. The
original actual WAL pin is acquired before intent publication and remains
owned through protected progress. The common cut guard prevents acquisition of
new pins while checking the index and rewriting the log; there is no empty-scan
then unpinned-insert window. No new global scheduler or database mutex is added.

After actual native page-WAL recovery, normal startup checks this index BEFORE
the generic logical-WAL scanner, row materialization, incarnation append,
broker/audit/CDC replay and paged-open logical-log reset. Live common cuts and
the recovery verifier's full-file rewrite also consult it under the actual
native cut guard. Page recovery itself can update native files; refused open is
not a claim that every database file remains byte-identical.

The new sticky local reader fence is persisted atomically with the first
Registered-image fence, before indexed row intents. Historical image-only
Registered prototypes are explicitly unsupported: normal open and attempted
activation refuse them rather than silently assuming their index is empty.
Older readers must reject the new unknown fence. No migration/adoption of old
unindexed attempts is implemented, and no portable SQL schema flag is added.

The proposed Unix process-loss control uses the existing actual
`retain_applied_known` callback, writes and fsyncs an external phase marker,
then parks. Its parent sends real SIGKILL and waits for process termination;
there is no child Drop/close. The parent retains exact logical-WAL bytes and
reads the actual recovered native index/intent. Fresh normal paged open must
refuse with the original attempt/Tx diagnosis while logical WAL, sync log,
event segment and incarnation journal remain exactly unchanged. The marker is
a phase observation, not a client acknowledgment. Existing cancellation
controls additionally check actual indexed intent before retry and an empty
index after committed protected progress, without weakening old assertions or
caller deadlines. Storage-only controls cover aborted, malformed and oversized
restrictive index DATA, plus refusal to upgrade an unindexed prototype. These
new controls are unexecuted at this source freeze.

This stage preserves evidence by refusal. It does not self-heal cold debt.
A separate restricted CLOSED cold-repair producer must resolve the exact
original logical TxID/incarnation/mutation hash/sequence from retained complete
WAL, retain all original attempt/history constraints and reacquire actual
current open/Slot/OWN/native authority before publication. It must not invent
a new transaction or treat visible rows, scalar B, cached completion or a
serialized pending entry as proof. Generic replay does not become authorized
merely because an outcome can be inspected. Ready, Serving, physical HA,
bounded fault-safe rebalance, sustained scale-out and production qualification
remain outside this preservation stage.

The new logical JSON `registered-row-cold-preservation.json` and phase/pre-cut
markers can be considered for an explicit public evidence whitelist after
inspection. Raw WAL bytes, full fixture databases, seed/signing/TLS keys and
other custody remain private unless separately reviewed. Nothing is added to
historical failed-run archives or frozen documentation evidence packets.
