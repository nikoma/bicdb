# Certificate quorum before an acknowledged range write

This source candidate repairs the narrow acknowledgement gap demonstrated by the
[unmodified463 RF3 cut](../range-certificate-ack-cut-2026-11-11/README.md).
It requires durable certificate replies from a voter majority before the
originating transaction reports success. This is local source/test evidence,
not a physical failure campaign or production qualification.

The coordinator still crosses the ordinary local transaction WAL boundary after
its decision becomes irrevocable. If certificate replies are insufficient, the
post-WAL callback returns `CommitOutcomeUnknown` with the actual database
incarnation, local sequence, transaction and range command identity. The local
receipt remains **Committed** because local application is durable. That
receipt/local outcome resolver is not proof that a cluster ACK was safe.

The callback invalidates the recovered epoch and releases its completed local
reservation so background recovery can repair the certified history. Later
admissions, including writers already waiting on that reservation, refuse until
readiness is re-established. Lost replies do not cause a provisional abort or a
claim that the command rolled back. A certificate checkpoint failure similarly
returns uncertainty after local WAL. Callback handoff state is constant-size per
active range; durable recovery evidence remains in the replica catalogs.

Matching event Append retries also cross admission again. A current local row
alone no longer produces `inserted:false`. The retry reserves the current row
using the normal read-committed mutation API and carries its exact observed
version, so an unrelated contiguous visibility gap does not authorize a blind
upsert or require a fabricated global sequence floor. Different payloads still
fail. Reopening a process does not bypass the recovered-epoch check.

The immutable event key/payload remains idempotent. **A successful matching retry
now consumes another range command and local WAL transaction.** Ordinary native
mutation/audit/change-capture effects may occur again. This change makes no
exactly-once audit, outbox, queue or business side-effect guarantee.

| Validation | Result and scope |
| --- | --- |
| Fsync-on three-database integration selection | 5 passed on prior candidate hashes |
| Distribution core unit selection | 127 passed on same prior candidate batch |
| Final constant-size callback handoff refactor | Awaiting parent integrated source check |
| Physical failover, reconfiguration and production qualification | Not tested here |

The integration cases cover unknown local receipt plus successful background
recovery/continued writes; an acknowledged certified quorum followed by only old
leader loss and successful surviving-leader reads/writes; a durable certificate
with lost reply; identical event retry after uncertainty; and close/reopen with
local resolver Committed but no retry ACK before recovery. Recovery establishes
the leader's applied state; the next admitted write repairs lagging followers.
It does not promise that every follower is caught up as soon as the leader's
recovery method returns.

The unit cases additionally check readiness invalidation while a writer waits,
and a current event row above the contiguous applied watermark. The latter uses
a test-double authority to isolate row-version correctness; it is not a quorum
or HA test. Existing distribution tests retain stale fencing, ambiguous-tail
refusal, repair, membership, schema and transport coverage. Permission, TLS,
transport wire formats, fsync and replication quorum requirements are unchanged.

The recorded commands used one Cargo job, incremental disabled, core opt0/debug0
and retained dependency artifacts, with an active12GiB available-memory guard.
Focused5 took39.02s including linking (7.02s test time; minimum26.27GB available);
broad127 took63.53s including compilation (5.28s test time; minimum24.04GB
available). Neither monitor paused the build. These are development checks, not
optimized-runtime performance measurements. The final refactor replaced a full
command clone with four constant-size fields; receipts deliberately preserve the
previous hashes and label final-head validation pending.

Earlier retained exploratory results are explicit: the first callback-gate
candidate exposed both matching/reopened retry ACK bypasses; two additional
assertions incorrectly demanded immediate follower repair and were corrected to
the actual recovery contract. A later candidate fixed the bypasses but inherited
a500ms test socket limit from a fsync-off fixture; healthy retry positives timed
out. The fixture's limit was increased to10s with fsync still enabled, after
which all five cases passed. Those failures remain adjacent and are not hidden
or counted as passing. No RTO bound follows from this timeout adjustment.

Scope remains **stable range membership**. This patch does not prove relocation
membership transitions or stale partition writer fencing. It does not fix
failures occurring after irrevocable admission but before the local WAL/receipt
boundary, nor establish snapshot/history/namespace/reader-capability image
readiness. These remain separate P0 obligations before a strong point-read or
complete acknowledged-write HA claim. It does not establish Azure causation.

[summary.json](summary.json) binds final source hashes and the narrower validation
scope. The command/result receipts bind the tested prior hashes;
[export-provenance.json](export-provenance.json) distinguishes original local
files from portable exports. Only task build paths are replaced. Original files
remain in `/tmp/bicdb-p0-range-ack-evidence`.
