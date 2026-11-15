# Native metadata supervisor: same-round store admission

PR203 source `7b965c6854843b7a127a5575214deddee2bc2c36` failed the
closed-ingress/same-worker test after stopping n2's actual incoming listener.
The final n0 leader label was not an anchor: term 3, commit index 0 and last
log index 2/term 3. n1 was a term-3 follower at committed/log index 1/term 1.
The original stderr records insufficient election majorities, paused ingress
and tick-store-busy errors. It records neither the original lock holder nor
per-peer append outcomes. CPU/IO attribution and the original failure cause
remain inconclusive. The unchanged test and supervisor sources passed PR201;
that earlier result does not validate this revision.

An instantaneous mutex probe can repeatedly overlap a brief actual snapshot
reader and abort every tick before replication, even when the store is free
for most of each heartbeat interval. The supervisor now retains that fast
probe and waits only within the SAME original round deadline on contention.
All seven acquisition sites are outside service, DB, journal, version and
other MStore guards. The original parent is checked after acquisition before
using the unchanged owner/current-term/election/append predicates. Missing
mutex admission still returns the existing busy error. No new RPC retry,
400 ms round extension, election/heartbeat change or quorum shortcut is added.

The new controlled-reader test holds a real MStore guard, completes the real
native supervisor snapshot and then observes the SAME worker's actual initial
`try_lock` failure before releasing the reader. This is an actual overlap, not
a fabricated busy result. Its first two real worker rounds respectively
exercise cancellation and natural expiry of the ORIGINAL 400 ms token; both
old tokens remain dead. Later normal ticks must elect a real anchored voting
leader while readers remain active, through the SAME existing 12-second wait.
The old source would abort a tick at the observed failed fast probe; no old
binary was executed as a comparison. Original test bodies are unchanged.

TEST-ONLY passive facts record bounded stage/peer/term/index headers, actual
transport returns/errors and the original `record_append_response` committed
Boolean. Transport success does not mean a counted ACK or committed quorum.
Each worker retains at most 256 facts, a 512-byte UTF8 display prefix plus
streaming full display SHA, and a bounded peer label. Omissions and unavailable
snapshots are explicit; no complete global stream is claimed. No Error, ACK,
store or authority owner is retained by that passive buffer. A SEPARATE armed
test synchronization hook queues the real original CancellationToken into
the controlled reader, with no renewal; this is not a passive-fact claim.
Diagnostics are emitted outside native/service/MStore scopes, including on
original assertion failure. Reader synchronization is removed before scoped
thread joins, including panic unwinding.

Validation at freeze is standalone formatting, source-preservation and diff
checks only. No Cargo build, Rust test, CI, physical fault, RPO/RTO, source
version reservation, push or merge result is asserted. Normal required CI
must establish compilation and actual regression outcomes before delivery.
