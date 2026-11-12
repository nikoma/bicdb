# Concurrent opening fault controls: failed before fault cuts

Classification: **failed bounded opening; intended fault-cut guarantees not tested**.
Source `61eb4d92b5938349cf18a283e936eee45fa97e9d`,
[run 43225435854](https://github.com/nikoma/bicdb/actions/runs/43225435854),
Core job 131207285182: zero passed, three failed, 886 filtered, 38.19 seconds.
All-target and isolated-consumer compilation passed.

All three concurrent fixtures completed the scoped publications on n1/n2/n3
and reconciled the actual lost n1 reply with its unchanged stored audit.
Opening exhausted each original four-second phase before the requested cuts.
The step- and original-caller-cancellation tests reported `fsync_cut=false` on
all three bounded attempts, then failed `must reach actual successful catalogue
fsync`. The foreign-writer test reported `actual_install=false` on every
attempt and failed its required installed/rejected assertion. No cancellation
or reenrollment invariant is proven by these refusals.

The step-cancellation trace shows collection leaving 2,333 ms, another complete
native sweep at round closure leaving 1,022 ms, and local post-quorum MQ leaving
946 ms. The final foreign sweep expired before the local native journal/fsync.
This distinguishes validation-budget exhaustion from a measured disk stall;
it does not identify the original Azure mechanism or physical RTO.

The next smallest correction removes only the repeated round-closure native
sweep after collection. It preserves original caller, every participant's
current MQ/writer lifetime, the distinct local post-quorum MQ, complete final
foreign sweep and local journal-backed proof. Source correction `674549a4c`
has independent source-only review; its doc-only successor `e8d263bac` is under
[the same three-test filter at run 43227153532](https://github.com/nikoma/bicdb/actions/runs/43227153532).
No timeout increase or test serialization is used. A pass must reach each
actual fsync/cancel or writer-reenrollment cut.

## Evidence custody

Raw complete Core job-log SHA-256:
`39cc01a924073e23438497af2e30dcc82820ac36044e6ddde786ae53b79fb3f0`.
Original GitHub artifact 13948123238 is retained unchanged (695,402 bytes),
SHA-256 `62c56fe36a526691a8d7c8015b9a267b80656fd9dd4bdc49c79263837621e051`.
The companion receipt verifies all 75 manifest-listed files in each of three
observed cuts (225 files, 12,773,040 listed bytes total), including the twelve
zero-byte `.backup-write.lock` files previously omitted in an older artifact.
There are no missing/extra files or hash/length mismatches within those cuts.

These copies were made with database/service/metadata/native-journal locks
held and engine owners still alive. They are observed stable file custody,
not stopped-owner images, atomic power-loss snapshots or independent engine
replays. Original private TLS directories were excluded. The retained archive
also contains generated test database key material; it remains in restricted
experimental custody rather than being republished here.
