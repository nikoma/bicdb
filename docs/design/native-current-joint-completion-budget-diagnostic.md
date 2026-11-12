# Current-joint completion budget diagnostic

Executed source d6331d1d611f4f31ccf960fa13b0897d02bfb92a, run 43253229007,
Core job 131304443885 compiled and failed the positive recovery acceptance in
61.11s at the first Complete RPC's original 4500ms deadline. The same-source
`current_joint_` controls separately passed 4/0 (48.83s); that is not the full
positive acceptance.

The positive run completed all three genuine CurrentJoint fences, promises,
canonical Abort3/native R3 assertions and native preparations. Read-only ZIP
inspection of artifact 13961016835 shows n1's catalogue **NativeCompleted R3**,
with serving checksum
`4443fc6cebf1b617d84d744bc4efea9f257a83819b5cf7d2ad512074fa0deb2f`;
n2 and n3 remain NativePrepared R3. Durable native/catalogue completion was
observed by the later custody capture, and its response is uncertain. The worker may have continued after the
client timed out while custody waited on service/DB locks; this capture alone
does not establish fsync-before-client-timeout ordering. This is not evidence
that no Complete or no NoData occurred. No Activate/ACK4/Strong R4 acceptance was reached.

Retained ZIP SHA-256:
`c0ad6a769710cb715b2c96df238605ae57d0008a68d32448b319d37779f8958d`
(393435 bytes). Its failure custody was observed stable with live engine owners;
JSON inspection is not an independent cold engine replay.

The successor adds only cfg(test) diagnostic spans. They observe the original
request/challenge/session/deadline and actual protocol selection, worker drain,
Ready QQ, post-QQ metadata authority, service/database acquisition, native
preflight/journal/WAL/readback, catalogue fsync, prepared-binding refresh,
receipt validation and response projection. RAII spans report incomplete early
returns, real remaining budgets and cancellation. Native/catalogue publication
markers describe actual returned storage operations and their actual R/H/B/SHA;
they do not mint authority or reclassify a timeout as a successful RPC.

All validation, network/lock order, foreign/local checks, original caller,
quorum tokens and 4500ms budget remain unchanged. No additional network,
journal acquisition or metadata/service lock is introduced. The next exact
focused run must locate the first exhausted boundary before any optimization.
Full recovery remains unqualified until actual Activate/ACK4/Strong succeeds.

## Diagnostic attempt and fixture ordering successor

The instrumented source a0ed97c26803a5ac9523f621c6fa94c41d2de20c, run
43255635176 / Core 131312976958, failed earlier at fence `round.record` after
three successful fence RPCs (64.37s whole test). It collected no Complete phase
timings. The fixture had spent the original five-second collector budget on
full native/row exports, archive assertions and fsynced diagnostic reports
before counting each authentic response. This does not measure producer-only
fence collection or qualify completion.

The test-only ordering successor captures the immutable BEFORE state before
starting that operation, counts each real challenged reply immediately, and
finishes/requires live the actual full quorum under the SAME original token
before doing deferred heavy verification. It retains every original native,
raw, archive, UID, row and custody assertion. Actual Runtime Frozen owners remain
held throughout verification. RPC/record/finish traces show actual remaining
original budget; a producer/collector that still exceeds it continues to fail.
Failures retain exact partial replies and snapshots before acceptance fails.
No token is renewed, deadline extended, authority fabricated or guard removed.

## Executed 99 collector and first Prepare boundary

Source 99ddcf4834997156b57f79707e91344c126f3ae3, run 43261048742 / Core
131332124296 compiled successfully (all targets and isolated consumer also
passed) but positive runtime failed 0/1, 908 filtered, in 80.85s. The corrected
actual fence collection completed within the SAME five-second parent:

| Receiver | Real RPC duration | Original budget remaining at immediate record |
|---|---:|---:|
| n1 | 1048ms | 3672ms |
| n2 | 1059ms | 2612ms |
| n3 | 884ms | 1727ms |

Actual quorum finish/require-live succeeded with 1727ms remaining. All three
promises and the assertions for canonical Abort3/native R3 completed. The
first n1 `prepare_native_ready_observation` then hit the unchanged 4500ms RPC
deadline at source line 700. No Complete request or completion timing span was
reached. Custody later observed n1 NativePrepared R3/B4 and n2/n3 still Active.
This proves a preparation effect by capture, not fsync-before-client-timeout or
a successfully returned Ready observation. No Activate/ACK4/Strong R4 followed.

Raw log SHA-256 is
`f0202fae0e16a7ca49ffee2fae741a192c2fcd6a7b28d22dcfaaad1773e3f3c3`.
Artifact 13964830796 ZIP SHA-256 is
`196a2730d1e2a1878b337bb6268d02d673f0725064cd0c58e63ce61426ec99e2`
(391362 bytes). Manifest file lengths/checksums were independently verified
without engine open; capture remains observed stable with live engine owners,
not atomic power-loss replay.

The next source adds cfg(test)-only Prepare spans at the actual metadata role,
service acquisition/preflight, owned freeze, local DB/native admission,
resolved catalogue/refsync, physical producer, catalogue producer/fsync,
post-persist current validation, response projection and actual-session
retention. Markers use existing actual original deadlines, point and saved
session. The 4500ms/5s budgets, foreign/local checks, cancellation, storage
operations and every acceptance assertion remain unchanged. This is a profile,
not an unmeasured optimization or a retry that revives the original request.
