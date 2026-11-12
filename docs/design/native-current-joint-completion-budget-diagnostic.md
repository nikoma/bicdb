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

## Measured 6ce preparation and scoped producer projection

Exact source 6ce80ad321049a3052ec7feb57cf8d74e2a08f49, run 43263149425 /
Core 131339520155 compiled (all targets/isolated consumer passed), then the
positive test failed 0/1, 915 filtered, in 82.18s. All three actual fence records
and finish succeeded with 1648ms remaining. Promises and canonical Abort3/native
R3 assertions completed. First n1 Prepare was the only preparation attempted;
no Complete request was reached.

The original request still asked for 4500ms under a five-second parent. Its
receiver's existing transport cap yielded 3998ms remaining at Runtime entry.
The profile establishes actual ordering and remaining original budgets:

| Actual n1 phase | Duration | Original receiver budget remaining |
|---|---:|---:|
| Fresh metadata role | 16.9ms | 3981ms |
| Service latch acquisition (preflight/create) | 2µs / 0µs | 3981ms / 3979ms |
| Preflight; owned freeze | 1.9ms / 19µs | 3979ms |
| Local DB/native admission; resolved catalogue | 1.6ms / 1.5ms | 3975ms |
| Actual physical producer | 2385.0ms | 1590ms |
| Catalogue producer's full physical preflight | 924.6ms | 666ms |
| Actual catalogue fsync return | 4.0ms | 662ms |
| Full post-persist current validation | 580.1ms | 80ms |
| Response observation repeating full current validation | 443.7ms | expired |

Total was 4361.2ms. No service-latch stall was observed in these spans. The
NativePrepared R3/B4 catalogue persistence marker actually returned with 662ms
remaining, and full post-persist validation returned with 80ms. This is direct
phase-budget evidence of persistence before expiration, unlike d633/99's later
custody-only observations. The extra response check then exhausted the original
receiver budget; no successful Ready reply, Complete, Activate or ACK4 followed.

Retained raw log SHA-256:
`cfd4f030a6e69ab44288250c557285a7d95d2bd006114a94bdc0783ea26f8f8e`
(`6ce-current-joint-ready-budget-job131339520155.log.gz` plus index). Artifact
13964603688 ZIP SHA-256:
`7223e1a0f07a758f5c5a1bbd91cb47fdae37975aa83164ffad4b14c58a27a739`.
Its private receipt is
`6ce-current-joint-ready-budget-artifact13964603688-receipt.json` beside the ZIP.
All 82 manifest files / 6016569 bytes verified without engine open; the capture
is not an independent power-loss replay.

The successor adds one narrow producer-and-projection method. It consumes the
actual owned LocalNativeRecoveryReady, performs the SAME full physical checks
before and after durable preparation, then immediately projects that newly
produced Prepared under the SAME exclusive Store and borrowed actual DB. The
actual Frozen/read pin stays owned throughout. The projection keeps all existing
scope/challenge/role/point/obligation checks; final original-caller cancellation,
role lifetime and actual original CurrentJoint writer/session are checked again.
It accepts no stored Prepared argument, DTO, unchecked token or cached current
grant. Existing later observer/reattest paths keep their full validation.

Only the immediate same-call redundant response physical/domain traversal is
removed; no before/after producer validation, final foreign/local check,
original token, 4s receiver cap, 4500ms request or five-second caller is relaxed.
There is no new journal/network acquisition. New tests use actual RF1 native
history/roles to check projected point, retained mutation refusal and canceled/
foreign-scope rejection with byte-identical catalogue state. They are not RF3,
CurrentJoint ABA or full recovery qualification. The actual full positive and
original-writer/caller controls must execute again before any passing claim;
no headroom or runtime-success guarantee follows from the measured savings.
