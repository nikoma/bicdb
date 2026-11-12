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

## Executed 2a7: preparation returns; completion refresh exhausts budget

Exact combined source 2a7a587fcfd392e851dc42eccc8762837b0c7fb6 (489) has three
separate immutable runs, all with successful all-target and isolated-consumer
checks:

| Run / Core job | Scope | Actual result |
|---|---|---|
| 43265974939 / 131349595566 | RF1 produced Ready controls | 2 passed, 0 failed, 917 filtered, 0.15s |
| 43265975524 / 131349582063 | CurrentJoint authority controls | 4 passed, 0 failed, 915 filtered, 47.80s |
| 43265975417 / 131349588082 | Full post-joint recovery positive | 0 passed, 1 failed, 918 filtered, 58.52s |

All three Ready responses returned successfully in the positive run: n1
2053.4ms, n2 2072.9ms, n3 1646.2ms. Immediate produced projections took
80/68/84 microseconds. These are observations of this run, not a comparative
capacity guarantee; runner speed also differs from 6ce. Complete first ran on
n1 and produced the following exact profile under its original 3999ms receiver
budget (the request remains 4500ms / caller five seconds):

| Actual n1 Complete phase | Duration | Original budget remaining |
|---|---:|---:|
| Protocol selection; worker drain | 893.4ms / 5.1ms | 3100ms |
| Actual closed Ready quorum | 543.8ms | 2556ms |
| Post-quorum fresh metadata role | 8.8ms | 2547ms |
| Service and DB acquisition | 2µs / 34µs | 2547ms |
| Final local quorum/physical check | 273.5ms | 2273ms |
| Native producer preflight | 762.0ms | 1511ms |
| Native WAL publication return | 111.4ms | 1400ms |
| Native readback/final current | 404.7ms | 995ms |
| Before-catalogue local quorum check | 292.6ms | 702ms |
| Catalogue fsync return | 2.8ms | 699ms |
| Post-catalogue full domain/physical/native refresh | 556.2ms | 143ms |
| Updated-binding repeated full current validation | 257.5ms | expired |

The full call took 4113.2ms and failed at source line 714. Native WAL and actual
NativeCompleted R3/B4 catalogue publication returned BEFORE the original
deadline; their serving checksum was
`d7d01434a032c9a80f637ece52ac8079c8d2d3e86cd417a6106fbbceb58a25b7`.
The completion response remains uncertain. No Activate, ACK4 or Strong R4 was
reached. Receipt-final-current and response-observation repeats are visible in
source but were not reached or timed in this failing run.

All raw files and indexes are retained privately under the foundations CI
capture directory. Their SHA-256 values are:

- RF1 raw: `5d332b41d1fec4a77e1afc81cb819f7d8e3cb4fb215af902f8384b1518e527c8`.
- Authority raw: `12688c1bf354216ec09b71286afe8a6a91a80fc8ed7a65391a4152a3a1e647cd`.
- Positive raw: `9ae0d1d40cefe16208e888e0a2bdf854250a2d7f38ebb52568ffb1324ff09615`.
- Positive artifact 13966228076 ZIP:
  `e64b4aa6996040d1dda1590c831c7c2cf31f2e60a2c9626a8bbccef308a3572a`.
- Positive manifest:
  `0201a9a8c753004519ff94e616b996cb3a44ef399da969260ab521a000883a66`.

The ZIP receipt verifies all 82 files / 6058854 bytes without engine open. It is
observed-stable live-owner custody, not an independent power-loss replay.

### Coherent immediate completion continuation

The successor keeps the full actual native checks before/after native WAL,
pre-catalogue quorum/physical check, catalogue fsync and full post-catalogue
physical/domain/native-serving readback. It separates exact updated catalogue
hash/phase/point validation from another identical full traversal. The actual
native producer receipt must match that actual post-catalogue native readback.
This comparison is private to the uninterrupted producer, not a caller-supplied
checked flag or proof reconstructed from a checksum.

Immediate Closed/Completed response projection runs through a module-private
continuation accepting real opaque quorum decisions, under the SAME exclusive
Store and borrowed actual DB, with the SAME owned Frozen/read pin. Both existing
projection helpers remain private. Return boundaries check exact current
scope/phase/binding, actual native-open/health/Frozen/pin, original CurrentJoint
writer/local promise, role lifetime and original caller cancellation. The new
pin-only check is a necessary refusal, not history/current-readiness authority.

Later standalone refresh, ordinary observers, public receipt validation and
Activated-decision response handling retain full validation. The common actual
completion producer's post-catalogue refresh is shared mechanically across its
sealed decision variants; no independent historical adoption rule is relaxed.
Activated-decision reply projection has no new fast path. Storage, unknown-write
refsync, original deadlines, nonce/membership floors and irreversible histories
remain unchanged.

New local controls cover actual RF1 completion/projection while still frozen,
both catalogue persistence ambiguity cuts followed by exact real refsync, and
actual caller cancellation after fsync with no returned observation. These are
source-only until executed and do not replace genuine CurrentJoint original
writer/caller/ABA controls, full positive acceptance or final broad regression.
No runtime success or measured savings is claimed for this successor.
