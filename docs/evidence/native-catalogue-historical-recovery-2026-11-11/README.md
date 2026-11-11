# Actual native catalogue adoption and historical replay: focused evidence

Exact private composition `9e37d812369de61bb9489ce5415d73d47fdcef35`, package
metadata 1.0.471-beta, passed **38 distinct focused Rust tests**. Public main base is
`b0e2498842be7b01e355bb8da1cc249ff16be056`. The private package version is not a
published release or binary identity. No broad suite or release CI is claimed.

| Group | Passed | What it exercised |
| --- | ---: | --- |
| Image catalogue adoption/finalization | 5 | Real nonempty fsynced RF1 image, native R1 ahead of raw R0, durable catalogue snapshot R1, preserved refusal, idempotent retry, acknowledged-persist ambiguity/re-fsync, close/reopen exact reissue, foreign-freeze rejection |
| Historical native Apply | 2 | Three actual fsynced data services, real role/fence/history/marker decisions, current certificate quorum without data-leader certifier; old-WAL cut/reopen/fresh-local-WAL recovery with original command identity |
| Nonzero completed crash window | 1 | Actual source and target reopen, full history/data re-audit and same already-published R1 anchor reissue |
| Prior interrupted image | 1 | Durable refusal/new attempt/late-chunk exclusion |
| Legacy catalogue compatibility | 2 | v1 decision migration and v2 omitted-field/checksum behavior |
| Local physical readiness | 8 | Actual local data/history/MVCC/open/identity/pin scenarios |
| Ordinary native origin/prefix | 7 | Original local WAL/commit callbacks, pending/Unknown and history controls |
| Current source/fence quorum | 7 | Actual current voter fences, unknown floor, initial genesis source/capture and poisoned verification |
| Recovery state | 5 | Durable session, persistence ambiguity/re-sync, exact range scope and unrelated-range stability |

## New guarantees exercised, and their limits

Catalogue format 6 records the actual physical image identity, R/H, birth and
attempt/scope. Only an owned actual complete verified image can establish this
snapshot: last/resolved/compacted become its actual R without fabricating individual
Applied command records. Image intent, recovery fences and permanent promises are
preserved. The receipt is issued only after complete catalogue fsync; uncertainty
retains refusal, and a new store-open needs fresh actual proof. The actual native
base, DB-open admission and intent fence stay sealed through finalization. A raw
receipt, caller number or foreign freeze grants no rekey or serving authority.
No new reader feature is inferred from the map; older unsupported catalogue
formats must refuse. Omitted empty maps preserve prior catalogue checksums.

The first historical test materializes a real replica from actual historical
commit evidence and a distinct current-session certificate majority, intentionally
excluding the current data leader from that certifier set. The test uses real
fsynced services and metadata quorum providers through registered in-process
identity transport, not supplied ACK counts or fake certificate voters. It does
not exercise authenticated remote data RPCs or automatic metadata failover.

The second historical test fails after an ACTUAL local WAL receipt but before
native materialized/prefix publication. It verifies the row is already physically
present, performs a real checkpoint that cuts the old transaction WAL, closes all
DB/service/store owners, strictly reopens and supplies an explicit new current
metadata epoch/leader precondition. Recovery must preserve the original command
checksum/index/leader/epoch while archiving the old local pending attempt and
producing a NEW actual local transaction/WAL receipt. Matching rows alone cannot
ACK replay. The new epoch/leader is a declared test precondition, not an election
or failover measurement. The prior source remains available to the fixture; this
is not a VM/process-loss campaign.

Active recovery still refuses ordinary NEW writes afterward. Tests do not clear
fences to manufacture availability. Native ReadyQQ, actual RecoveryComplete,
serving configuration/ancestry, actual ordinary remote replica Apply, full
audit/sync/broker effects, StrongGet, fault-safe online relocation and physical
four-to-five-node qualification remain open. The proof remains rows/native-prefix
only; no index/FTS/queue/audit/multi-range SQL parity follows from it.

## Retained failure and reproducibility

At exact initial source `20f803764f51febf919014e7a758eaf7c3f33325`, four catalogue
scenarios passed, then BOTH historical tests failed before native Apply: the
fixture requested `budget_ms: 30000` while the actual RPC maximum was 5000.
Remaining jobs were not run. This was a fixture error; production limits were not
relaxed. The correction changes three fixture RPC budgets to 5000, retains the
30-second overall test parent, and does not renew an original authority deadline.
A preliminary commentary estimate of eight seconds was incorrect; archived source
and failures establish thirty seconds. Both attempts and all result/resource logs
are retained, with classifications separate.

The corrected first group took 87.022 seconds including forced compilation from
its actual intended worktree and five test executions. Related controls reused
only that unchanged compiled source. Minimum available RAM was 22,993,879,040
bytes, with no resource pauses or interruption. Core optimization/debug are zero
with release dependencies; durations are not production performance/RTO numbers.
The shared test executable is mutable and was not separately archived for this
bundle. Tests used actual local fsynced server_paged files and close/reopen, no
independent machines, power loss, VM resets or Azure allocation.

`engine-from-main471.patch.gz` is the complete engine diff against the named public
main base. `initial-failure-from-corrected.patch.gz` reconstructs the earlier
failed source from that corrected engine, including its actual 30000 ms fixture
requests. `source-inputs.json` hashes every changed engine source, retained as
compressed exact snapshots. Apply the patch to that pinned public source and use
its unchanged locked dependencies. `run.py` files record exact commands, source,
patch hash and resource guards; paths can be adapted to the reconstructed checkout.
The manifest checks retained bytes; it does not rerun Rust or promote the result
to operational qualification.

The engine patch was independently applied to a temporary Git index at the named
public main base; all 59 reconstructed source hashes matched the exact candidate.
The workspace index and tracked source were not changed by that check.
