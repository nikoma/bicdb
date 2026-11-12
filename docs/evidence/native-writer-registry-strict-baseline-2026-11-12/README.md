# Strict metadata-addition DATA continuity baseline

Exact source `1d3976f9f8011f3ac7411494aa0dfb432763ce78`, full native prototype,
source package 1.0.489-beta. This is distinct from the public read-delivery
runtime sharing that version. [Run 43282887768](https://github.com/nikoma/bicdb/actions/runs/43282887768)
executed all six CLI cases: **5 PASS / 1 FAIL**, 84.04 seconds. Workspace
all-target compilation and the isolated no-default consumer passed. The new
strict case reached its final health assertion; it was not a setup failure.
The earlier five cases passed, including their original receiver assertions.

Four real initial processes first produced an external fsynced full-value ACK
and exact Strong read. One fifth process used the single public `join-fresh
--replica-receiver` command. It became a same-owner metadata voter/control/
unassigned receiver; DATA remained false. Actual before/after configurations
retain every DATA range/epoch/leader/replica/placement and the original births.
All four retained coordinator IDs, registration generations, DB-open IDs,
Store-open IDs and persisted policy checksums stayed equal. Their weak/atomic
binding diagnostics stayed live; `writer_policy_current` changed true→false.
These diagnostic snapshots do not lock the current DB hook or prove Ready.
The new metadata voter n4 had `labels={}`.

The SAME public gateway then attempted both an old-ACK Strong read and a fresh
Append in each of two rounds. Each call retained its original 2s request
budget. The explicit 10s gate bounds round admission; this frozen source lacks
a final monotonic receipt-time success check and does **not** establish a hard
10s wall-clock guarantee. That test-only correction is a separate successor.
Neither operation succeeded; no successful false absence/stale value occurred.

| Actual operation | Captured pre-dispatch request/complete reply frames | Actual scope and result |
| --- | --- | --- |
| Old ACK Strong, round 1 | 8 requests / 7 complete replies | n0, range4/E1; seven typed `WriterAdmission` unavailable, generation4/leader n0; final attempt interrupted at request budget |
| Fresh Append, round 1 | 5 / 5 | n2, range2/E1; all `cluster_data_operation_failed`, `current voter registry changed` |
| Old ACK Strong, round 2 | 8 / 7 | Same actual scope/refusal; final frame canceled before any bytes were written |
| Fresh Append, round 2 | 5 / 5 | Same actual scope/full immutable payload/refusal |

There are **26 pre-dispatch frame representations**, not 26 proven sends.
Twenty-five attempts report positive bytes-written. Read round1's final attempt
reports 404 bytes then interruption; round2's final attempt reports zero bytes
and no possible dispatch. Its captured frame is not counted as an actual send.
The 24 complete refusal replies independently establish real post-join requests.

Read business IDs are `01a1d122-811b-7761-bd0c-a4b5734c071a` and
`01a1d122-9046-78d0-8784-31077d5ea209`; every attempt has its original fresh
challenge in the journal. Legacy `ApplicationEvent` carries numeric request
IDs and range/epoch, **no business UUID or topology-generation field**. Its
attempt/request IDs and logical linkage are retained exactly; no ID is
invented. Across these four logical calls, **109** metadata snapshot exchanges
succeeded (34+20+35+20), rather than all nodes being offline. Typed read replies
carry generation4. Write routing scope is verified against the actual observed
configuration, not a nonexistent write generation field.

The two fresh API attempts used the same key/full value. Both original primary
ledger records remain `NO_USER_ACK_OUTCOME_UNKNOWN`. Repeated registry error
text does not prove whether it came from a pre/post persistence boundary;
missing ACK is not rollback. All five SAME child PIDs were actually queried
and still running after the health gate, with no restart. Only afterward did
test failure cleanup kill/wait all five and perform the opted-in bounded copy.

## Retained custody and offline classification

Both downloaded artifacts match GitHub's exact ZIP size/digest in receipt.json.
The stopped-copy manifest is
`919990aa4020154427e271788478811e79a6e7fa0ac581bf69dfd75c14840234`.
It records all five actual SIGKILL/wait successes before copy, **96 regular
files / 2,275,560 logical bytes**, and 40 directories. Every file's size/hash
was independently verified offline. Artifact ZIPs omit empty directory entries;
private extraction recreates those directories from the manifest and records
no claim that the ZIP preserves inode identity. Copies were fsynced privately;
no original DB fsync, repair, engine open or WAL replay was performed.

The stopped DBs belong **only to the failing strict fixture**. Its one prior
ACK maps to the identical raw range4/E1/index1 command and full payload in the
n0/n1/n2 catalogues, each labeled `applied`; n3/n4 have no raw range entries.
Decoded retained raw catalogues contain no fresh attempted key. This is an
inventory observation, **not** a native page/WAL replay, native absence proof,
canonical Abort, ACK reconciliation or power-loss guarantee. The uncertain
fresh write classifications remain unchanged. The other four ACKs in the
cohort belong to earlier passing fixtures; their receipts/journals are retained,
but their DB roots were removed and are not reconciled against this snapshot.

The three complete journals contain 524/208/587 rows, 1,319 total. Logical
intents/replies, observer/sink sequences and exact length-prefixed frames are
verified. Explicit Bootstrap setup calls remain logical-only with
`bootstrap_raw_frame_covered=false`; this is not a global wire ledger.
The 62 emitted primary records were fsynced before stdout. No private TLS or
native signing-key bytes are included in this portable directory. Full private
DB/WAL files remain restricted in the ZIP custody location.

Run `python3 verify.py --repo /root/bicdb` and `python3 test_verify.py` here.
Optional `--private-dir` names the restricted
`native-writer-registry-baseline-1d3976f9` directory and verifies both ZIPs plus
all stopped file hashes. The verifier checks byte/source consistency and
semantic claims; it cannot authenticate execution independently of the retained
CI source/logs. No engine is opened. Source leaf verification is not a complete
source reconstruction or rerun.

The [separate refresh contract](../../design/native-retained-writer-registry-refresh-contract.md)
requires genuine schema publication and retained-writer audit refresh. No
repair is included in this baseline. Earlier ca7 prerequisite success and 3ee
null-write uncertainty remain unchanged; this does not explain the older 3ee
failure or an Azure incident. DATA relocation/controller/joint wire, physical
five-node qualification, SQL and full Milestone 1 remain open.
