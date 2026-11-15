# Native preparation/publication: failed 1fc acceptance

This is a failed software acceptance run, not physical qualification. Source
`1fcfdc3adf7e38422173e072e6fec4ad56e48188` identifies the then-unmerged
1.0.515-beta candidate; actual CI checkout
`6856b17344a32d3f8e783f26e35e1c4f6b5ac727` has the same tree,
`9d24cdf8cac1d9db4da35227182c71ff72a1bb7e`.

[Required run 43642552502](https://github.com/nikoma/bicdb/actions/runs/43642552502)
failed. The [serial native job](https://github.com/nikoma/bicdb/actions/runs/43642552502/job/132593097140)
finished November 14, 2026 at 22:43:12 UTC: **330 passed, 16 failed, one
ignored, 941 filtered**, in 3,968.80 seconds. Core, SQL, PGwire, remaining
workspace, all-target compilation and static jobs passed; the separate
[stock Rails run](https://github.com/nikoma/bicdb/actions/runs/43642552431)
passed. These results neither close issue195 nor qualify this candidate.

| Requirement or observation | Outcome and scope |
| --- | --- |
| Complete native acceptance | **Failed**: 16 actual failing tests. |
| Four preparation cases | **Failed** at `hot_policy_tests.rs:756`, preparing n4, before collection-round creation. The original OWN window expired; the original five-second caller still had about 3.48 seconds. |
| Ten bridge-publication cases | **Failed** at `hot_policy_tests.rs:876`. Final foreign validation and local journal preflight completed, then the request expired before any bridge-WAL begin marker. The exact intervening check is **inconclusive**. |
| Remote decision positive | **Failed** at `remote_decision_tests.rs:102` with a cluster RPC timeout. Its actual native outcome is **inconclusive** because the large native diagnostic was omitted. |
| Remote postcommit-cancellation case | **Failed** at retry line210. Earlier actual-cut2, commit-returned and same-native/gate assertions passed. The numeric native xid is not retained in the available log. |
| Protected CLOSED archive deleted after scope | **Passed**, actual component test and source assertions; not an independently archived native DB. |
| Retained middle history deleted after scope | **Not tested**: its preparation prerequisite failed before the planned corruption. |
| Controlled writer after preparation collection | **Not tested**: its preparation prerequisite failed before the planned negative control. Five other collection controls passed. |
| Supervisor closed-ingress recovery and new snapshot-reader overlap | **Passed**, actual software cases. This does not establish native DATA restart or physical RTO. |
| RF3 foreign-MStore control | **Passed**, actual software case. |
| FIRST application ACK/full Strong after relocation | **Not tested** by the failed serving cases; their earlier prerequisites failed. |
| Physical four-to-five expansion, RPO/RTO and capacity gain | **Not tested** by this CI run. The original Azure qualification remains failed. |

The ten publication cases measured final foreign validation at 539,251–563,383
microseconds and journal preflight at 70,694–95,648 microseconds. The remaining
aggregate interval is not a measured CPU, disk, fsync or scheduler attribution.
GitHub failure-stdout timestamps are libtest replay times, not operation times.
No timeout proves rollback or durable-data loss. Absence of a WAL marker alone
does not prove that a different earlier operation had no effects.

The complete raw native log is 13,461,744 bytes, SHA256
`b251eae9489a8ff883c7ca230d9d3844d98067f2776881f25caef71dfaaadb30`;
gzip SHA256
`c44729ad76b344fc792ead0bbaf97c8536fffcce0e88e2340b26bbf56466ad43`.
The immutable capture contains 11 complete gzip logs and eight original ZIPs,
with digests, CRC checks, actual checkout and job identities, and no missing
listed downloads. Canonical manifest SHA256:
`171291deb451e527497e6ef852963acc55ff770776c13040fc4d3eb290047f09`;
complete-verification SHA256:
`2472d0e9be7eb377bd6acccc6585a25e6a59b29bb407eac2abe76cd23063eb80`.
Offline verification passed, including a wrong-manifest rejection control.

Those ZIPs contain the workflow's actual pressure/Rails artifacts, not a native
fault-state DB bundle. Three remote native diagnostic JSON files exceeded the
existing one-MiB logging bound (1,486,106; 1,486,288; 1,694,729 bytes) and were
explicitly omitted. Exact returned API errors were retained separately. Do not
invent the omitted xid, native state or cold-replay evidence. Native pressure
samples observed at least 10,306,424,832 bytes of available memory; they do not
identify the cause of the deadline failures.

The next deterministic acceptance remains the same full preparation, publication,
ALL-release, original application ACK and complete Strong contract, plus the
original corruption/cancellation controls. Preserve actual authority, all foreign
checks and the original request/OWN deadlines. Verify both planned corruption
cuts actually execute. A later optimized or instrumented source must retain its
own source, tree and complete results; it cannot rewrite this failure.
