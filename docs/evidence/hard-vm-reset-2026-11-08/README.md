# Hard VM reset evidence (qualification in progress)

This campaign kills only a task-owned QEMU/KVM guest, discarding its RAM,
then boots the same dedicated ext4 virtual disk and checks acknowledged SQL
transactions against host-fsynced ledgers outside the guest. It is a hard VM
reset, not physical device/host power loss. QEMU uses writeback caching with
guest flushes honored, 4GiB guest RAM and two virtual CPUs. The host and
production services are not reset. No production deployment is qualified.

`python3 reconcile.py` independently checks the retained external ledgers and
reports each run's source, persisted storage mode and outcomes. The guest
reconciliation also checks exact rows/payloads, atomic row+receipt updates,
indexed query results, duplicate-key rejection and the initial backup restore.
Later harness revisions add native SQL transaction IDs, host-fsynced commit
attempts, a >pool acknowledged preseed and concurrent CREATE/DROP constraints.
The later seeded runs retain their exact `runs/<run>/harness.py`. The first
v1 runs predate that capture; `harness/campaign-v1.py` is the retained v1
snapshot with a later disk-path override correction, not a byte-exact capture
of those early executions. Their portable outcomes/ledgers remain original.
Older v1 results do not cover newly added assertions.

| Run | Source | Persisted mode | Acknowledged | Outcome |
|---|---|---|---:|---|
| 1794188065 | 9ee0a5b3 | embedded_memory | 2,996 | 12 writer resets +2 repeated recoveries pass; **not a paged control** |
| 1794188812 | 8313120f | did not open | 0 | infrastructure failure: unavailable guest proxy applet |
| 1794188961 | 8313120f | server_paged, experimental | 474 | v1 12 writer resets +2 repeated recoveries pass; no native-ID/DDL/preseed coverage |
| 1794189323 | 8313120f | server_paged, experimental | 2,045 | incomplete: concurrent fresh table constraint creation XX000; source fix #111 |
| 1794191086 | 1a9e3d56 | server_paged, experimental | 2,089 | incomplete: acknowledged native SQL IDs reused after checkpoint/reopen; source fix #114 |
| 1794195817 | 5d04f91f | server_paged, experimental | 2,428 | complete: preseed, native IDs, 123 DDL attempts, 12 writer resets +2 repeated recoveries, initial backup restore; zero violations |

Both failing application runs retain `complete: false`. Their acknowledged
row/index/receipt checks passed up to the reported failures; this does not make
the campaigns successful. The transaction-ID defect was independently
reproduced on production main 8b1e1d47, not only the structural-cut prototype.
The corrected prototype passed the new source-pinned campaign at5d04f91f.
An explicit `server_paged` production-main control remains pending; the
experimental source is not promoted or deployed by this result.

Initial assumptions about the first CLI run's storage mode were wrong. Its
persisted `format.json` proves `embedded_memory`. Each retained format record
was obtained by replaying the filesystem journal on a separate disposable copy
of the initial backup, then reading with debugfs; original crash disks remain
untouched. The first oracle-server prototype attempt failed before SQL opened
because the guest BusyBox lacked tcpsvd. The checked-in static TCP proxy fixes
that infrastructure issue. Early proxy timing had Nagle enabled: these runs
are not a performance comparison.

Raw disk images and binaries remain in the task-owned s3 directory
`/root/bicdb-durability-s3/vm-reset/`. Raw images contain generated test identity
material and are deliberately excluded from the portable evidence. Portable
JSON ledgers contain only generated fixture rows and outcomes. Portable file SHA256s appear in `manifest.json`; source and server binary
hashes appear in successful/partially completed campaign results. Future
campaigns additionally capture allowlisted configuration and dependency hashes; no private signing
keys, external credentials or production data are included.


The qualified5d guest used optimized release code, a2MiB paged pool,
128KiB WAL and32KiB segments. Its2,000+ acknowledged preseed exceeds the
stress pool before resets begin. Host-fsynced ledgers retain2,472 intents,
2,448 COMMIT attempts and2,428 acknowledged transactions. Every acknowledged
row and receipt remains, unacknowledged outcomes are whole and drawn only from
attempted commits, indexed results reconcile, native transaction IDs do not
repeat, and acknowledged CREATE/INSERT/DROP outcomes agree after recovery.
The run includes123 attempted DDL tables. Unknown outcomes are reconciled;
they are not counted as lost acknowledged writes.

`qualified-artifact.json` records both the input release hash and the exact
exercised guest hash. Guest preparation runs GNU strip after copying, so these
byte hashes differ: release `f930850c…`, guest `0433dacf…`. The exact exercised
artifact is retained at
`/root/bicdb-durability-s3/skr/bin/oracle-server-structural-cuts-5d04f91f-vm-qualified`.
The same-source build's library and integration results are separate evidence;
this VM campaign does not test all SQL/app behavior or physical power loss.
