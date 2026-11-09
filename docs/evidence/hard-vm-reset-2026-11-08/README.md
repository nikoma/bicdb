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
attempts, an acknowledged preseed and concurrent CREATE/DROP constraints.
The preseed grew the page file beyond the pool; the separate large-transaction
campaign below records actual eviction pressure.
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
| 1794203435 | 0af1b55e | server_paged, ordinary main | 2,252 | incomplete: startup failed after 12th writer kill with mismatched `vm_ddl_75_pkey`; generic DDL catalog crash boundary under investigation |

All incomplete campaigns retain `complete: false`. Their acknowledged
row/index/receipt checks passed up to the reported failures; this does not make
the campaigns successful. The transaction-ID defect was independently
reproduced on production main 8b1e1d47, not only the structural-cut prototype.
The corrected prototype passed the new source-pinned campaign at5d04f91f.
The explicit `server_paged` production-main control at0af1b55e failed:
11 writer-reset recoveries reconciled without row/index/receipt errors, then
the next restart refused an existing `vm_ddl_75_pkey` definition. The client
had acknowledged table creation and its row, attempted DROP, and received no
DROP acknowledgment. Exact JSON inspection confirms that the renamed `__bicdb_bin_*` collection
and its original `storage_name=vm_ddl_75` backing remain. An initial quick
name filter incorrectly classified that collection as missing; that inference
is retracted. The interrupted move remapped the index collection, while SQL
schema/index naming had not completed, causing startup primary-key backfill
to collide with the retained index. Recycle-bin behavior also applies to the PostgreSQL
profile used by this VM; this is a generic transactional DDL/catalog boundary,
not an Oracle-only or typed-catalog regression. The exact source and terminal
error are preserved. No success is inferred from earlier clean recoveries.
Experimental source is not promoted or deployed by these results.

A preliminary **ordinary-default** candidate at2c3503db separately passed the
[large-transaction campaign](large-transaction/README.md), including actual
eviction pressure, values larger than the pool, seven hard VM kills and
independent structural checks. Final integrated-source and postmerge
qualification remain separate.

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
128KiB WAL and32KiB segments. It seeds2,000+ acknowledged transactions before resets begin. The first
saved crash image's page file is4,775,936 bytes versus the configured2,097,152-byte
pool; that proves pagefile footprint, not the live working-set size or an
eviction counter. Acknowledged row count alone is not a pressure-byte proof. Host-fsynced ledgers retain2,472 intents,
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


Independent structural verification of all14 saved crash cuts also completed
clean (exit0) using verifier source26051caf. Each ext4 journal was replayed on
a separate disposable copy, then the paged directory was extracted and scanned
without opening the database engine. SHA256 of every extracted store file
remained unchanged by verification. Original crash-image sizes and mtimes
remained unchanged. Reports and exact verifier binary hash are retained under
`runs/campaign-1794195817/independent-verifier-all-cuts.json` and per-cut reports.
Page-file footprints grew from4,775,936 to5,947,392 bytes. These structural
checks complement the independent SQL ledgers; neither substitutes for all
logical index, application or power-loss tests.
