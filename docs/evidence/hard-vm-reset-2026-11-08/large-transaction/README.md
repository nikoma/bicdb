# Exact default candidate: final premerge qualification

Source `6d225c1940cf13ffbb7693f8ebc2917e5abaad82` passed the same large
transaction workload in campaign `1794204317301666890`. It preserved the
acknowledged 8,192-row batch and 4,259,840-byte value with a 2 MiB pool,
recording 14,845 evictions and zero unlogged steals. Uncertain generations 1
and 2 were absent; generation 3 committed wholly without a client response.
All 16,384 final rows, index results, receipt and blob agreed through repeated
recovery. The nonempty disk image restored the exact original batch and value.
All seven saved crash cuts independently verified complete and clean.

The release input SHA-256 is
`a9d2f903ffda39b15d90ad5cc45d8a42c48111506b46faf7e50ba33193268ee9`;
the actual additionally stripped guest SHA-256 is
`b5331b45dd94c76ea46453db2996736bf1311f91478b0076803d05f30b2ff09e`,
retained at `/root/bicdb-durability-s3/skr/bin/oracle-server-default-cuts-6d225c19-vm-qualified`.
The same exact binary also passed the separate full 14-reset campaign,
retaining 2,275 acknowledged transactions and exercising 84 DDL attempts.
Together, all 21 saved crash cuts independently verified clean. This is
premerge source qualification; merge, postmerge testing and deployment remain
separate. The control's interrupted generic recycle-bin move remains a separate
known defect; passing randomized DDL churn does not prove that boundary fixed.

The audit permits reuse of an absent, uncommitted transaction allocation:
this run allocated ID 20 for absent generation 2, then for generation 3.
It explicitly rejects reuse of any identity already proven committed at the
previous recovery. Committed identities 10 and 20 are distinct and stable.
The reusable harness checks this inline, and portable audit checks the same
chronology independently. This distinction preserves the actual durability
contract without inventing a per-BEGIN fsync guarantee.

# Preliminary candidate evidence

Preliminary ordinary-default source `2c3503db6b3933139e4a4ac819fd9bda519fcd12`
passed campaign `1794202882515055041`: seven hard QEMU kills, an acknowledged
8,192-row transaction with 3,145,728 bytes of row payload and a 4,259,840-byte
value, against a 2,097,152-byte pool. The transaction also wrote its receipt.
The live pool counters recorded 14,845 evictions and zero unlogged steals.
This is actual eviction evidence, separate from row counts or file footprints.

Kills occurred 5, 25 and 100 ms after the client sent subsequent large COMMITs.
Generation 1 was wholly absent; generations 2 and 3 were wholly committed despite
connection loss before acknowledgment. Reconciliation checked every row/value,
secondary-index results and cross-table receipt/blob atomicity. The final
24,576 rows survived two further recoveries. A retained nonempty raw disk image
restored the exact original 8,192 rows and large value. This is a raw-image
restore test, separate from product online-backup qualification.

The independent read-only verifier at source `26051caf` also found all seven
saved crash cuts complete and clean. Ext4 journal replay happened only on
disposable copies, then debugfs extracted each paged directory without opening
the database engine. Extracted file contents, sizes and mtimes remained
unchanged through verification; original crash-image sizes/mtimes also remained
unchanged. Page-file footprints ranged from 12,328,960 to 45,678,592 bytes.
The verifier is complementary to the SQL acknowledgment and atomicity checks.

The release build input SHA-256 is
`6d60eef97e46fb3f2c92029223c1ea7c1223cedf01c414d9e23f4d0831e9e2b0`.
VM preparation additionally strips the binary; the actual guest SHA-256 is
`f3ca97a03859178b8289aba3583ce9bc3aede6c0520578cf734c941e4bc33494`.
Those exact guest bytes are retained on s3 at
`/root/bicdb-durability-s3/skr/bin/oracle-server-default-cuts-2c3503db-large-vm-qualified`.
Build provenance is the default candidate's release manifest. The guest had
4 GiB RAM, two virtual CPUs, 128 KiB WAL trigger and 32 KiB segments, with
compression disabled by the ordinary default. Guest flushes were honored by
the QEMU writeback drive. This does not simulate physical power loss.

The first run, `1794202766372217291`, is preserved as **incomplete: harness
contract error**. It incorrectly forbade reuse even of transaction IDs allocated
by a transaction subsequently killed before any durable decision. Only
acknowledged/durably committed identities require retention; no per-BEGIN sync
or globally persistent allocation contract was promised. It stopped when an
absent, unacknowledged allocation was reused, with no acknowledged identity or
row loss. The corrected run preserves acknowledged identity uniqueness, and its
separate audit also checks the identities of uncertain transactions later
proven committed (10, 20 and 30, all distinct). Its four allocated IDs happen to
be distinct as well. The reusable harness now performs this stronger committed
identity check inline; each run retains the exact earlier script it executed.
No original application assertions were changed.

Run `python3 reconcile.py` to verify portable hashes, deterministic expected
row/blob hashes, external ledgers, eviction counts and every cut report.
The run directories contain exact harness versions and allowlisted metadata;
raw disk images and database signing material are deliberately excluded.
This qualifies the specified unmerged candidate and workload. Final integrated
main qualification, performance comparison and deployment remain separate.
