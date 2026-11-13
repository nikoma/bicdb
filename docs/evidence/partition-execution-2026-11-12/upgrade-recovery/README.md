# Complete ACK preflight and migration recovery

These results cover private runtime `6c16e66e742bb575041aebce4086ebf434b9fe64`,
which includes actual main489. They are not a main delivery, release or deployment.
The full campaign remains active. Exact sources and scope are in `summary.json`.

The test-only baseline `4508ece82` wrongly certified all five invalid stationary
leaf cases: CHECK, NOT NULL, UNIQUE, foreign key and inconsistent physical PK.
The fix validates every resulting row against proposed v1 descriptors before
writing the reader fence. Uniqueness uses bounded keys without trusting old
indexes; foreign keys see the complete proposed data, including virtual parents.
Existing `NOT VALID` exemptions remain, with new-write enforcement tested.

At `6c16e66e`, thirteen admission units and twenty-five schema/key integration
tests pass. Thirty genuine old482-store cases pass: eight default admission,
eight explicit operator migrations and fourteen entry/startup controls. They
retain the fourteen independent source ACKs and immutable original file hashes.
The original failed tests and the intervening compile failure remain archived.

The recovery matrices use copies of those real old stores. Four cases in each
storage mode include both reviewed policy choices. Controls bind complete old
and new Native records, content hashes and raw schemas to the old ACK ledger.
Each actual migration consumes exactly one Native commit in these controls.

| Matrix | Executed cases | Result | Limit |
| --- | ---: | --- | --- |
| Process abort / specified volatile emulation | 152 | All pass; 88 recover complete old state, 64 complete new state | Simulated unsynced page-WAL tail and directory loss is not a VM power cut. |
| Actual scoped I/O failures | 96 | All pass; all recover complete old state | One-shot and persistent EIO at fence write/sync/rename/directory-sync and publication transaction-log write/sync. |

Every process cut was reached and aborted. The independent fsynced migration
ACK is written only after `apply_partition_upgrade` returns; acknowledged cases
must recover the complete new state. All cases match exactly one whole expected
row/schema set through three stable opens, validate Native indexes, and prove
old-reader refusal without changed files whenever the fence is present.

Every EIO case requires the requested fault to fire and the migration to return
an error without an ACK. After removing injection, the same failed handle must
refuse a legitimate write. Another process checks recovery without a graceful
database close. The independent verifier checks exact coverage, no duplicates,
canonical per-case results, stable reopen observations, binary hashes and all
original fixture files. These are overlapping invocations, not 248 unique ACKs.

`original-source-scoped-results.tar.gz` retains 1,762 reports, snapshots and
stdout/stderr files. `archived-files.json` binds every member by size/SHA256;
`manifest.json` binds the files here. Database directories and credentials are
excluded. The source snapshots contain only the controlled old-fixture data.
`production-equivalence.json` proves that the later process/I/O probe sources
have exactly the same Core, Page, SQL and wire runtime trees and workspace
manifests as `6c16e66e`; they add qualification-example behavior only.

The exact probe binaries remain on S3 under
`/root/bicdb-durability-s3/qualification-artifacts-debug/`: `6c16e66e...` contains
the upgrade/startup probes, `fa7a2c59...` the process probe, and `430067f7...` the
I/O probe. These are qualification artifacts. The Root standard runnable CLI
remains main482; no new standard CLI, server release or deployment is implied.

The earlier `cc84f24f` broad run finished with 1,391 Core / 2,550 SQL / 491 wire
passes and zero failures. Its source predates main489 and the later SQL batch.
The exact `6c16e66e` broad run is still active at this checkpoint. Required VM,
protected Native/replication/HA, remaining partition lifecycle and final-source
CI/merge gates remain open, as do the original Oracle/PG 1,464-test inventories,
unchanged waits, UI400 encrypted full restore, portable rename origin and P1/P2
qualification. P2 remains off; no cloud spending or promotion occurred.
