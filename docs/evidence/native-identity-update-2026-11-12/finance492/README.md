# Finance-based identity candidate qualification

Exact code and source version: `2403deefde68869c15f02b73d40406ffdfb50148`,
1.0.492-beta, over actual finance main490. The final parent-harness count is
1,700 Core passes, zero failures and seventeen ignored across 126 Cargo targets;
one nested child summary is excluded. Current SQL authority and version identity
add three passes. Version synchronization checks all twenty-nine packages,
internal requirements, tracked lockfiles and the current-source compatibility
report. Final PR CI and merge/version coordination remain separate.

`focused/` preserves the actual two failing reader regressions and their twelve
passing distinct controls (fourteen invocations), plus the earlier seventeen
passing issuer/grant/SQL/version checks. `validation/` preserves current full-Core
and SQL/version receipts, compressed original logs, the canonical parser and
controller specifications. Only derived result parsing removes interleaved
telemetry; raw log bytes are hashed and retained.

`recovery/` preserves the source-pinned probe custody, controllers, independent
verifier, all process/EIO stdout and stderr, original ACK files, VM consoles and
fsynced external witnesses. All 38 process, eight actual EIO and ten KVM cases
passed independent source/binary/hash/transaction/audit/reopen verification.
The disposable VM images remain on the existing qualification host at the paths
and SHA256 values in the VM result; they are not embedded in this repository.
The `production_equivalent` field in the inherited controller format identifies
the candidate implementation source; it is not a deployment receipt.

Every raw manifest maps original file names to stored names and raw SHA256/length.
Compressed files reproduce the original bytes when decompressed. Historical 490
CI metadata is labelled separately. No result here qualifies the separate private
partition/rename composition, full application acceptance, physical HA, capacity
expansion or experimental production rollout.
