# Original cold-archive catalog reconciliation

This is a **read-only catalog examination**, separate from the failed
[longer Azure growth trial](../azure-long-growth-failure-2026-11-11/README.md)
and its later routed row-read checks. Runtime: **1.0.461-beta**, frozen source
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`. The five original stopped database
archives were captured before the bounded diagnostic restart. Their controller
digests matched the remote export receipts. No archive was modified, no files
were extracted, and no engine, replay, build or VM operation was run here.

| Original phase ledger | ACK identities checked | Exact payload in retained Applied command at/below resolved watermark |
|---|---:|---:|
| Four-node baseline | 3,804 | 3,804 |
| Four-to-five growth | 1,223 | 1,223 |
| Total | 5,027 | 5,027 |

Each of the two initially unverified ACK identities has an exact expected
payload in an Applied entry in four guest catalogs. Across all known ACKs,
matching Applied/QuorumCommitted command histories appear in two catalogs for
eight identities, three for 2,339, four for 2,556, and five for 124. These are
**retained catalog counts**, not a live replication-factor or independent-host
claim. Snapshot-installed prefixes, relocation cleanup and compaction can remove
old command history while rows remain; absent history would not prove lost data.

All five catalog envelope checksums and all retained command checksums matched.
Entry indexes and resolved/compacted watermarks passed the implemented structural
checks used by this script. Expected metadata was reconstructed from the archived
native ledgers (`run`, 1 KiB `payload`), with event record identity
`namespace/key` in collection `__bic_cluster_events`. Source definitions are
[`distribution_events.rs`](../../../crates/bicdb-core/src/distribution_events.rs)
and [`distribution_range_consensus.rs`](../../../crates/bicdb-core/src/distribution_range_consensus.rs).
Compact JSON checksums matched for this retained corpus; this Python reader is
not a general replacement for BicDB's recovery validator.

Applied/QuorumCommitted are persisted local protocol states. This check does
**not** independently verify voter signatures, a quorum certificate, current
row/page visibility, or successful reads through a running database. Unkeyed
checksums establish internal consistency, not authenticity. Five guests do not
establish five independent physical hosts. This result does not turn the original
failed growth/availability trial or later unsuccessful row-read checks into a pass,
and establishes no general RPO, RTO or production qualification.

`summary.json` retains numeric findings, original archive/catalog/ledger digests
and explicit limits. `per-ack-catalog-observations.json.gz` retains hashed event
identities, expected/observed metadata hashes, command checksums, node labels,
states and watermark observations. It contains no archive, raw key, certificate,
endpoint or account identity. `manifest.json` hashes the exported bytes and the
exact reconciliation helper. Raw archive/catalog hashes describe original private
inputs; exported observation hashes describe newly generated public evidence.

From the repository root, after the linked longer-trial ledgers are present:

```sh
python3 scripts/verify-azure-cold-catalog-evidence.py
```

This checks public artifacts and matches the 5,027 hashed identities/payloads to
the public native ACK ledgers. It explicitly reports `private_archives_read=false`
and does not read, open or replay private databases. To repeat the independent
tar-member reconciliation against the original retained private inputs:

```sh
python3 scripts/verify-azure-cold-catalog-evidence.py --archive-dir /path/to/private-original-archives
```

The optional check requires `receipt.json` and `node-0.tar.gz` through
`node-4.tar.gz`; it verifies their original hashes and recomputes the complete
export. Private inputs are never copied into this public evidence directory.
