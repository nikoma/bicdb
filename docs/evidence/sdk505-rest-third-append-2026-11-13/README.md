# SDK505 broad REST failure: third append remains uncertain

In [run 43531547345](https://github.com/nikoma/bicdb/actions/runs/43531547345),
[REST job 132250282106](https://github.com/nikoma/bicdb/actions/runs/43531547345/job/132250282106),
the `native_metadata_operator_cli` integration target reported **5 passed,
1 failed, 0 ignored** in 52.18 seconds. This is one target within the failed
REST workspace job, not the entire workspace's test count or an RTO result.
The original failure remains failed; its cause is **inconclusive**.

| Positive operation | Actual retained result |
| --- | --- |
| Append `items/external-first-ack` | Acknowledged, inserted true |
| Strong read of that key | Present, exact full object |
| Append `items/external-json-null` | Acknowledged, inserted true |
| Strong read of that key | Present true, value JSON null |
| Append `items/external-empty-object` | `QueryTimedOut`; no returned ACK; effect uncertain |

This was **not a first-write failure**. Two positive appends were acknowledged
and their full Strong replies were retained before the third positive append
failed. The earlier expected negative `before-initial-data-quorum` control is
recorded separately and excluded from positive-operation counts. JSON null
being present is distinguished from successful absence.

The uncertain operation has logical ID
`01a1d7a9-7fda-7e02-bc20-f7cecc2e95d9`, expected payload `{}` and SHA256
`44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a`.
Its subsequent Strong read was not reached. Timeout does not prove rollback,
durable loss or absence. The two earlier successful reads do not reconcile
this third operation or establish post-failure durability of earlier ACKs.

The API run head is `01308fb09512f02e0c61b740fc339382e44b3521`
(1.0.505-beta candidate). The actual PR checkout recorded in the log is
`222dda9319a54336e8428465338affb3b145ba60`. Retained Git commit/tree objects
verify their complete trees are equal:
`a25798570d16209cedc469de011ad3836c59dd4a`. The two commits are not the same SHA.
Seven source copies have blob and tree-membership proofs.

The fixture measured its actual CLI executable before process startup:
SHA256 `67468c83b7145ac299e1e20cf01b4aa31167520988b7d8d9ea9464c8fc39b09f`,
774,154,024 bytes. The binary file itself is not retained here. Its observed
runtime source field remains **`UNAVAILABLE_RUNTIME_SOURCE`**, with
`source_available: false`. Checkout/tree evidence is separate from that
missing declaration. The later retention adapter does not retroactively fill
the original source environment or prove a published binary/release/deployment.

The [exact original gzip log](raw-rest-job.log.gz) expands to 85,809,455 bytes,
SHA256 `e4b08eb8746494e23f63a64e358a27d2facc1e45a8b4826012a45b8aa2f94bfa`.
All [18 original ledger records](ledger-original.json) are matched independently
against that log. [Raw boundaries](raw-boundaries.json) identify the six test
results, failed stdout section, source panic and ledger positions. GitHub's
stdout delivery timestamps can reflect test-harness buffering; they are not
operation latency samples.

The existing fsynced RPC journal stayed under an unarchived runner TempDir.
Actual primary/refresh requests, retries, destination/range/epoch and native
outcomes are therefore unavailable in this packet. No database/custody files
were recovered or opened. Exact [terminal job metadata](job-terminal.json),
[run observation](run-observation.json), [source/binary identity](source-and-binary-identity.json)
and the [original classification](classification-original.json) retain those
limits. The run observation is a capture at its recorded time, not an invented
whole-workflow result.

Run the portable standard-library verifier without Git, network access or BicDB:

```sh
python3 path/to/this/packet/verify.py
python3 path/to/this/packet/verify.py --self-test
```

It streams the bounded raw log, derives the actual six results and 18 records,
checks ACK/read/uncertainty distinctions and missing source identity, and
validates source-tree membership. Tamper controls cover file integrity and
ledger mutations even with an updated local manifest. A passing verifier
validates evidence integrity; the native CLI test still failed.

See [issues and smallest next tests](issues.md). This evidence-only packet is
based on the original candidate and includes no runtime fix or CI-retention
change. No HA/failover, physical scaling, RPO/RTO, production or deployment
qualification follows. The historical failed Azure result is unchanged.

This is a public GitHub repository and its logs/artifacts follow upstream access
rules. Local restricted file permissions do not make upstream capture private.
The packet includes synthetic-test log/ledger data and public Git signatures,
not TLS keys, database roots or raw RPC journal files. The literal private-key
marker scan is deliberately limited and cannot guarantee absence of every
credential or secret format. Committed projections of future captures must
exclude keys, custody contents and raw frames.
