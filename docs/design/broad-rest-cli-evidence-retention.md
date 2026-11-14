# Broad REST CLI evidence retention

This CI-only successor uses the existing CLI RPC journal and bounded stopped-file
failure allowlist. The broad `rest of workspace` test step supplies
`BICDB_TEST_SOURCE_SHA` from the actual workflow checkout SHA and places the two
existing evidence outputs under exact, separate runner evidence subtrees. An
`always()` upload retains only those subtrees, including after a failed test.

The tests already fsync their journals. No runtime/test code, test sequence,
assertion, API timeout, retry, test order or four-thread setting changes. The
workflow does not enable stopped database custody, a wholesale fixture root or
recursive `/tmp` retention. The existing failure copier accepts only its
explicit metadata/stdio/ledger paths after actual child waits; it does not
recursively copy database files or TLS directories.

This responds to run `43531547345`, REST job `132250282106`, at candidate head
`01308fb09512f02e0c61b740fc339382e44b3521` (1.0.505-beta). The specific native CLI
target remained **5 passed, 1 failed** in 52.18 seconds. That duration is not RTO.
The failure was the **third positive append**, `items/external-empty-object`,
logical ID `01a1d7a9-7fda-7e02-bc20-f7cecc2e95d9`, payload `{}` with SHA256
`44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a`.
It returned `QueryTimedOut` without an acknowledgment; its durable effect is
uncertain. This was not a first-APP failure: two earlier positive appends were
acknowledged, and their Strong reads returned the full object and a present
JSON-null value. The expected pre-open negative write is a separate control.
The failing append's subsequent Strong read was not reached.

The retained original log has SHA256
`e4b08eb8746494e23f63a64e358a27d2facc1e45a8b4826012a45b8aa2f94bfa`
and 85,809,455 bytes. Its actual PR checkout was
`222dda9319a54336e8428465338affb3b145ba60`, whose complete tree
`a25798570d16209cedc469de011ad3836c59dd4a` is verified equal to the candidate
head's tree, despite the different commit SHA. The fixture measured CLI binary
SHA256 `67468c83b7145ac299e1e20cf01b4aa31167520988b7d8d9ea9464c8fc39b09f`
and 774,154,024 bytes before process startup. Its runtime source environment was
**unavailable**; this new workflow setting does not retroactively fill it.

The original fsynced RPC journal stayed in an unarchived runner TempDir. Actual
request paths, routing changes, retry counts, range epochs and native outcomes
therefore remain unestablished. The cause is **inconclusive**; two successful
reads do not reconcile the later uncertain append or qualify this release.
This retention change preserves the next run's existing evidence and does not
repair or qualify the failed behavior.

Upstream artifacts belong to a public GitHub repository under its access and
retention policy. Local 0700/0600 permissions do not make that upload private.
These are synthetic test journals, including complete protocol frames, and the
existing narrow failure allowlist. The retained paths omit database and TLS
directories; the journals still contain complete test application payloads.
They must not be described as excluding raw frames. Neither public artifact
access nor local file permissions establish absence of secrets in arbitrary
output.
