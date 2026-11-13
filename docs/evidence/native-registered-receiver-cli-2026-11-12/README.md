# Registered receiver passed; initial DATA cohort failed (2026-11-12)

The exact source `3ee40c19b44affa0a481a66b278ded3af817e910`, package
`1.0.488-beta`, produced **3 Core passes** and **4 CLI passes / 1 CLI failure**.
The new registered-receiver CLI case passed. The earlier initial-DATA case failed
on its JSON-null append. This bundle preserves that failure and contains no
runtime fix or rerun.

| Check | Actual result | Immutable CI run/job |
| --- | --- | --- |
| `native_registered_replica_receiver_` | 3 passed, 0 failed, 913 filtered; 2.28 s | [43262614352 / 131337645351](https://github.com/nikoma/bicdb/actions/runs/43262614352/job/131337645351) |
| CLI `native_metadata_operator_cli` | 4 passed, 1 failed; 79.87 s | [43262683104 / 131337870637](https://github.com/nikoma/bicdb/actions/runs/43262683104/job/131337870637) |

The replacement CLI run used immutable branch
`ci/native-registered-receiver-cli-488`. The earlier run `43262614233` was canceled
by same-ref workflow concurrency before a test result; it supplies no result for
this table. Workspace all-target and isolated-consumer checks passed on the
replacement run. No local build was run for this investigation.

## What failed

`initial_data_tests::native_initial_data_actual_cli_four_process_first_ack_and_strong_full_value`
failed at `initial_data_tests.rs:254`. Its original fsynced ledger contains **15
records**, including these three attempts:

| Key / phase | Actual reply | Subsequent read |
| --- | --- | --- |
| `before-initial-data-quorum` | No ACK; explicit pending local-leader refusal | Not attempted |
| `external-first-ack` | ACK, inserted `true`, entire nested object recorded | Strong returned the exact full object |
| `external-json-null` | No ACK; `no event cluster member is reachable` | Not reached |

The empty-object attempt and later absence assertions were not reached. The null
operation's **durable outcome is unknown**. No-user-ACK does not mean rollback.
The captured range catalogue/page/WAL needed to reconcile it is unavailable.
The earlier ACKed full object was read before the failure; no post-failure
ACK reconciliation or successful re-read was established.

All four children (PIDs 16241–16244) were alive before the test's cleanup. Cleanup
then SIGKILLed and actually waited for each child. The copied metadata stores
agree on term 1, generation 1 and commit index 1: n0 leader, n1/n2/n3 followers
of n0. Saved command statuses show listeners, writer policies and dispatchers
published, with each node's local leader-admission observation true. These are
saved diagnostics, not fresh per-request Strong readiness proofs.

Child stderr contains metadata supervisor store-busy / heartbeat-builder-busy
messages, one supervisor timeout, and replica RPC EOF / broken-pipe errors.
Those messages have no per-operation timestamp/correlation sufficient to assign
them to the null append. They do not establish its cause or durable result.

Offline application of the recorded placement hash to the actual captured C
routes the full-object key to range 4/E1, leader n0, voters n0/n1/n2; the null
key routes to range 1/E1, leader n1, voters n1/n2/n3. This is an offline route
derivation, not a claim that an attempt reached its destination. In the captured
`distribution_events.rs`, `request()` can overwrite the earlier APP error with
`no event cluster member is reachable` when a later bounded discovery attempt
fails. The final message therefore does **not** prove that all four processes
were offline, nor that the null command never reached Prepare/Commit/WAL.

## What the new receiver case proved

`replica_receiver_tests::native_registered_replica_receiver_actual_cli_four_data_then_fifth_same_owner_no_data_grant`
passed with **13 stdout ledger records**. Four real initial processes first
ACKed and Strong-read the complete nested object. One fifth `join-fresh
--replica-receiver` process then reached:

- Same retained birth, metadata StoreUUID and namespace identities; sequence 0,
  with no hidden command-file orchestration.
- Actual local committed metadata-voter state, owned control listener and
  receiver-ready status; `data_serving=false`, no assigned data range.
- Actual APP Append/Get/Scan requests refused. No writer hook, native genesis,
  recovered-epoch seed, image installation or data-serving grant was inferred.

The same old gateway's post-join Strong call returned **client query-timeout
error**. It did not return successful absence, but it also did not prove
healthy bounded ACK/read continuity after join. Permanent unavailability is not
an acceptable completed operator outcome. Native placement/controller,
authenticated native image/joint wire, actual image/joint OpenAdmission and
healthy post-join service remain required gates. This receipt is not a
five-node DATA move, multi-VM result, SQL qualification or Azure-cause claim.

The other three CLI cases passed: original four-process metadata/data-refusal,
single JoinFresh metadata voter plus sixth metadata control, and promoted
metadata-voter cold reopen. [receipt.json](receipt.json) names all five cases.

## Custody and verification

The full raw CLI log SHA-256 is
`620630305bc999b9ba4095ea679efb640592fa23114d8d04d9c000059df872a5`;
the Core log SHA-256 is
`0d35656d8afccbedaca28556409913c562195b0a5fcc94943ee14ba26e1ad5af`.
Both compressed full logs and their original capture indexes are under `raw/`.
All 28 already-fsynced ledger records emitted to stdout are also extracted under
`observations/`; this is not a newly fabricated external ACK ledger.

The runtime-source field is the actual workflow-provided source SHA. The actual
CLI executable was streamed before startup: 750,173,072 bytes, SHA-256
`9d13261e50cd1b7526736a9f2344ec932ab14047c7f6e0591d61cf11d189e703`.
This is an execution binary digest, not a published-release/download claim.

Artifact **13965697314** is 92,073,518 bytes, SHA-256
`92901d8d7dfeb372461a9afc7fc7858432f96ec1785f58c1b045ea87b5f1d832`.
Its exact ZIP remains in restricted private custody. The original manifest
`c42946732dde932a03eda16c9ae4ec58071f22fb9ddbbc33064e974ee93cf38d`
verifies **26 allowlisted stopped files / 58,126 bytes**: command/status/stdout/
stderr, common plan, bootstrap seed, saved metadata stores and the failed case's
15-line ledger. They are copied exactly under `diagnostics/`. **There is no full
DB tree, page/WAL, range-write catalogue, TLS key/certificate or durable ACK
reconciliation archive.** The artifact's tracked source archive is retained in
the private ZIP; its separate checksum is in `receipt.json`.

```sh
python3 verify.py
python3 test_verify.py
python3 verify.py --repo /path/to/bicdb
python3 verify.py --private-zip /restricted/path/artifact13965697314.zip
```

The optional repository check verifies the three bundled source excerpts against
the exact commit; it does not independently reconstruct every runtime file or
prove execution authenticity. The private-ZIP check verifies the entire original
archive and allowlisted-file parity, without opening any engine.

To execute the original cohort on a suitably provisioned runner, check out the
exact source and run the original public integration target with no assertion
changes:

```sh
BICDB_TEST_SOURCE_SHA=3ee40c19b44affa0a481a66b278ded3af817e910 \
  cargo test -p bicdb-cli --test native_metadata_operator_cli --locked -- \
  --nocapture --test-threads=1
```

This command is a reproduction path, not a claim that the availability failure
is deterministic or that a rerun has occurred. Keep its runtime source/result
separate from this preserved first failure. Original Azure evidence is unchanged.
