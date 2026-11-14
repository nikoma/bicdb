# Native image preservation, rows and catalogue: exact e1ec evidence

This packet preserves three terminal hosted CI runs of **`e1ecfeab71d3f1e104530e9d6aa3bf37971abd65`**, runtime candidate **1.0.494-beta**. It is evidence for that source, not a test of a newer `main`, a released binary, a deployed cluster or physical scale-out. Source/tree/file digests are in [source-index.json](source-index.json); all six complete raw job streams and their raw/compressed SHA-256 identities are under [jobs](jobs). No runtime code or historical evidence is changed by this packet.

| Exact-source run | Core / workspace job | Observed result | Classification and boundary |
| --- | --- | --- | --- |
| [Cold7](https://github.com/nikoma/bicdb/actions/runs/43492849838) | `132118360383` / `132118274703` | 7 passed, 0 failed, 16.22 s | **Passed:** index/refusal/readonly controls and actual SIGKILL preservation. Cold repair remains **not tested**. |
| [Rows5](https://github.com/nikoma/bicdb/actions/runs/43493986492) | `132122207844` / `132122153321` | 5 passed, 0 failed, 27.05 s | **Passed:** stored history/row copy, pre/post-WAL cancellation, missing-audit refusal and inner durable-WAL apply failure. Serving remains **not tested**. |
| [Catalogue4](https://github.com/nikoma/bicdb/actions/runs/43493938281) | `132122021034` / `132121981221` | 2 passed, **2 failed**, 34.34 s | **Passed:** actual selected-floor adoption and missing-base refusal. **Failed:** two prerequisite native-base publications timed out before fault arming. Their named fault guarantees are **not tested**; native mutation outcomes are **inconclusive**. |

Each workspace job passed `cargo check --workspace --all-targets` and the workflow-consumer compile. These are compile checks, not a full workspace regression execution. Core selectors and every actual result are retained in the public artifacts and raw logs. The workflow uses Cargo jobs 1, incremental compilation disabled and four Rust test threads. Suite durations are **not RTO measurements**. This corpus has 14 passing focused tests and two failures; verifying the packet successfully does not turn those failures into passes.

## What was observed

Cold7 killed the real child process with SIGKILL (9) at **AppliedKnown before protected RowsHeader progress**. The original logical transaction `23`, native row-intent xid `12`, attempt `01a1d669-c523-78e2-83bc-5710b2b9c944`, original database-open identity and mutation hash are correlated among the pre-crash marker, recovered native pending index and readonly WAL facts. After the kill, the stopped **target only** was copied before its first native open. The ordinary open refused before generic logical replay/cut with one outstanding pending attempt. Four business/log files were reported unchanged: `events/events.seg`, `incarnations.log`, `sync.log` and `transactions.log`. Native page recovery itself can mutate native files; the test does not claim a byte-identical database.

An independent verification receipt is retained in [provenance](provenance/root-e1ec-cold-seven-independent-verification.json): **20 stopped-target files** were hashed and three raw CRC-framed records (`tx_write`, `tx_commit`, `tx_commit_materialized`) were parsed. The actual selected record and mutation hash were recomputed from retained bytes. These bytes establish the observed commit outcome; they do not mint a durable-sync receipt, reconstruct authority or prove cold self-healing. All five services died in the same process, and the other four stopped roots were not archived by this control. The next cold gate is actual recovery/materialization and safe re-admission of this exact pending attempt.

Rows5 independently exercised all five named controls. Three completed copies each matched one full selected acknowledged event record, 177 serialized bytes, with target visibility B=0→1 and audited stored history R=1. The other target range remained empty/unknown. Two cancellation controls independently cross-check insertion of the actual pending intent (logical Tx23/native xid12), original-open/birth binding and its removal in the same protected progress transaction. The sticky reader fence remains after removal. Native page head-xmin/status are enforced by the engine and reported by the fixture; the public verifier does not decode native pages.

Three same-open WAL controls retain actual owners and pins. Pre-WAL cancellation releases its pin only after same-Tx progress. Post-apply cancellation retains 568 WAL bytes through an explicit ServerPaged checkpoint, then same-Tx progress releases the pin and the next checkpoint cuts the log to zero. Inner apply failure retains the original known-durable sequence with no applied progress, keeps the pin and refuses a fresh retry through the existing health fence. The unhealthy database is not checkpointed. The original pre-checkpoint bytes are not separately archived for Rows5; the retained-byte/hash observations are cross-checked among actual reports. Cold7 supplies separate stopped-byte evidence.

Catalogue4 reached actual adoption in one case: native base xid21 remains anchored to final-image xid20, and only the selected protected catalogue floor/binding changes. The full selected ACK event record is observed afterward; the writer remains uninstalled and Serving false. The second passing case refuses adoption without an original verified native base and preserves state.

Both `TargetCatalogueFloorDirSyncRetry` and `TargetCatalogueFloorCanceled` failed at **`catalogue_tests.rs:102`, `PublishNativeBase.unwrap(QueryTimedOut)`**. At this pinned source the directory-sync hook and cancellation branch occur later. Both journals retain a successful Finished source reply, but neither case emits a post-error owner/native-base outcome or catalogue-fault report. Thus no directory-sync EIO, cancellation-retry, rollback, committed-base, absence-of-base or post-timeout read claim follows. The exact native mutation outcome remains **unknown**. [issues.json](issues.json) retains these failures and the next smallest diagnostic test: capture retained attempt/xid/outcome and native CURRENT/archive/readback at the failing boundary before assertions, preserve the original budgets and four-case cohort, then reach each named cut. Later diagnostics, singleton results and source-only reviews at `e6b` are excluded from this packet.

## Acknowledgments are separate from internal projection outcomes

| Ledger | Returned source API ACKs | Failed source calls | Uncertain source calls | Separately recorded caller retries |
| --- | ---: | ---: | ---: | ---: |
| Cold fixture | 2 | 0 | 0 | 0 |
| Five Rows fixtures | 10 | 0 | 0 | 0 |
| Four Catalogue fixtures | 8 | 0 | 0 | 0 |
| Total | **20** | **0** | **0** | **0** |

These are `ClusterEventClient.append` calls to the **logical event collection `items`**. Their underlying stored records are in **`__bic_cluster_events`**; the physical `items` collection is empty. They are not ordinary PostgreSQL table inserts. Retries internal to the wire/client are **not measured** by this caller ledger. Logical source-frame calls are counted only when `operation == source_image_frame`, excluding the seed/OLD entries in the same journal (Rows:45; Catalogue:71). Original Manifest and subsequent frame requests/replies are preserved rather than reissued to fill evidence gaps.

Internal target effects have a different ledger: one Rows apply outcome is uncertain to its caller while actual WAL durability is retained; one Cold projection is killed before returning, with committed WAL observed afterward; two Catalogue native-base prerequisites time out without a captured post-error outcome. These four internal outcomes must not be subtracted from source ACKs or silently treated as rollbacks. The completed selected-record checks cover the stated stages and do not reconcile every source ACK through full failover. This is **not an “18/20” or “20/20” failover RPO result** and not evidence of zero acknowledged loss under the production fault model.

## Portable verification and custody

Run from an exported checkout, without credentials, network or an engine binary:

```sh
python3 docs/evidence/native-cold-rows-catalogue-e1ec-2026-11-13/verify.py --self-test
```

`--repo /path/to/bicdb` additionally compares every source digest with `git show` at the exact pinned source. Verification checks the complete packet inventory, all six raw and compressed log hashes, actual test selections/results, three public ZIP inventories, source ACK payloads, actual OLD observations, source-frame bindings, row/index/checkpoint outcomes and catalogue classification. Negative checks reject an altered ACK payload hash and an invented post-error catalogue report. It neither opens retained databases nor constructs any recovery or writer grant.

The public ZIPs contain only explicit logical reports, test/source/toolchain logs and ledgers. Entire native DB/custody trees, key material, binary WAL and private custody manifests are excluded. Each projection manifest records every included and omitted member's path, byte count and SHA-256. Cold/Rows projections are byte-identical to their earlier captured safe projections; [project_catalogue.py](project_catalogue.py) provides the new explicit Catalogue whitelist and deterministic projection. The complete original archives stay private with restricted permissions, identified by artifact id/SHA in [manifest.json](manifest.json). Hash manifests prove identity and custody accounting, not contents or correctness of omitted pages.

For custodians only, `verify.py --cold-original /private/original.zip` additionally streams the original Cold file hashes and CRC/payload checks, without invoking a database. Without this option, the public verifier checks the retained independent receipt and matching logical facts; it does not pretend to inspect omitted bytes. Rows/Catalogue private file checks were performed on **guarded live-owner stable copies**, not stopped, atomic, power-loss or reopened snapshots. The unchanged original private Catalogue reconciliation receipt is retained for provenance; its `catalogue_adopted:false` in failed cases means no adoption was observed, not proof of an absent or rolled-back native base. This packet's verifier explicitly reports those outcomes as inconclusive.

## Qualification remains open

No test in this packet establishes actual joint activation/Open, Ready/Serving, ordinary APP admission followed by full Strong reads, populated second-range movement after first-range Serving, controller-driven bounded convergence, source retirement, independent-range progress during stalled fsync, arbitrary SQL joins/transactions, full index/FTS/queue/audit parity, independent VM/host failover, sustained post-convergence capacity gain, production qualification or deployment. Local spatial maintenance events may occur during copy; suppression of newly generated business audits/broker publishes is not qualification of all assets. Accepted index bounds do not establish a strict pre-allocation bound for corrupt oversized cursor values.

The original failed Azure scale-out run, old 7ac failure cohort and frozen d690/phase evidence remain unchanged. The production gate still requires zero lost acknowledged commits, correct reads/routing, bounded fault-safe convergence and sustained throughput gain **after** convergence.
