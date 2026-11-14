# Original failed catalogue cohort: separate native WAL follow-up

**Both original failed targets contain exact base/archive publication bytes logged under committed native xids.** This later readonly investigation strengthens the observation available for the original artifacts. It does not make the failed tests pass, establish current key visibility or prove that a sync completed.

The source is **`e1ecfeab71d3f1e104530e9d6aa3bf37971abd65`**, unmerged runtime candidate **1.0.494-beta**, original [run 43493938281](https://github.com/nikoma/bicdb/actions/runs/43493938281), private artifact **14070838685**. The exact original archive SHA-256 is `5445c509695372da8f5038c5e8073e9e8ce9619349c311221dbed743d97ab026`. The earlier frozen three-run packet remains byte-identical, including its then-inconclusive native-base outcomes and missing post-error runtime observations. This packet adds a distinct interpretation based on a later inspection of its original retained bytes. No later experiment or source revision supplies evidence here.

| Original failed case | Exact base attempt | Native decision observed in the retained WAL |
| --- | --- | --- |
| `TargetCatalogueFloorDirSyncRetry` | `01a1d670-8f70-7123-8133-17aaf606bbf3` | Commit xid21, LSN97, byte offset469824 |
| `TargetCatalogueFloorCanceled` | `01a1d670-8e3e-7f81-b7df-2357de618e2f` | Commit xid21, LSN97, byte offset469824 |

The xid was identified from each case's exact `install:base:1` and attempt-specific archive keys, their actual Btree leaf tuple locators, generation-checked heap slots, MVCC `xmin` and overflow values. It was not guessed from a previous passing fixture. Both values are the same 3,914-byte checksummed Registered receipt, with native xid21, selected range R1, source H0/H, target birth, namespaces, intent, image root and final-image pointer matching the original manifest and final report. The base pointer was followed to actual native final CURRENT/archive values under xmin20 and Commit20. Their full record checksum and nested source-manifest checksum were independently recomputed; the native final record matches the original final report, including the original database-open identity and protected catalogue UUID.

The [safe observation](observation.json) retains selected decoded receipt fields, source/blob/SHA pins, complete target WAL member identities, exact frame hashes and locator/MVCC/overflow bindings. Each failed target has one 469,864-byte native WAL, with **73 fully CRC-checked contiguous records, LSN25 through97**. No sealed target segments were omitted from its retained inventory. The unavailable earlier prefix is not reconstructed, and absence is never treated as an abort or a no-write proof.

This proves the publication bytes and native decision recorded in that retained suffix. It **does not prove reachability from the actual committed meta/Btree root**, current native-key visibility, a successful native API return, a returned durable-sync receipt, completed fsync, physical power-loss survival, catalogue adoption, Strong reads or Serving. Leaf/heap/overflow parsing checks exact WAL-protected bytes and bounds; it does not claim disk-page finalization CRC verification or an engine visibility calculation. No database, recovery or checkpoint is opened or executed.

The copies were made with the original service/DB/native-journal guards held while their engine owners remained alive. They are stable **live-owner file copies**, not stopped or atomic power-loss snapshots. Copied commit bytes can include page-cache contents. Pinned source uses ordinary `paged.commit`: it appends the native commit decision and then syncs. The frame alone cannot prove the later sync result. Under the pinned single-publication control flow, the observed `QueryTimedOut` accompanies an operation that reached its identified native Commit decision; a precommit closure refusal instead takes abort. This is a **source inference** about reaching the decision, not evidence of which later authority check timed out or a successful native API return.

The original cohort remains **2 passed / 2 failed**. Both failing cases still stop at the `PublishNativeBase` prerequisite before their named catalogue fault is armed. Directory-sync retry and cancellation-retry remain **not tested** in that cohort; the later raw observation does not retroactively exercise them. Original post-error owner state, native API return and post-timeout selected-record visibility remain unavailable. Production qualification and the failed Azure scale-out result are unchanged.

## Verification

Public verification needs no credentials, engine or private archive:

```sh
python3 docs/evidence/native-original-catalogue-wal-e1ec-2026-11-13/verify.py --self-test
```

It checks this packet's complete hashes, selected receipt/decision correlations and explicit claim limits. **It verifies the retained public observation, not omitted raw WAL.** `--repo /path/to/bicdb` additionally verifies all 17 source blob and SHA-256 pins at e1ec. Negative public controls reject a different MVCC xid and an invented current-visibility claim.

An authorized custodian can independently reproduce the original private-byte inspection:

```sh
python3 docs/evidence/native-original-catalogue-wal-e1ec-2026-11-13/inspect-native-wal.py \
  --original /private/focused-core-e1ec-catalogue-four.zip \
  --repo /path/to/bicdb --self-test --output /tmp/native-wal-observation.json
```

The original ZIP must match the exact hash above. The inspector never extracts a database or prints page/record/secret bytes. It parses bounded WAL frames and selected Btree/heap/MVCC/overflow values directly from the ZIP, checks all retained CRCs and receipt/source/final checksums, and emits only the safe structural observation. Six private-byte negative controls reject CRC corruption, a wrong tuple generation and a bad receipt checksum in each case. Bounds are explicit in the observation and code: 64MiB archive/WAL member, 128MiB total uncompressed members, 100,000 frames per case, 1MiB page payload, 128KiB selected value, 32 overflow pages and 4,096 selected-xid pages. Unexpected formats or missing/mismatched evidence refuse verification.

No raw native WAL, pages, encryption/signing keys or full native record bytes are published in this packet. The public fields are integrity observations and cannot authorize recovery or writes.
