# Legacy live path-based paged backup has incomplete physical snapshot exclusion

Source concern with an actual failed acceptance case; no corruption or lost-ACK claim.

At main `1.0.517-beta` / `bde74650c40934096e1cc6c6e75a3ea3c4643cf4`, postmerge Tests run `43666718926`, Core job `132667643037`, the unchanged `a_backup_taken_while_writes_are_in_flight_restores_consistently` failed at `crates/bicdb-core/tests/backup_paged.rs:271`:

```
Backup("source file paged/store.wal changed while the backup was streaming")
```

The target reported 3 passed/1 failed. Its original backup verification, restore/open and 100 pre-existing-key assertions were not reached. Earlier Core lib reported 932 passed/0 failed/8 ignored. The same source tree's separate premerge Core run passed; that success does not erase the postmerge failure or identify a timing cause.

Retained raw Core SHA256: `24ff5f1bab02071168fdf81460026e00e533da802ebaa0e832971f17adc32fb7`; gzip SHA256: `19abab5933712f4fb9d27093dc5009bb755ddf7d3254914b5f0a5b37f86e0b0e`. Canonical capture: `/root/bicdb-ha-private-evidence/main517-push-bde74650c-run43666718926`.

The legacy contract must remain visible. `backup.rs:2246–2270` explicitly allows an already-open source, claiming its segment/page WAL tails make the copy restorable. `backup_paged.rs:231–300` expressly requires successful backup/verify/restore while another real writer performs 300 inserts; it requires all 100 earlier rows remain. Do not change this producer or narrow the test merely to obtain green.

Source boundary:

- `create_backup` selects Standard (`backup.rs:365–370`), which takes the Core file-backed exclusive snapshot gate after preparation (`1096`, `2287–2294`). Standard files remain size-and-hash strict; `1336–1341` correctly refuses changed bytes.
- Core transaction-log writes hold the corresponding shared gate only around their individual physical write (`db.rs:2944–2962`). Atomic Core metadata/segment writes also acquire it (`storage.rs:245`, `626`, `674`, `780–805`). This does not establish exclusion across the complete later page application.
- Actual page materialization follows the Core decision (`spatial_paged.rs:2979–3051`). The page engine has independent writer/WAL locks; this source has no matching Core backup-gate acquisition in `bicdb-page`. In-flight page application or maintenance is therefore not proved excluded by the Standard gate. The log identifies a changed WAL, not the precise writer/checkpoint operation responsible.

A separate supported interface already preserves an actual cut: `BicDb::capture_online_backup` (`db/online_backup.rs:13–39`) takes commit exclusion and durability, obtains the actual page-writer/WAL locks, backup pin, page before-images and exact retained WAL descriptors (`backup/capture.rs:249–341`, `bicdb-page/paged.rs:847–887`, `wal.rs:488–527`). Its owned snapshot streams outside host DB guards. Existing `backup_repository.rs:74–190` checks concurrent writes/DDL/checkpoint and exact restored cut. That is separate acceptance of the owned protocol, not a repair or waiver of the legacy path-based contract.

Before an engine correction, audit and define one consistent lock/ownership protocol across ordinary/native page application, dirty-page flush and eviction, checkpoint/WAL reset, append-time rotation, close/drop, extent migration and recovery. It must bind the same actual database directory and exclude or safely retain every physical byte the legacy manifest/copy uses. A guard around just one foreground append is insufficient. Lock ordering and cross-process file-gate behavior must be explicit, including outstanding in-flight work at acquisition.

Keep the original concurrent test unchanged. A future deterministic regression should pause a real selected transaction after its actual Core WAL write but before page application, acquire the real legacy snapshot boundary, then exercise the actual page mutation/maintenance path. It must demand successful verification/restore and preserve every original row assertion; no fabricated counters, fuzzy flag, skipped hash check or error-as-success. The smallest lawful engine primitive is not yet established by this review.

Status: actual safe refusal observed; incomplete cross-layer exclusion is source grounded; precise mutator schedule and engine fix are unimplemented/unexecuted. The earlier proposal to replace this test's producer with owned capture is withdrawn as a fix path. Original proposal drafts are retained as history. No source edits/builds/CI actions occurred.


The [machine-readable issue and eleven source pins](legacy-standard-live-backup-defect.json) bind this finding to the exact source. Root independently checked all eleven blob/SHA pins and the retained failure logs.

| Mode | Source/CI evidence | Qualification boundary |
| --- | --- | --- |
| Legacy Standard path-only live paged backup | Actual post-merge acceptance failed; incomplete cross-layer exclusion inspected | Original success/restore test and strict changed-file refusal remain; engine fix open |
| Owned whole-database online capture | Same-tree pre-merge Core target reported 14 passes, including the actual concurrent-commits/DDL full-cut case | Separate protocol; post-merge Core stopped before this target; no legacy repair, physical result or production SLO follows |

Owned-capture execution receipt SHA-256: `15b12cbcbbb5f2167355edb4a1bc97e53170f52702a0ed5afa1249f8980b0bf6`. Source checks and CI functional execution remain distinct from current-artifact physical qualification.
