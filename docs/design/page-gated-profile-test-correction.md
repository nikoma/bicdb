# Current Page profile test-oracle correction

This test/documentation leaf starts at source
`0a04ccc06db33a04d254a5e00776248d7ea1772a` (candidate 520). The mandatory
physical backup gate and all four output-profile mappings already exist in
that source. The correction changes exact output assertions to that contract;
production format, parser, gate, protocol and lifecycle code remain unchanged.

## Preserved actual failure

Normal Tests run `43694564959`, REST job `132752155091`, checked out the exact
candidate tree. The original case
`segmented_superblock_uses_v2_magic_and_legacy_stays_v1` failed at
`crates/bicdb-page/tests/extent_segments.rs:137`: actual bytes were
`[66,73,67,68,66,80,71,54]` (`BICDBPG6`), while the assertion required
`BICDBPG2`. The plain-file assertion at line 142 was not reached.

The complete original raw log is retained by the sole collector at
`/root/bicdb-ha-private-evidence/visibility520-pr207-0a04ccc06-run43694564959/job-132752155091.log.gz`,
raw SHA256 `d2d755c06f12a42d2bbcfb6c5bc1dafa9da3978e24d313aad36c1af250de4902`.
Actual failing stdout is lines 2619–2624. The target ran 6 passed / 1 failed.
This is a real byte-contract assertion failure, not a measured lost commit.

## Exact case mapping

| Original case | Corrected case | Current-output requirements |
| --- | --- | --- |
| `segmented_superblock_uses_v2_magic_and_legacy_stays_v1` | `mandatory_gated_profiles_keep_segmented_and_monolithic_layouts_distinct` | Fresh segmented `BICDBPG6`; fresh monolithic `BICDBPG5`; distinct layouts recover through actual current opens. |
| `tracked_page_format_fences_old_writers_and_survives_relayout` | `mandatory_tracked_profiles_preserve_relayout_and_old_magic_separation` | Tracked segmented `BICDBPG7` before/after reopen; tracked monolithic `BICDBPG8` after relayout; conversion back retains `BICDBPG7` and its original tracking-generation check. |

The second case had not executed in this run: bounded REST stopped at the
failing extent target. Its old 3/3/4/3 output expectations contradict the same
current mapping by source inspection; they are not a measured second failure.
Both cases create their files using the real current Page API, so the
variable formerly named `legacy_path` was a fresh monolithic producer, not a
historical v1 artifact.

The corrected assertions accept only the exact current value at each stage.
Actual bytes are logged before those assertions. Additional checks preserve
the physical layout distinction through real current reopen/relayout and keep
each output outside all four pre-gate magic values. That last check is a
byte/source compatibility assertion against the literal accepted set in
main519 `587dd6e4775e454cc9e8224d5fed76e8795c6ff6`,
`manager.rs:2672–2701`; it is not a mirrored decoder or old-binary execution.

Every existing extent-routing, migration, crash-replay, row, relayout, reopen,
tracking-generation and budget assertion outside the corrected byte oracles
is retained. Historical input/fixture bytes remain unchanged. This test leaf
does not relax output assertions to accept both old and new profiles.

## Evidence states

| Scope | State |
| --- | --- |
| Original extent byte-contract case on `0a04ccc06` | Failed, actual 6/1 target preserved |
| Related tracked-output mismatch | Failed source expectation check; runtime not tested |
| Exact four-profile contract and unchanged production source | Passed source inspection/preservation only |
| Corrected test execution and historical reader execution | Not tested |
| Core/Native/SQL/PGwire/Oracle/Rails acceptance | Not tested by this leaf; original run has separate Core `PagedWriteGuard: Drop` compilation failures |

The new checksum/no-repair Page unit actually passed on `0a04ccc06`; that is
coverage of its original source, not execution of these corrected test bodies.
No binary, merge, release, deployment or HA qualification is produced here.
