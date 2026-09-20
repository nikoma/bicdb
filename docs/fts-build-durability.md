# Full-text index build durability

A corpus-scale full-text build runs for hours or days. This records what
survives an interruption, what does not, and what was measured rather than
assumed.

## What is genuinely crash-safe

`fts_build.rs` is written to be restartable and the machinery works:

- a phase checkpoint (`checkpoint.json`) written with `write_atomic`, moving
  through `Tokenizing -> MergePk -> MergeImpact -> Publishing -> Complete`;
- sorted runs completed atomically, so a crash may leave a `.tmp` file but
  never a run a resume mistakes for complete;
- the page store, not the build directory, remains the final home of posting
  blocks.

Verified by `fts_build_resume.rs`: interrupting at every phase boundary and a
planted truncated `.tmp` run both produce an index that answers **identically**
to one built without interruption. Nothing is corrupted and nothing is lost
from the base data.

## Resume, and what it was worth (fixed in 1.0.208-beta)

The build machinery always resumed correctly; what was missing was
**reachability**. The catalog entry survives a crash, so the retry an operator
naturally issues — `CREATE INDEX` again — was rejected with
`index ... already exists`, and the only way forward was `DROP INDEX`, which
discards the workspace. A paged B-tree already had a recovery hook for this
(`reconcile_published_paged_btree_build`); full text did not.

`create_index_online` now has a full-text arm: when the named index exists but
its workspace holds a checkpoint whose phase is not `Complete` **and** the
catalog entry describes the same index, the build is re-entered and continues
from the recorded phase. A different index under the same name is still a
genuine collision and is still reported as one.

The arm adds no build logic of its own — it re-enters the same backfill the
original `CREATE` ran — so resume and first-run cannot drift apart.

Measured on the same 20,000 documents, crash injected entering **Publishing**:

| | Before | After |
|---|---|---|
| clean build | 6.98 s | 6.93 s |
| recovery | 7.72 s (**1.11x**, a full rebuild) | **0.38 s (0.06x)** |

The report returned by the retry sets `interrupted_previous`, so an operator
can tell a resumed build from a fresh one.

### Run files are checksummed (1.0.209-beta)

Every completed run now carries a 48-byte trailer: sentinel, payload length,
SHA-256 of the payload.

- **Truncation** is caught in `O(1)` at open, before a single posting is read:
  the recorded payload length must match the file size.
- **Corruption inside a correctly-sized run** is caught by the digest, streamed
  as the merge reads — **no extra I/O**, because the merge reads those bytes
  anyway.

The magic header alone proved only that a file *started* as a run. A torn write
from power loss leaves a valid header above truncated payload, and the merge
consumed it happily. `BUILD_CHECKPOINT_VERSION` moved to 4, so a workspace from
before this refuses adoption rather than being read without trailers.

### Progress is reportable (1.0.209-beta)

`full_text_build_progress(index)` reads the durable checkpoint, so it answers
**from another process while the build is running** — the only time it matters:

```
phase                 tokenizing | merging_primary_key | merging_impact
                      | publishing | complete
documents_tokenized   documents through the scan
documents_total       collection rows (tokenizing only)
runs_written          sorted runs on disk
scan_cursor           where the scan resumes
percent               tokenizing only
```

`percent` is deliberately **absent outside tokenizing**. Only the scan has an
honest denominator; the merge phases work on intermediate runs whose size is
not a fraction of anything an operator would recognise. A fabricated
percentage is exactly the number someone would use to decide whether to kill a
multi-day build.

Remaining guards worth adding:

- Nothing outstanding on integrity or progress; see above.

## Operating a long build

Re-issue `CREATE INDEX` after an interruption; it resumes. Do **not** `DROP`
first — that is what discards the workspace and forces a full rebuild.

Poll `full_text_build_progress` to see phase and scan position. A run torn by
power loss is now refused rather than read, so an interrupted build either
resumes or reports which run is damaged.

## Fault injection

`BICDB_FTS_BUILD_CRASH_AT=tokenizing|mergepk|mergeimpact|publishing` fails the
build as it enters that phase. Test-only, and the seam the tests above use.
