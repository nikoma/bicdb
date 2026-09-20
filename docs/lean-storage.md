# Lean storage campaign — self-shrinking, auditable files

Goal: a LOT of data, files that stay proportional to live data without
operator babysitting. One PR per item, bump 0.0.1 each, mark with PR
numbers.

- [x] **L1. Autovacuum** (PR #510) — vacuum ticks wired into the pgwire maintenance
      loop beside the checkpoint ticker (same governor lanes, periodic +
      pressure triggers) + a `VACUUM` SQL statement driving bounded steps.
- [x] **L2. Space report** (PR #511) — `bicdb_space_report()`: per-store pages/free
      pages/file bytes, WAL bytes, event-log bytes, per-FTS-generation
      sizes; the operator surface for everything else here.
- [x] **L3. Hole-punch reclaim** (PR #512) — return interior free extents to the
      filesystem (FALLOC_FL_PUNCH_HOLE) as a bounded maintenance step;
      files go sparse without moving pages. Tail reclaim stays for the
      trailing case.
- [x] **L4. FTS fold hygiene** (PR #513) — automatic tail folding when the unfolded
      tail crosses a threshold, on the maintenance ticker.
- [x] **L5. Audit-log retention** (PR #514) — snapshot-then-trim for
      `bicdb.records` with a durable floor; mesh-safe (peers behind the
      floor are told to full-resync rather than silently diverging).
- [x] **L6. Archived-WAL pruning** (PR #515) — retention API honoring verified
      backup chains.
- [x] **L7. Compact value codec (opt-in zstd)** (PR #516; the record
      frame was already binary — compression captures the ratio win; a
      structural metadata codec remains a possible further step) — compact binary metadata
      encoding + zstd for large values, write-new-read-both (no-rebuild
      rule), measured before default-on.

Progress log: campaign started 2026-08-15.

**Campaign L1-L7 COMPLETE** (PRs #509-#516, 1.0.168 → 1.0.174-beta,
2026-08-15). The store is now self-shrinking: autovacuum + automatic
checkpoints/tail-reclaim + hole punching + fold hygiene run on the serve
loop; VACUUM / TRIM AUDIT HISTORY / backup prune-archive cover the
operator paths; bicdb_space_report measures all of it. Remaining named
follow-ups: page migration/defrag + whole-segment release (interior
compaction beyond punching), broker EventStream disk-backed reads,
structural binary metadata codec, spill-crash leak audit under the new
autovacuum.
