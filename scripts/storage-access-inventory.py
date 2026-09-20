#!/usr/bin/env python3
"""Inventory the resident-storage access surface in bicdb-core.

Phase 0 of docs/server-paged-storage-todo.md requires an inventory of every
direct access to `CollectionState.shards`, `RecordMap`, `VersionMap`, and
`IndexState`, grouped into point read, range cursor, snapshot read, write,
validation, maintenance, and recovery operations.

The point is not the document. The point is that this surface is exactly the set
of call sites a second storage engine has to satisfy, so its size and shape
decide whether Phase 2 can land incrementally or has to arrive as one
unmergeable branch. Generating it mechanically keeps the number honest as the
code moves.

Usage:
    scripts/storage-access-inventory.py                 # write the doc
    scripts/storage-access-inventory.py --check         # verify it is current
    scripts/storage-access-inventory.py --count         # print the site count
"""

from __future__ import annotations

import argparse
import collections
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SOURCE = REPO / "crates" / "bicdb-core" / "src" / "db.rs"
OUTPUT = REPO / "docs" / "storage-access-inventory.md"

# The accessors that hand out resident record state. Anything reaching the
# shards goes through one of these, so they are the boundary a page-backed
# engine has to reimplement.
ACCESSORS = [
    ".shard(",
    ".shard_by_rowid(",
    ".shard_mut(",
    ".shards_mut(",
    ".read_all()",
    ".write_all()",
]

# Operation groups, in the order the roadmap names them.
GROUPS = [
    "point read",
    "range cursor",
    "snapshot read",
    "write",
    "validation",
    "maintenance",
    "recovery",
]

GROUP_NOTES = {
    "point read": (
        "Resolve one or a batch of known keys/locators to records. The narrowest "
        "and most mechanical group: a page-backed engine satisfies these with a "
        "primary B+ tree lookup plus a heap fetch."
    ),
    "range cursor": (
        "Walk a key range, prefix, time window, or the whole collection. These "
        "are the sites Phase 5 must convert from 'materialize a Vec' to a lazy "
        "cursor pinning a bounded number of pages."
    ),
    "snapshot read": (
        "Read through MVCC visibility rules or inspect version chains. Blocked "
        "on Phase 2 putting tuple visibility metadata on disk."
    ),
    "write": (
        "Insert, delete, conflict-check, and stage index mutations. The group "
        "with the strictest correctness requirements — write-conflict, unique, "
        "and snapshot semantics must be identical across modes."
    ),
    "validation": (
        "Uniqueness checks, index verification, and protected-data evidence "
        "sampling. Mostly read-only, but must observe the same state a "
        "concurrent writer would."
    ),
    "maintenance": (
        "Compaction, checkpointing, index and vector rebuilds, statistics, key "
        "rotation. Phase 3 replaces whole-database compaction here with bounded, "
        "resumable page-level work."
    ),
    "recovery": (
        "Segment/WAL replay and replicated-write application. Phase 3's gate is "
        "that these become proportional to the post-checkpoint WAL suffix rather "
        "than to total database bytes."
    ),
}

# Enclosing function -> operation group. Every function that touches the
# accessors must appear here; an unclassified one is a hard error, so a new
# direct access has to be categorized deliberately rather than drifting in.
CLASSIFICATION = {
    # -- point read ---------------------------------------------------------
    "get": "point read",
    "get_unchecked": "point read",
    "get_by_rowid": "point read",
    "get_record_by_rowid": "point read",
    "get_records_by_rowids": "point read",
    "get_records_by_rowids_visible": "point read",
    "get_stored_by_rowids": "point read",
    "get_stored_by_rowids_visible": "point read",
    "get_records_by_pks": "point read",
    "get_records_by_pks_visible": "point read",
    "get_visible_unchecked": "point read",
    "registry_stored_by_rowid": "point read",
    "rowid_for": "point read",
    "rowids_to_pks": "point read",
    "record_system_metadata": "point read",
    "record_system_metadata_batch": "point read",
    "record_system_metadata_batch_with_presence": "point read",
    "shard_by_rowid": "point read",
    # -- range cursor -------------------------------------------------------
    "scan_collection": "range cursor",
    "scan_collection_unchecked": "range cursor",
    "scan_collection_record_ids_with_prefix": "range cursor",
    "scan_collection_record_ids_with_prefix_unchecked": "range cursor",
    "scan_collection_visible_unchecked": "range cursor",
    "scan_time_range_unchecked": "range cursor",
    "latest_value_per_device": "range cursor",
    "time_series_summary": "range cursor",
    "spatial_results_from_ids": "range cursor",
    "spatial_point_scan": "range cursor",
    "search_vector_with_metric_unchecked_cancellable": "range cursor",
    "search_vector_record_scan_with_metric_unchecked": "range cursor",
    "profile_vector_search_with_metric_unchecked": "range cursor",
    "profile_vector_search_record_scan_with_metric_unchecked": "range cursor",
    "vector_hits_to_results": "range cursor",
    "hnsw_vectors_for_collection": "range cursor",
    # -- snapshot read ------------------------------------------------------
    "snapshot_for_tx": "snapshot read",
    "read_committed_update_record": "snapshot read",
    "read_committed_delete_record": "snapshot read",
    "read_committed_delete_record_inner": "snapshot read",
    "read_committed_mutation_record": "snapshot read",
    "mvcc_debug_stats": "snapshot read",
    "chain_len": "snapshot read",
    "version_totals": "snapshot read",
    # -- write --------------------------------------------------------------
    "batch_delete": "write",
    "batch_delete_unchecked": "write",
    "apply_record_writes_to_state": "write",
    "apply_committed_tx_write_to_collections": "write",
    "prepare_mutations": "write",
    "prepared_index_mutations": "write",
    "delete_index_mutation": "write",
    "tx_index_mutations": "write",
    "fill_index_mutation_rowids": "write",
    "detect_commit_conflicts": "write",
    "repair_conflicting_delta_writes": "write",
    "dirty_ids": "write",
    # -- validation ---------------------------------------------------------
    "ensure_spatial_field_present": "validation",
    "verify_phi_blind_index_coverage": "validation",
    "verify_protected_data_blind_index_coverage": "validation",
    "verify_tenant_isolation_evidence": "validation",
    "sample_phi_ciphertext": "validation",
    "sample_protected_data_ciphertext": "validation",
    "verify_index_detects_missing_stale_and_wrong_entries": "validation",
    "record_mutations_maintain_indexes_without_rebuilding_unrelated_records": "validation",
    # -- maintenance --------------------------------------------------------
    "compact_collection_inner": "maintenance",
    "checkpoint_begin": "maintenance",
    "checkpoint_abort": "maintenance",
    "checkpoint_write_segments": "maintenance",
    "build_index_state_with_options": "maintenance",
    "rebuild_vector_store_shared": "maintenance",
    "materialize_graph_projection": "maintenance",
    "collect_table_statistics": "maintenance",
    "write_startup_snapshot": "maintenance",
    "load_paged_btree_index": "maintenance",
    "paged_index_state_from_rows": "maintenance",
    "rotate_phi_field_encryption_key": "maintenance",
    "stats": "maintenance",
    "residency_report": "maintenance",
    # -- recovery -----------------------------------------------------------
    "apply_replicated_upsert": "recovery",
    "apply_replicated_delete": "recovery",
    "legacy_tx_write_superseded_by_segment": "recovery",
    "open_inner": "recovery",
    "hnsw_vectors_from_state": "recovery",
}

FN_RE = re.compile(r"^\s*(?:pub(?:\(crate\))?\s+)?(?:async\s+)?fn\s+([A-Za-z0-9_]+)")
# Top-level `impl ... for Type {` / `impl Type {`, used to tell which structure's
# shards a call site is reaching into.
IMPL_RE = re.compile(r"^impl(?:<[^>]*>)?\s+(?:([A-Za-z0-9_]+)\s+for\s+)?([A-Za-z0-9_]+)")

# Structures whose `shard`/`shards` accessors index an ORDERED INDEX, not the
# resident record state. The roadmap counts `IndexState` in the inventory, but
# conflating it with record access would misstate the size of the Phase 2
# refactor, so it is tracked as its own surface.
INDEX_STORE_IMPLS = {"ShardedBTreeIndexStore", "BTreeIndexStore", "ShadowIndexStore"}


def collect():
    """Return {group: {function: [(line, accessor)]}} plus any unclassified fns."""
    lines = SOURCE.read_text(encoding="utf-8").splitlines()
    current = None
    current_impl = None
    per_fn = collections.defaultdict(list)
    index_sites = collections.defaultdict(list)
    for number, line in enumerate(lines, 1):
        impl_match = IMPL_RE.match(line)
        if impl_match:
            current_impl = impl_match.group(2)
        match = FN_RE.match(line)
        if match:
            current = match.group(1)
        for accessor in ACCESSORS:
            if accessor not in line:
                continue
            # `RowId::shard()` takes no arguments and returns the shard index
            # encoded in the locator. It is arithmetic on a u64, not an access to
            # resident state.
            if accessor == ".shard(" and ".shard()" in line:
                continue
            if current_impl in INDEX_STORE_IMPLS:
                index_sites[current_impl].append((number, current, accessor))
                continue
            per_fn[current].append((number, accessor))

    unclassified = sorted(fn for fn in per_fn if fn not in CLASSIFICATION)
    grouped = {group: {} for group in GROUPS}
    for fn, sites in per_fn.items():
        group = CLASSIFICATION.get(fn)
        if group:
            grouped[group][fn] = sites
    total = sum(len(v) for v in per_fn.values())
    return grouped, unclassified, total, index_sites


def render(grouped, total, index_sites):
    out = []
    out.append("# Resident-storage access inventory")
    out.append("")
    out.append(
        "**Generated by `scripts/storage-access-inventory.py` — do not edit by hand.**"
    )
    out.append(
        "Regenerate after changing `crates/bicdb-core/src/db.rs`; "
        "`--check` fails CI when this file is stale."
    )
    out.append("")
    out.append(
        "Phase 0 of [`server-paged-storage-todo.md`](server-paged-storage-todo.md) "
        "requires an inventory of every direct access to the resident record "
        "state, grouped by operation. This is that inventory."
    )
    out.append("")
    out.append("## Why this number matters")
    out.append("")
    out.append(
        f"There are **{total} direct access sites** across "
        f"**{sum(len(fns) for fns in grouped.values())} functions**, all reaching "
        "the per-collection shards through one of "
        + ", ".join(f"`{a.rstrip('(')}`" for a in ACCESSORS)
        + "."
    )
    out.append("")
    out.append(
        "This is the set of call sites a second storage engine has to satisfy. "
        "It is the gate on Phase 2: until these are expressed against a trait "
        "that both an in-memory and a page-backed implementation can provide, "
        "disk-resident rows cannot land incrementally — every partial step would "
        "leave the tree uncompilable, so the work would arrive as a single "
        "unreviewable branch."
    )
    out.append("")
    out.append(
        "The encouraging finding is that `CollectionState` already funnels every "
        "access through a small number of accessors rather than exposing "
        "`shards` directly. The refactor is mechanical rather than "
        "architectural — but it is 95 sites of mechanical, which is a real "
        "piece of work and should be scheduled as one."
    )
    out.append("")
    out.append("## Groups")
    out.append("")
    out.append("| Group | Functions | Sites |")
    out.append("| --- | ---: | ---: |")
    for group in GROUPS:
        fns = grouped[group]
        sites = sum(len(v) for v in fns.values())
        out.append(f"| {group} | {len(fns)} | {sites} |")
    out.append(f"| **total** | **{sum(len(f) for f in grouped.values())}** | **{total}** |")
    out.append("")

    for group in GROUPS:
        fns = grouped[group]
        if not fns:
            continue
        out.append(f"### {group}")
        out.append("")
        out.append(GROUP_NOTES[group])
        out.append("")
        for fn in sorted(fns):
            sites = fns[fn]
            locations = ", ".join(
                f"[{line}](../crates/bicdb-core/src/db.rs#L{line})" for line, _ in sites
            )
            accessors = sorted({accessor.strip(".") for _, accessor in sites})
            out.append(f"- `{fn}` — {locations} (via {', '.join(f'`{a}`' for a in accessors)})")
        out.append("")

    index_total = sum(len(v) for v in index_sites.values())
    out.append("## Index-store access (tracked separately)")
    out.append("")
    out.append(
        f"A further **{index_total} sites** shard an *ordered index* rather than "
        "the record state. They are listed apart from the groups above because "
        "conflating them would misstate the size of the Phase 2 refactor, and "
        "because they are already behind the swappable `OrderedIndexStore` trait "
        "— which is why Phase 4 (persistent secondary indexes) is the cheapest "
        "phase relative to its value: the seam it needs already exists, complete "
        "with a `ShadowIndexStore` for validating a new implementation against "
        "the proven `BTreeMap` one."
    )
    out.append("")
    for impl_name in sorted(index_sites):
        sites = index_sites[impl_name]
        fns = sorted({fn for _, fn, _ in sites if fn})
        out.append(f"- `{impl_name}` — {len(sites)} sites in {', '.join(f'`{f}`' for f in fns)}")
    out.append("")

    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if the doc is stale")
    parser.add_argument("--count", action="store_true", help="print the site count only")
    args = parser.parse_args()

    grouped, unclassified, total, index_sites = collect()

    if unclassified:
        print(
            "error: these functions access resident storage but are not "
            "classified into an operation group:",
            file=sys.stderr,
        )
        for fn in unclassified:
            print(f"  {fn}", file=sys.stderr)
        print(
            "\nAdd each to CLASSIFICATION in this script. A new direct access to "
            "the shards is a decision about the storage boundary, so it should be "
            "categorized on the way in rather than discovered later.",
            file=sys.stderr,
        )
        return 1

    if args.count:
        print(total)
        return 0

    rendered = render(grouped, total, index_sites)

    if args.check:
        if not OUTPUT.exists():
            print(f"error: {OUTPUT} does not exist; run this script.", file=sys.stderr)
            return 1
        if OUTPUT.read_text(encoding="utf-8") != rendered:
            print(
                f"error: {OUTPUT.relative_to(REPO)} is stale. "
                "Run scripts/storage-access-inventory.py and commit the result.",
                file=sys.stderr,
            )
            # Explicit UTF-8: the rendered doc contains em dashes, and piping
            # it through a subprocess in a latin-1 locale raised
            # UnicodeEncodeError instead of showing the diff — turning a helpful
            # "here is what changed" into a traceback.
            subprocess.run(
                ["diff", "-u", str(OUTPUT), "-"],
                input=rendered.encode("utf-8"),
                check=False,
            )
            return 1
        print(f"OK: inventory current ({total} sites).")
        return 0

    OUTPUT.write_text(rendered, encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(REPO)} ({total} sites).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
