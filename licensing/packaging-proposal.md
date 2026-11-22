# Packaging BicDB under Apache-2.0

BicDB is licensed under the [Apache License, Version 2.0](../LICENSE). This page
records how packages and artifacts carry the license and notices.

## Source of truth

- `LICENSE` is the verbatim, standard Apache License 2.0 text (hash-pinned in
  [transition.toml](transition.toml)); `LICENSES/Apache-2.0.txt` is an identical copy.
- `NOTICE` is the project attribution notice that Apache section 4(d) requires
  redistributions to keep.
- `THIRD_PARTY_NOTICES.md` lists components that keep their own licenses.

## Cargo

The workspace declares `license = "Apache-2.0"` and every package inherits it with
`license.workspace = true`. No package uses `license-file`. Each workspace member
ships byte-identical `LICENSE`, `NOTICE` and `THIRD_PARTY_NOTICES.md` copies so
they are included in its `.crate` archive. `scripts/sync-package-licenses.py`
regenerates the copies; `--check` verifies them without writing. `deny.toml`
needs no first-party clarification because every first-party package declares
an SPDX-listed license.

## npm, ABI, examples and Jepsen

`web/bicdb-client`, `abi`, `examples` and `jepsen` ship `LICENSE` and `NOTICE`
copies identical to the root files (listed as `package_paths` in
`transition.toml`). The npm client declares `"license": "Apache-2.0"` and lists
its legal files in `files`. A separately supplied engine WASM binary carries the
notices of that engine build.

## Binary archives and containers

Release archives and container images carry `LICENSE`, `NOTICE` and
`THIRD_PARTY_NOTICES.md`, plus any notices required by the exact third-party
components they contain.

## Source headers

Source files need no per-file header; the package license applies. New files
that carry an SPDX header use:

```text
SPDX-License-Identifier: Apache-2.0
```

`scripts/validate-licensing.sh` checks these invariants in CI.
