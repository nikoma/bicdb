# Embedded wasm module: size audit and diet plan

*Measured 2026-07-26 against main (0.9.41-beta), rustc stable, wasm32-wasip1,
Binaryen `wasm-opt`. Re-measure after major SQL-surface changes; the method is
reproducible below.*

`bicdb-wasm` is the C-ABI reactor module the browser worker
(`web/bicdb-client`) instantiates over OPFS. This note answers two questions:
how big is it really, and where would further size come from.

## The headline numbers

| Variant | raw | gzip -9 | brotli -11 |
|---|---:|---:|---:|
| `--release` as built (with DWARF from `debug = "line-tables-only"`) | 114.5 MB | — | — |
| `--release`, `wasm-opt --strip-debug` | 22.4 MB | — | — |
| `--release`, `wasm-opt --strip-debug -Oz` | 19.3 MB | 6.3 MB | 3.8 MB |
| **`--profile wasm-release`, `wasm-opt -Oz`** | **11.3 MB** | **3.8 MB** | **2.7 MB** |

Take-aways:

- ~92 MB of the on-disk artifact is DWARF debug sections inherited from the
  workspace release profile. It must never ship; `wasm-opt --strip-debug` (or
  the `strip = "debuginfo"` in `wasm-release`) removes it losslessly for
  browsers.
- The `wasm-release` profile (workspace `Cargo.toml`) halves the real module:
  `opt-level = "z"`, fat LTO, `codegen-units = 1`, `panic = "abort"`. Exports
  are identical to the release build. Fat LTO is the interesting part — thin
  LTO leaves 15–20 KB derived impls (`sqlparser::ast::Statement::clone`,
  `PartialEq::eq`) duplicated up to 8x across codegen units.
- `panic = "abort"` is free on this target: stable wasm32-wasip1 has no
  unwinding, so panics already abort in the browser today. The
  `catch_unwind` in `bicdb-wasm/src/lib.rs` only ever catches in native
  builds of the rlib (tests), which keep using the `release` profile.
- The client fetches the module by URL (`web/bicdb-client/src/client.js`), so
  the wire cost is whatever the server negotiates. Serve brotli: **~2.7 MB**,
  the weight of a large JS bundle, cached thereafter.

Build + shrink:

```sh
cargo build --profile wasm-release -p bicdb-wasm --target wasm32-wasip1
wasm-opt -Oz --strip-producers \
  target/wasm32-wasip1/wasm-release/bicdb_wasm.wasm -o bicdb_wasm.wasm
```

`wasm-opt -Oz` is worth a further ~15% on top of what rustc emits and takes
under a minute. Trade-off note: `opt-level = "z"` optimizes for size over
speed. No in-browser benchmark exists yet; if interactive query latency ever
regresses noticeably, try `opt-level = "s"` (or plain `3`) in the profile
before blaming the engine — the LTO/panic/strip settings are speed-neutral.

## Where the bytes live

Code section is ~20 MB in the release build (25k functions). Attribution by
crate, from the wasm name section (script under "Method"):

| MiB | share | crate |
|---:|---:|---|
| 4.9 | 26% | `bicdb-sql` |
| 4.9 | 26% | `sqlparser` (parser 1.1, Display impls 0.5, rest AST derives) |
| 3.5 | 19% | `core` — fmt machinery, panic paths, drop glue |
| 1.4 | 7% | `alloc` — BTreeMap monomorphizations |
| 0.8 | 4% | `regex` + `regex_automata` + `regex_syntax` + `aho_corasick` |
| 0.7 | 4% | `bicdb-core` (with default features off it is genuinely small) |
| 0.5 | 3% | `serde_json` |
| 0.2 | 1% | `waken_snowball` + `rust_stemmers` (FTS stemming) |
| 0.2 | 1% | `sxd_document` + `sxd_xpath` (XML functions) |
| ~2 | 10% | long tail: hashbrown, serde, chrono, icu_collator, geo, num_bigint, … |

Plus a 3.4 MB data section: chrono-tz's timezone database, unicode property
tables (regex + ICU), sqlparser's keyword tables, pg_catalog seed strings.

The engine itself is not the weight — `bicdb-core` compiles to 0.7 MB because
its heavy dependencies (ort, rustls, zstd, osmpbf, mmap) are already opt-out
features and the wasm build turns them all off. The weight is the SQL surface:
sqlparser's AST (parsing, cloning, comparing, printing several thousand node
types) plus `bicdb-sql`'s execution and pg-compat catalog emulation.

## If it needs to be smaller still

Ranked by effort against expected return:

1. **Feature-gate SQL corners in `bicdb-sql`** the way `bicdb-core` already
   gates its native deps: `xml` (sxd), `fts-stemmers` (snowball), `regex`,
   `geo`, `collation` (icu_collator + data), `tz` (chrono-tz + its table).
   Roughly 1.5–2 MB of code plus a good slice of the 3.4 MB data section.
   Each gate should degrade at runtime with a clear error, per the
   `bicdb-core` feature doctrine. chrono-tz alternatively supports pruning via
   `CHRONO_TZ_TIMEZONE_FILTER` at build time if full removal is too blunt.
2. **Nightly, when justified:** `-Zbuild-std` with `panic_immediate_abort` and
   `-Zlocation-detail=none` attacks the 3.5 MB of `core` fmt/panic plumbing —
   typically another 15–25% on top of `wasm-release`. Costs a nightly
   toolchain in the build path; not worth it at today's 2.7 MB wire size.
3. **Not worth pursuing:** sqlparser's `visitor` feature contributes ~0 bytes
   after LTO (measured), so dropping it saves nothing. And sqlparser itself is
   load-bearing for the whole SQL surface — there is no lean-dialect option
   that keeps pg compatibility.

## Ground truth notes (learned the hard way)

- **The wasm target silently broke** between the Jul 21 artifact and #204: the
  Windows positioned-I/O split left `bicdb-page/src/pio.rs` with unix/windows
  arms only, and nothing builds wasm in CI. Fixed alongside this note with a
  wasi arm (seek-then-read — safe because wasm32-wasip1 is single-threaded;
  `std::os::wasi::fs::FileExt` is the right spelling once it stabilizes,
  rust-lang/rust#71213). A `cargo check -p bicdb-wasm --target wasm32-wasip1`
  somewhere in CI would have caught it.
- Anything in `target/wasm32-wasip1/release/` may predate the current tree by
  days; check mtimes before quoting sizes.

## Method

- Per-crate attribution: parse the code section's per-function body sizes and
  join against the `name` custom section, bucketing by the mangled symbol's
  crate segment. ~60 lines of Python, no external tools; `twiggy top` gives
  the same view if installed. `wasm-objdump -h` (wabt) for section totals.
- Duplicate-function detection: identical symbols with distinct `.llvm.<hash>`
  suffixes in the name section = same function instantiated in multiple
  codegen units; fat LTO / `codegen-units = 1` removes them.
- Compression numbers are `gzip -9` and `brotli -q 11` of the post-`wasm-opt`
  module, i.e. what a static file server with precompression would send.
