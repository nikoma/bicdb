# BicDB application ABI v2 artifacts

This directory is the versioned, public artifact bundle for
`bicdb_extension::abi_v2`, the sole source of truth for the BicDB application
ABI. Downstream compilers consume the Rust types directly or vendor an exact
snapshot; they must not maintain wire-compatible shadow structs.

`application.wit` is the component-model projection of the transport. The
current core-Wasm lowering imports exactly one function:

```text
bicdb:app/host.call(i32 request_ptr, i32 request_len,
                    i32 response_ptr, i32 response_capacity) -> i64
```

The high 32 bits of the result are transport status (`0` success, `1` buffer
too small, `2` protocol error) and the low 32 bits are the response length or
required capacity. The payloads are canonical JSON encodings of the Rust
`HostCall` and `HostCallResult` types. Guest modules export
`bicdb_extension_alloc`, `bicdb_extension_dealloc`, and
`bicdb_extension_invoke` as documented in `docs/application-runtime-abi-v2.md`.

The `compatibility/v2` corpus is normative. Every accepted case must decode
and validate; every rejected case must fail at the phase named in `index.json`.
Files marked `canonical` must round-trip byte-for-byte after trimming the final
newline.
