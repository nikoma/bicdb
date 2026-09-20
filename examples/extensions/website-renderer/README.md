# BicDB website renderer example

This import-free WebAssembly extension renders an immutable website JSON
release selected by BicDB. It cannot bind a port, open a socket, read a file,
or access Tokio. The native BicDB host owns all of those resources and passes
the renderer only a bounded request plus the active release.

Build:

```bash
rustup target add wasm32-unknown-unknown
cargo build --release \
  -p bicdb-website-renderer-example \
  --target wasm32-unknown-unknown
```

Output:

```text
target/wasm32-unknown-unknown/release/bicdb_website_renderer_example.wasm
```

Install that file through `ExtensionPackageStore`, use its returned SHA-256 in
`CREATE EXTENSION ... FROM MODULE`, and activate it. The complete installation,
website publication, rollback, runtime synchronization, and listener examples
are in [`docs/extensions.md`](../../../docs/extensions.md#versioned-websites).

`site-v1.json` is a deliberately clumsy 1999-style `0.1.0` release and
`site-v2.json` is the modern `1.1.0` release. The runnable
`bicdb-website-host-example` publishes both and provides host-owned POST
controls that atomically switch the active BicDB release in either direction:

```bash
cargo run -p bicdb-website-host-example
```

Those control routes are intentionally public for the demo. Authenticate
deployment controls in a real host. The renderer treats each release's `html`
as trusted administrator-authored HTML; sanitize untrusted content before
publishing it.
