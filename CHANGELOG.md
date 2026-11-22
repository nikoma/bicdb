# Changelog

BicDB publishes per-release notes rather than a single accumulated file. Each
release has its own document under [`docs/releases/`](docs/releases/), named for
the version it describes.

- **Latest:** see the highest-numbered file in [`docs/releases/`](docs/releases/).
- **Current version:** the workspace version in [`Cargo.toml`](Cargo.toml) is
  authoritative.

Release notes begin at `0.9.34-beta`. Earlier history is in the commit log.

## Versioning

From `1.0.524` on, BicDB uses stable version numbers `1.0.x`; earlier
versions used `1.0.x-beta`. The patch component increments per shipped
improvement. The `bicdb` crate on crates.io and its `bicdb-*` dependency crates
are published together at the same version; the internal crates' APIs carry no
compatibility guarantee between versions. The documented
[current limitations](README.md#current-limitations) still apply.

BicDB is licensed under the [Apache License, Version 2.0](LICENSE) from
`1.0.523-beta` on.
