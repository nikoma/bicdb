# Changelog

BicDB publishes per-release notes rather than a single accumulated file. Each
release has its own document under [`docs/releases/`](docs/releases/), named for
the version it describes.

- **Latest:** see the highest-numbered file in [`docs/releases/`](docs/releases/).
- **Current version:** the workspace version in [`Cargo.toml`](Cargo.toml) is
  authoritative.

Release notes begin at `0.9.34-beta`. Earlier history is in the commit log.

## Versioning

BicDB is beta software and uses `1.0.x-beta`. The patch component increments
per shipped improvement; it does not carry semantic-versioning compatibility
guarantees while the project is in beta.

BicDB is licensed under the [Apache License, Version 2.0](LICENSE) from
`1.0.523-beta` on. Earlier source versions carried the custom BicDB License 1.0;
version numbers and historical release notes are not relabeled by the
[transition](docs/licensing-transition.md).
