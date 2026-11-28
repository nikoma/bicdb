# SXD-XPath source

This directory contains the XPath evaluator from
[sxd-xpath-no-unsafe 0.5.1](https://crates.io/crates/sxd-xpath-no-unsafe/0.5.1),
by Jake Goulding and Neil Soiffer. The crates.io source archive checksum is
`c0ff5bfe9c78786014d5a76d9328ea6508b0b74b7dc4a7c4d9b881ace23ce50d`.
The original [MIT license](LICENSE-MIT) and
[Apache License 2.0](LICENSE-APACHE) apply to this source.

BicDB embeds the evaluator in the published SQL crate so source and
crates.io builds both use fallible namespace resolution. The upstream safe
configuration is always enabled. The relevant changes are alongside the
vendored source in Git.
