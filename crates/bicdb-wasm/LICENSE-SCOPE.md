# BicDB License 1.0 — operative scope notice

This notice accompanies LICENSE and identifies the Covered Software and Licensor
under that license. The BicDB License is **active**, not a draft or a promise
of later adoption. The project identifier is `LicenseRef-BicDB-1.0`.

## Licensor and authority

WalkNorth, Inc. and Nikolai Manek, each solely to the extent of rights owned or
authorized to license. These are the existing BicDB attribution names. The
repository owner expressly directed immediate activation on 2026-09-08. That
instruction is the recorded adoption authority; this notice does not represent
that an independent chain-of-title investigation or signed assignments are complete.
No grant on behalf of an unauthorized rights holder is made.

## Transition boundary

The first source revision carrying this simplified license is
`a97028c4f3fb7b6d7e01618dca2434a848956621`, recorded in `licensing/transition.toml`. The previous
Community License 1.0 began at `5dc999de025d684af23b8878e367afbfa692ce41`.
This is a new license version/identity; it does not rewrite earlier copies' terms.
These terms apply to the covered work as distributed from that activation revision
and subsequent revisions carrying this license. No historical version or artifact
is relabeled, and the existing source version number is not a retroactive boundary.
No package release, tag, or effective date for an earlier copy is asserted here.

## Covered software

Except for the exclusions below, the first-party BicDB source, its modifications,
binaries and associated documentation/support material in this tree are covered.
The covered Rust packages are:

- bicdb-core, bicdb-page, bicdb-sync, bicdb-sql, bicdb-pgwire, bicdb-resp;
- bicdb-wasm, bicdb-analytics, bicdb-bench, bicdb-app-runtime;
- bicdb-cell, bicdb-cell-admission, bicdb-cell-device, bicdb-cell-grant,
  bicdb-cell-ha, bicdb-fleet, bicdb-cli.

These names also identify the covered packages when separately packaged with a
normalized directory layout. The included first-party administration frontend for
exception 3 is `crates/bicdb-cli/src/tui.rs`, including its compiled
form. No independent application dashboard, website, or headless service becomes
an included BicDB frontend merely by using BicDB APIs. No web UI is added.

## Separately licensed exclusions

The following first-party components retain Apache-2.0, with their own applicable
notices and third-party exclusions:

- `crates/bicdb-extension` (the whole mixed SDK/host package);
- `crates/bicdb-blob-s3` and `crates/bicdb-provider-*`;
- `web/bicdb-client`, `abi/`, `examples/`, and `jepsen/`.

Dependencies, vendored code, third-party-derived inputs, and their licenses/notices
retain their independent terms. Historical release/provenance records retain their
historical meaning. External proprietary platform/integration software remains
outside this scope and is not licensed by this repository.

The Apache JavaScript client does not relicense a separately supplied engine WASM
binary. A combined distribution must carry the terms/notices applicable to each.

## Existing grants and notices

Earlier Apache-licensed copies and code remain usable under any grants already
received. This includes copies provided privately. The owner reports the repository
has not been publicly open-sourced; this is not a representation that no private
recipient ever acquired rights. The license cannot withdraw existing grants or
prevent independent implementations. It applies only to lawfully reservable rights
in the covered work and modifications; underlying permissions remain intact.

The Apache-era source through `4b9d738046b067fdab1e93989d9d8545075fa6c1` is the
prior-Apache reference. `LICENSES/Apache-2.0.txt` preserves its license text and
`licensing/NOTICE-Apache.txt` preserves its attribution notice. These are not an
alternative Apache grant for newly covered material. Retain applicable attribution
and third-party notices on distribution, including the versions shipped in packages.

Commercial authorization must be a separate written agreement with the relevant
authorized rights holder. No dedicated commercial contact or notice address has
been supplied; this omission does not grant reserved rights or postpone these terms.
