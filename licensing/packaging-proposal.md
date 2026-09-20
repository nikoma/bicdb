# Packaging BicDB License 1.0: Apache 2.0 + three exceptions

The simplified BicDB License is **active**. Its Apache baseline is verbatim;
this document now records distribution rules. LICENSE-SCOPE.md and transition.toml
identify the covered source revision and exclusions. No historical release is retagged.

## Cargo and mixed scope

The workspace uses `license-file = "LICENSE"`; 17 covered engine packages inherit
`license-file.workspace = true`. They do not declare an Apache alternative.
The 11 retained SDK/connector/example packages explicitly declare Apache-2.0.
The browser JavaScript client, ABI, examples and Jepsen harness retain their terms.
See [scope](../LICENSE-SCOPE.md) for the exact boundaries.

Each covered crate ships the community LICENSE, LICENSE-SCOPE.md, NOTICE,
THIRD_PARTY_NOTICES.md, LICENSES/Apache-2.0.txt and licensing/NOTICE-Apache.txt.
The latter two preserve earlier/underlying grants, not new Apache rights over the
whole covered package. Retained Apache packages keep their existing legal texts.
`scripts/sync-package-licenses.py` maintains this distinction; `--check` never writes.

Cargo includes the referenced custom license-file in archives and normalizes its
path. Check both the normalized Cargo.toml and the actual text inside packages.
No unsupported SPDX expression or “Apache WITH restrictions” is used to make tools
pass. The custom identifier is not represented as SPDX-listed or OSI-approved.
The appended Apache text must remain byte-identical to LICENSES/Apache-2.0.txt;
its presence is not a separate grant without the three exceptions for covered code.

`deny.toml` uses hash-pinned license clarifications and explicit package-specific
exceptions for the 17 first-party community packages. The dependency allowlist is
unchanged: a new third-party package does not inherit those exceptions. A license
change requires deliberate hash/config review, not a lower detection threshold.

## npm and separate components

The client package's JavaScript remains Apache. Its independently supplied engine
WASM binary carries the license of that engine build. If combining them, supply
both sets of applicable terms and notices. The current npm files list contains no
engine binary. Use `npm pack --dry-run --ignore-scripts --json` to inspect it.
A new custom-license npm package would use `SEE LICENSE IN LICENSE` and ship its
legal texts; none is introduced here. Jepsen's Apache link stays pinned to an
immutable Apache-era license. The root license does not overwrite vendor notices.

## Other artifacts and source headers

Binary archives, containers and installers must carry the applicable LICENSE,
scope, NOTICE, prior Apache and third-party texts. No current build/publish workflow
for those artifacts or active SBOM generator was found. Existing published assets
are untouched. A future SPDX SBOM should put the custom identifier/text in its
extracted-license information; OCI labels may identify LicenseRef-BicDB-1.0
with the shipped scope. Separate components still require separate license entries.

No blanket Apache source header was present on covered Rust implementation files.
Package licenses and scope identify them; preserve existing third-party source
comments. New covered first-party files may use this header:

```text
SPDX-License-Identifier: LicenseRef-BicDB-1.0
See LICENSE and LICENSE-SCOPE.md; earlier and third-party grants remain intact.
```

Independent applications need no UI branding. No runtime license checks, telemetry,
feature controls, frontend changes, release tags or package publication are added.

References: [Cargo license-file](https://doc.rust-lang.org/cargo/reference/manifest.html#the-license-and-license-file-fields),
[cargo-deny custom clarifications](https://embarkstudios.github.io/cargo-deny/checks/licenses/cfg.html),
[npm license metadata](https://docs.npmjs.com/cli/v11/configuring-npm/package-json/#license),
[SPDX custom license information](https://spdx.github.io/spdx-spec/v2.3/other-licensing-information-detected/).
