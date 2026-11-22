# BicDB licensing transition record

## Current terms

From source version `1.0.523-beta` on, BicDB is licensed under the
[Apache License, Version 2.0](../LICENSE) (`Apache-2.0`), unmodified. The root
[NOTICE](../NOTICE) carries the attribution notice. The custom BicDB License 1.0
(`LicenseRef-BicDB-1.0`, Apache 2.0 plus three exceptions reserving managed
database services, database-product/OEM sales and console white-labelling), its
scope notice `LICENSE-SCOPE.md`, the per-package scope copies and the
first-party cargo-deny carve-outs are retired. Every package, including the
packages that were previously excluded from the custom license's scope, now
declares Apache-2.0 and ships the same LICENSE and NOTICE.

The repository owner directed this relicensing. The last main revision carrying
the custom license is `previous_license_last_revision` in
[transition.toml](../licensing/transition.toml). The sections below are the
history of the earlier license and are kept unchanged as a record; they do not
describe the current terms.

## Earlier license (historical)

The earlier developer-facing model was **Apache 2.0 + three exceptions**:
BicDB License 1.0, identifier `LicenseRef-BicDB-1.0`, with the complete Apache
2.0 text appended verbatim. It was source-available, not standard Apache-2.0.
At adoption, its scope notice listed seventeen covered engine packages and eleven
retained permissive packages; the browser client, ABI, examples and Jepsen
harness kept Apache-2.0.

## Revision history and prior rights

- `6aadb6da6e32c0c73cef77bad96889e9aca9baa6`, source version `1.0.363-beta`, recorded
  the Apache grant. Apache-era source through `4b9d738046b067fdab1e93989d9d8545075fa6c1`
  remains the prior-code reference. This is not proof of every version's delivery.
- `4b9d7380` installed the first non-operative proposal for review.
- `5dc999de025d684af23b8878e367afbfa692ce41` first committed Community License 1.0,
  merged to main at `155c3659`. That distinct license is not silently rewritten.
- The simplified BicDB License 1.0 started at revision
  `a97028c4f3fb7b6d7e01618dca2434a848956621`, following baseline `894c114c`.
- `1.0.523-beta` replaced it with unmodified Apache-2.0 for the whole repository.

Copies received under an earlier license keep the grants they were received
with; this relicensing does not rewrite them. Historical release notes and
evidence describe the license in force at their time.

## Rights and contributions

The relicensing does not assert new assignments or alter third-party rights.
Contributions are received under Apache section 5; see
[CONTRIBUTING.md](../CONTRIBUTING.md). The earlier draft contributor agreement,
which was never adopted, was retired with the custom license.

## References

[Apache text](https://www.apache.org/licenses/LICENSE-2.0.txt). Current
[packaging rules](../licensing/packaging-proposal.md) describe how packages carry
LICENSE and NOTICE. The [implementation report](licensing-implementation-report.md)
records the earlier custom-license implementation.
