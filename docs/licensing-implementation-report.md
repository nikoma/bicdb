# Simplified BicDB License implementation

The operative license now reads **BicDB License 1.0 — Apache 2.0 + three exceptions**,
with identifier `LicenseRef-BicDB-1.0`. The complete Apache text is appended verbatim.
The short preface reserves only managed database services, commercial database
products/OEM, and included-console attribution removal for white-label distribution.

The same-license requirement for modifications and the rewritten grants, warranties,
patent and cure/reinstatement sections are removed. Apache section 4 permits
separate terms for modifications; distributing underlying BicDB still passes on
its complete license. Proprietary applications, unlimited SaaS, private changes,
consulting/support and community forks remain allowed. There is no application badge.

README, FAQ/scenarios, scope, NOTICE, package copies, commercial overview,
contributor guidance and active tooling use the new structure. The version/identifier
is distinct from Community License 1.0; earlier copies' grants are preserved.
No runtime code, dependency version, vendor notice, release tag or historical artifact
is changed. Existing permissive SDK/client/connector boundaries remain intact.

Focused verification checks the appended Apache text byte-for-byte, exactly three
numbered additional conditions, active metadata/scope, all 28 package legal copies,
18 scenario outcomes and licensing links. Package file lists, an actual no-compile
archive fixture, npm dry-run and cargo-deny verify tooling and distribution behavior.
The custom identifier is allowed only for the seventeen covered first-party packages,
with hash-pinned clarifications, never as a blanket dependency permission.

The owner authorized implementation and merging to main. The named licensors and
unspecified dedicated commercial contact remain as previously recorded; no ownership
assignment, patent, registered mark or contract is invented. Incorporation and
pass-through wording and actual licensing authority remain limited counsel review
items. The active license is source-available, not standard Apache-2.0 or OSI-approved.

Verification results: all 28 actual Cargo metadata entries and package file lists
passed; the no-compile `.crate` fixture contained the exact combined license and
scope/notices with normalized license-file metadata; npm dry-run contained all
client legal files (23 files total, no bundled engine). cargo-deny licenses/bans/
sources, repository boundary, shell syntax and diff checks passed. No engine build
or broad regression suite was appropriate for this license/documentation change.
