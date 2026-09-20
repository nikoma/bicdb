# BicDB licensing transition record

## Current terms

The current developer-facing model is **Apache 2.0 + three exceptions**.
The operative [BicDB License 1.0](../LICENSE), identifier `LicenseRef-BicDB-1.0`,
uses that structure with the complete Apache 2.0 text appended verbatim. It remains
source-available, not standard Apache-2.0 or an OSI-approved license.

The exceptions reserve managed database services, commercial database products/OEM,
and included-console attribution removal for white-label database distribution.
The old same-license requirement for modifications is removed. Apache's own section
4 permits different terms for the recipient's modifications, while applicable
conditions on underlying BicDB code remain. Apache grants, warranties, patent terms,
redistribution machinery and liability text are not rewritten. The previous custom
30-day cure/reinstatement and custom patent language are not carried into this version.

The full notice/scope follows [LICENSE-SCOPE.md](../LICENSE-SCOPE.md). Its seventeen
covered engine packages and eleven retained permissive packages are unchanged.
The browser client, ABI, examples, Jepsen harness, dependencies and vendor material
keep their separate terms. The client license does not relicense an engine WASM binary.

## Revision history and prior rights

- `6aadb6da6e32c0c73cef77bad96889e9aca9baa6`, source version `1.0.363-beta`, recorded
  the Apache grant. Apache-era source through `4b9d738046b067fdab1e93989d9d8545075fa6c1`
  remains the prior-code reference. This is not proof of every version's delivery.
- `4b9d7380` installed the first non-operative proposal for review.
- `5dc999de025d684af23b8878e367afbfa692ce41` first committed Community License 1.0,
  merged to main at `155c3659`. That distinct license is not silently rewritten.
- The new simplified license starts at `first_covered_revision` in
  [transition.toml](../licensing/transition.toml), following baseline `894c114c`.
  Its different identifier prevents two substantively different texts sharing a version.

Earlier copies keep their received grants. Nobody's terms change automatically.
Existing Apache permissions permit continuing earlier Apache code, including uses
reserved for newly covered rights here. The preserved Apache text and notices are
not a parallel unrestricted grant for newly covered BicDB rights. Independent
implementations remain outside the license; no guarantee against copying is made.

## Rights and contributions

The licensors and limits of their authority are stated in
[LICENSE-SCOPE.md](../LICENSE-SCOPE.md). This transition does not assert new
assignments or alter third-party rights. Earlier source and artifact terms
retain their historical meaning; a new source distribution does not itself
establish the terms under which every earlier copy was delivered.

The [prospective contributor agreement](../licensing/contributor-agreement.draft.md)
remains a draft requiring affirmative acceptance. It does not secure consent
from past contributors. Detailed internal provenance review records are retained
privately; their removal from this snapshot does not resolve outstanding rights
questions or change the operative license and scope notice.

For commercial inquiries, see [commercial licensing](commercial-licensing.md).

## References and verification

[Apache text](https://www.apache.org/licenses/LICENSE-2.0.txt) and
[Apache's modification FAQ](https://www.apache.org/foundation/license-faq.html#mod-license)
were checked. The recognizable baseline is preserved verbatim; the additional
conditions make the combined instrument a distinct license. Dify is inspiration,
not authority to license BicDB; its tenant-count rule and patent assertion are absent.

[Packaging rules](../licensing/packaging-proposal.md) cover custom license-file
metadata, mixed-scope archives and hash-pinned first-party cargo-deny exceptions.
[Implementation report](licensing-implementation-report.md) records the checks.
