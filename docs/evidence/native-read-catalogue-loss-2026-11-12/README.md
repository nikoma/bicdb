# Confirmed unsafe absence in the unmerged native read candidate

**Failed safety regression; production qualification remains blocked.**
This is a separate deterministic test, not the original Azure failure.
Source **1.0.487-beta**, 3d0041e09dab91100ffaed6c1cdee27696a62ee8,
at [run 43218099779](https://github.com/nikoma/bicdb/actions/runs/43218099779),
job 131182402124: zero passed, one failed, 846 filtered, 6.30 seconds.
All-target and isolated-consumer compilation passed.

The selected born-v2 enrollment/Strong-read implementation is **unmerged** and
absent from ordinary single-host main487. This result does not establish a
defect in that single-host mode or its encrypted backups. Main's legacy
distributed missing-catalog behavior would require a separate audit.

## Actual fault and paired result

Actual TLS elections and metadata quorums preceded an application ACK through
data voters n0 and n1. Native Apply delivery to n1 was withheld after its actual
CQ1 persistence; n2 remained clean at zero. Every original database/directory
owner ended before two copies were made from that same stopped state.

The intact copy had to return the complete acknowledged value before the
second copy was tested. Only the second copy's n1 range catalog was removed,
with its directory fsynced. Both cases used genuine cold E2 election,
fresh metadata authority, discovered-history recovery and the typed Strong
endpoint. No response, vote, term or recovery position was fabricated.
Old acknowledged leader n0 remained intact and offline.

| Case | Discovered command / recovery cut | Actual Strong result |
| --- | --- | --- |
| Intact stopped copy | ACK1 command recovered; range position 1 | Present, full expected payload |
| Same cut, only n1 catalog removed | No command discovered; clean-zero history admitted | **Successful absence at range position 0** |

Both routed to n2 at range epoch 2, topology generation 2, with final metadata
term 2/read index 3 and distinct first/final metadata contexts. Thus this is
not a stale route, allowed stale read or a verifier collapsing an unavailable
response into absence. It demonstrates unsafe reconstruction of authority
after deliberate catalog loss. It does not show spontaneous file loss,
permanent loss of every copy, physical power-loss behavior or Azure causality.

The exact record ID is tenant/one-real-ack-before-catalogue-loss-1.
The payload SHA-256 is
d524cc40d247220709df4ccdb6229cc84bccb53026a849a54ff1cdcfe0a0339d.
The command SHA-256 is
454fca1cabc5613b84cae21c6aad8c06bc642c249266e025e27f9d217e34aee1.
The deleted catalog SHA-256 is
2990cbcf324df56fee62ffb750242175515f7c380ee9c743d787703be6ffb6da.
Complete typed replies, challenges and contexts are retained in the artifact.

## Retained evidence and portable checks

The baseline-artifact.zip file is original GitHub artifact 13944064417, unchanged:

* Archive SHA-256: e2e114da50a02c2b625b9e445a98f6adb54fb64473b898e088aa0c065a0f0864.
* Stopped-original manifest SHA-256: 187a1daab5d6b1e4f7c047512239f5a89e2c7bd64db96c1301c2d42cb67fb073.
* Fifty stopped-original files, 803,250 bytes, all hashes/lengths and exact file set verified.
* Eighteen separately mirrored stage/verdict reports, plus source/toolchain/test logs.
* Actual original data and metadata roots only; no private TLS files or live
  intact/damaged case images copied. Directory structure is listed in the manifest.

From the repository root:

    python3 scripts/verify-native-read-catalogue-loss-evidence.py \
      docs/evidence/native-read-catalogue-loss-2026-11-12 \
      --expected-source 3d0041e09dab91100ffaed6c1cdee27696a62ee8

This verifies custody, the original CQ1 catalog record and recorded paired
outputs. It does not independently reopen the engine, verify quorum signatures,
replay recovery or qualify production. Inspect the genuine test implementation
in native_strong_point_tests/endpoint_tests/missing_catalogue_tests.rs;
the exact test filter is
native_event_endpoint_missing_catalogue_same_birth_restart_must_never_return_acked_absence.

## Open repair gate

Missing catalog must refuse before creating a blank replacement or publishing
a new writer policy. Genuine fresh bootstrap needs explicit private provenance
and durable catalog-before-policy ordering, with older readers fenced.
Repeated attempts must not turn a failed blank-file write into eligibility.
The intact positive control must keep passing. Bootstrap interruption,
post-policy/pre-hook recovery, normal cold reopen and reader-compatibility
controls are required. See the
[repair contract](../../design/native-range-catalogue-loss-bootstrap-audit.md).

The failing source and artifact remain immutable when a repair is added.
