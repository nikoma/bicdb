# HA evidence successor: prepared docs-only delivery

This candidate starts from actual main **1.0.491-beta**,
`be388fdb20c53ea47ca57003aa7df9087a3ee0a1`. Its imported evidence and status
files are copied exactly from unmerged integration source
`07a96d4b6f7958711b48a60deb957aba508eace1`. The delivery contains documentation,
offline evidence verifiers and retained evidence bytes; it carries no native
runtime, application test, workflow or capability-policy change. Rust files
inside an evidence packet are historical source exhibits, not compiled inputs.

Version **1.0.493-beta** is a proposed delivery position, not a reserved or
published version. The owner must first complete the independent 492 merge,
then rebase this documentation candidate, confirm the next unused version,
perform the synchronized version bump and validate that final head. This
preparatory commit deliberately retains main491 version metadata.

## Revision-scoped results preserved

| Packet | Actual source and result | Boundary |
| --- | --- | --- |
| [Strong prerequisite](../evidence/native-strong-prerequisite-fourcase-2026-11-12/README.md) | `7b55059f…`, run43330054124: **0/4**, 78.27s; compile/consumer pass | All four fail before the requested CurrentJoint cuts. No later pass overwrites this failure. |
| [Original-origin HOT recovery](../evidence/native-hot-origin-recovery-2026-11-12/README.md) | `8334a99e…`: original two pass; `60c47ae2…`: six controls pass | Eight distinct tests; fourteen executions include one preserved earlier failure. All three recipients reachable, one Core process. |
| [Serving and public CLI](../evidence/native-serving-cli-focused-2026-11-13/README.md) | `ca8383ef…`: **9/0**, 10.43s; `5cec7687…`: **6/0**, 71.94s; both compile/consumer pass | Different sources and cohorts. Six business ACKs across three fixtures; 29 logical pairs, 141 actual attempts and 281 frames. No fifth DATA movement. |
| [Four-case passive diagnostic](../evidence/native-current-joint-fence-fourcase-2026-11-13/README.md) | `5812d575…`, run43343984328: **4/0**, 67.70s; compile/consumer pass | Whole workflow **failed formatting**. Diagnostic-only source does not establish a production correction for the earlier 3/1 timing failure. |
| [One unavailable peer](../evidence/native-hot-one-peer-loss-2026-11-13/README.md) | `ae584588…`, run43345593406: **0/1**, 21.74s; compile/consumer pass | Actual Unknown, two peer materializations, stopped n1 and surviving metadata QQ precede the failed required health gate. No next ACK. |
| [Genuine fifth assigned receiver](../evidence/native-registered-fifth-required-2026-11-13/README.md) | `a98c6a6b…`, run43343369839: **0/1**, 4.31s; compile/consumer pass | Both handoff fences return NoWriter while caller/role remain live. Two source ACKs and four original guards remain; target unchanged. Explicit Core assignment is not automatic relocation. |

The [current status](../ha-milestone1-status-2026-11-12.md) retains the original
Azure **FAILED** qualification and separates implemented, tested, qualified and
deployed states. No source repair currently awaiting execution is counted as a
passing test here. No new binary, deployment, physical 8GiB capacity result,
automatic four-to-five DATA convergence, cold DATA recovery, production
admission or transparent distributed SQL claim follows from this delivery.

## Offline checks on this prepared candidate

Run from the repository root with `PYTHONDONTWRITEBYTECODE=1`. These commands
read evidence/source objects only; they do not open an engine, access a network,
build a binary or execute the native acceptance tests.

```sh
python3 docs/evidence/native-strong-prerequisite-fourcase-2026-11-12/verify.py --repo /root/bicdb --self-test
python3 docs/evidence/native-hot-origin-recovery-2026-11-12/verify.py --repo /root/bicdb --tamper-tests
python3 docs/evidence/native-hot-one-peer-loss-2026-11-13/verify.py --tamper-tests
python3 docs/evidence/native-serving-cli-focused-2026-11-13/semantic_tamper.py
python3 docs/evidence/native-current-joint-fence-fourcase-2026-11-13/semantic_tamper.py
python3 docs/evidence/native-registered-fifth-required-2026-11-13/semantic_tamper.py
python3 docs/evidence/native-strong-prerequisite-trace-source-2026-11-12/verify_source_parity.py --repo /root/bicdb --candidate 7b55059fd610b40189afd239cf8e9ea7b2986ad4
git diff --check
```

All seven checks pass on this preparatory candidate. Six packet checks reject
**47 semantic mutations** (6+8+8+11+6+8); their implementations distinguish
direct semantic checks from tests that also repair manifest/derived hashes.
The source-parity check separately compares six exact historical Rust files
after removing only marked diagnostics. These counts concern offline verifier
controls, not additional native runtime tests or independent execution
authenticity. Native pages, signing material and private custody archives are
not republished by this delivery; packet-specific limits remain authoritative.

After the owner492 merge, the final delivery additionally requires the normal
version-bump/check commands and repository-required checks on its exact final
head. Reusing the retained native results must preserve their original source
identities; documentation/version validation is not a new native runtime pass.
