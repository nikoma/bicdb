# In-flight event client: failed 34-case qualification

[Hosted run43538783755](https://github.com/nikoma/bicdb/actions/runs/43538783755) **failed: 33 passed, 1 failed**, with exactly34 selected and executed tests, none ignored, in12.39s. The whole run remains failed. Workspace/all-targets and workflow-consumer checks passed.

The qualified source was `91713c0fed694505086f5351a2b0df755b7c6ffa`, whose complete Core subtree is identical to runtime candidate `1ce471aa315b93f58c00ddce5c0577505df472ab` (`1.0.505-beta`, unmerged qualification source). Only the qualification workflow and memory guard differ. The fixed union is25 client tests plus9 existing RPC-evidence tests, executed serially. Original test assertions and request budgets were preserved.

| Finding | Classification | Actual evidence |
| --- | --- | --- |
| Whole focused cohort | **Failed** | 33/1; cleanup control's first error-detail assertion failed |
| Typed cleanup refusal | Partial observation within failed control | `RpcEvidenceFailure { may_have_dispatched: true }`; no cached success returned |
| Original callback error detail | **Failed** | API returned generic observer-fence text instead of the injected callback error |
| Cleanup seam | Independently reconciled observation | APP frames2, mock ACK1, callback faults1; validated ACK precedes canceled sibling's partial-response record |
| Later cleanup-control assertions | **Not executed** | Failure at first detail assertion; independent reconciliation does not turn the test green |
| Caller expiry, shared peaks, same/different-sink reentry, typed out-of-order uncertainty, prior client and RPC-evidence cases | **Passed within this cohort** | Other33 exact test results retained |
| Four guarded compile commands | **Passed** | Helper, list, all-targets, consumer: child/guard exit0; no74/75/caller interruption |
| Physical HA, native distributed commit durability, Azure expansion, production admission | **Not tested** | No corresponding experiment in this packet |

The first failing assertion is `event_client_sibling_cleanup_evidence_failure_cannot_hide_behind_actual_mock_ack` at source line379. Its complete pre-assertion observation retains the original RPC records, network counts, actual caller-window timing and API outcome. Source review identifies a cancellation/error-precedence path that can replace an already returned evidence failure, followed by another evidence emission that produces generic fence text. That review is separate from the observed failure; no replacement implementation or passing rerun is included here.

The original seven-text-file focused artifact is retained unchanged as `artifact14091141775.zip` (SHA-256 `2d87f9051135c77ec96e6dce81dfbfa9f388130de8d91b4b08786f9a7b6da8bc`). The complete workspace-pressure artifact, both full job logs in lossless gzip, requests, terminal metadata, source/qualifier diffs, exact list/results, raw observations and independent Root verification are also included. There are no database custody roots and no projection.

**Text-only does not mean frame-free:** the complete ZIP and logs contain serialized RPC evidence with raw JSON-frame byte arrays from the six loopback mock observations. `decoded-frame-inspection.json` accounts for all30 raw-frame fields and inspects every nonnull frame (14 complete/partial byte arrays), including decoded synthetic append values and topology replies. No inspected decoded bytes matched the bounded private-key/credential markers. This is not comprehensive secret detection. Synthetic payloads and configuration/TLS path references are not private-key bytes. These were public-repository GitHub artifacts under GitHub access/retention; locally restricted copies do not establish owner-only GitHub privacy.

These controls cover SDK/TCP mocks, process-local numeric envelopes and existing local RPC-evidence fixtures. They do not qualify physical hardware, failover, scale-out, production durability or deployment. The earlier SDK8352 result and paired PR182/synchronous505 counterexample are separate historical evidence. The separate original first-APP68 failure is not causally resolved.

Verify offline from this exported packet:

```sh
python3 -B verify.py
python3 -B negative_control.py
```

Verification uses Python's standard library only, checks exact hashes/source/list/results/pressure and decoded frame coverage, and does not open an engine, build Rust or access the network. The negative control alters an original ZIP byte in a temporary copy and requires rejection.
