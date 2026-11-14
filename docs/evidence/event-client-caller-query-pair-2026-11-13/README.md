# Caller-query scope: paired SDK regression evidence

The worker implementation in draft PR182 dispatches an append even when its caller's public `QueryMemoryScope` has already expired. The synchronous SDK505 baseline refuses before opening a TCP connection. Identical public-API tests and byte-identical workflows established this difference; neither run includes a repair.

| Source / hosted run | Whole focused result | Expired caller scope | Live control |
| --- | --- | --- | --- |
| `560d8a873e65a7931226a0e38fc460f38c058ae6`, test-only child of PR182 `b18fb02685de53a90529adff3eb429e5f0957aad` / [43532705713](https://github.com/nikoma/bicdb/actions/runs/43532705713) | **Failed: 1 passed, 1 failed**, 0.01 s | `Ok(true)`, 1 accepted TCP connection, 1 APP frame | `Ok(true)`, 1 connection, 1 APP |
| `8ec10012cf7aa9f746bc503fca104de1e896f418`, test-only child of frozen SDK505 `01308fb09512f02e0c61b740fc339382e44b3521` / [43532727697](https://github.com/nikoma/bicdb/actions/runs/43532727697) | **Passed: 2 passed, 0 failed**, 1.00 s | `Err(QueryTimedOut)`, 0 connections, 0 APP frames | `Ok(true)`, 1 connection, 1 APP |

Both selected and executed exactly the same two named tests. Both workspace all-target checks and workflow-consumer compile checks passed. The paired test file SHA-256 is `a7e9f0b65a5ea9db0c3cc699ff30eaca282add7353001cc1cc6349e24432fd80`; workflow SHA-256 is `48c255ae145e842d8e6667e915a951ebf170190ec9617b303cb08372ca7f4c47`. Each run was dispatched once. No failed result was retried or hidden.

The test creates the expired scope using a real elapsed `Instant` after fixture setup. The public `append` call reaches a real plaintext loopback TCP server that validates the original namespace, key and value and returns a mock ACK. Observations are emitted before assertions. PR182's new threads do not inherit the caller's thread-local query envelope; the original synchronous transport performs its checks on that caller thread. This is a **confirmed SDK regression**, with a **passed synchronous control**. It is not an actual database commit, native quorum, HA, physical scale-out, production qualification or deployed fix.

The other review concerns remain source-derived and **not executed by this pair**: retention of typed uncertain outcomes across later worker/refresh failures, sibling evidence-error precedence during cleanup, and wall-time bounds around blocking DNS/evidence callbacks. The original read-only review and pre-run receipts are preserved under `source/`; their pending/unexecuted wording describes that earlier snapshot. The authoritative paired outcomes are above and in `pair-terminal-verification.json`. The earlier SDK8352 error-preservation result (run43528329600, 12/0) is separate historical evidence, not another result of this pair. No causality is established for the separate original first-APP68 failure; the retained56f occurrence was a different request.

Each `original-focused-core.zip` is the complete unchanged four-text-file GitHub artifact, with its full selected-list and test output. Full Core/workspace logs are retained in lossless gzip, alongside source patches, workflow, requests and terminal metadata. There are no runtime database custody roots and no custody projection. These were artifacts of a public GitHub repository under GitHub access/retention rules, not owner-only private artifacts. `content-inspection.json` records bounded UTF-8 and literal private-key-header inspection of retained payloads; it is not comprehensive secret detection.

Run offline from an exported packet, using Python's standard library:

```sh
python3 -B verify.py
python3 -B negative_control.py
```

The verifier checks every retained file hash, original ZIP/member identities, exact source/run/list/results, complete network observations, healthy controls and compile gates. The negative control changes an original ZIP byte in a temporary copy and requires rejection. No repository checkout, network, engine open or Rust build is needed.
