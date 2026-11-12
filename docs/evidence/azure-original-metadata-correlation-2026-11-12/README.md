# Original Azure metadata correlation — 2026-11-12 UTC

This is a read-only derivative of the five original stopped archives from the
failed **1.0.461-beta** scale-out trial, source
`b05385d4b033edf0db03f3eccf4bdd86f7c8eddb`. It checks whether the retained
metadata supplies a historical witness of the separately reproduced bounded
Append defect. It changes no original evidence and performs no database replay,
engine startup, network operation or cloud provisioning.

All five archive digests match the previously published [cold-catalog receipt](../azure-cold-catalog-reconciliation-2026-11-11/summary.json).
All five metadata state-envelope checksums verify. The archives were captured
at different times during sequential stopped-state collection; this is not one
coherent quorum snapshot or request-time routing evidence.

| Node | Current term | Committed/base index | Base term | Topology generation | Uncommitted suffix entries |
| --- | --- | --- | --- | --- | --- |
| 0 | 4 | 1812 | 4 | 8969 | 0 |
| 1 | 4 | 1820 | 4 | 9034 | 0 |
| 2 | 5 | 1825 | 5 | 9066 | 1 |
| 3 | 5 | 1826 | 5 | 9073 | 1 |
| 4 | 6 | 1826 | 5 | 9073 | 1 |

Only nodes 3/4 have the same positive committed index available for direct
comparison. Their committed topology hashes and base terms agree. Different
uncommitted tails alone are normal election evidence; they do not establish
conflicting committed histories. Different committed indices cannot be compared
as if they were identical snapshots.

| Finding | Classification | Evidence and limit |
| --- | --- | --- |
| Original archive custody | Passed | All five SHA-256 values match the retained original receipt |
| Metadata state integrity | Passed | All five compacted-state envelopes match their stored checksums |
| Comparable retained snapshot pair | Passed | Nodes 3/4 agree at index 1826, term 5, generation 9073; exact hashes are in `summary.json` |
| Bounded-Append defect caused the original failure | Inconclusive | These captures contain no equal-index conflict witness. They contain no complete intermediate history or request-time Append/reply trace. A transient falsely committed topology could be replaced before collection, as the independent regression demonstrates. Absence here cannot exclude an earlier occurrence. |
| Original two ACK records' request-time visibility/restoring event | Inconclusive | This metadata-only derivation supplies neither the missing read paths nor their first restoring event |
| Business-row contents, quorum-signature authenticity, physical RPO/RTO or production qualification | Not tested | No engine or client reads, signature verification, fault injection or timing campaign ran |

The later 300-second diagnostic status window is a separate post-restart capture.
Its term/generation observations must not be substituted for these earlier cold
files. The original [failed scale-out report](../azure-long-growth-failure-2026-11-11/README.md)
and all earlier reconciliation results remain unchanged.

The committed [`summary.json`](summary.json) contains only allowlisted counters,
original archive/member hashes and suffix descriptors. Full metadata members
remain in restricted custody; no private archive or credential is published.
The analyzer streams archive members in memory and never extracts them.

```sh
python3 scripts/analyze-azure-original-metadata.py \
  --archives /path/to/original/diagnostic-databases.private \
  --check docs/evidence/azure-original-metadata-correlation-2026-11-12/summary.json
```

The check was executed against all five original private archives. A deliberately
wrong receipt was also rejected before any output was produced. Re-running the
check requires access to those restricted archives; the public summary alone
cannot independently prove their contents or the historical execution.
