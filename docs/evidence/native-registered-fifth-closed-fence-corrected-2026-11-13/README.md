# Registered fifth closed fence: corrected ten-case cohort passes

Exact source **`550c23d0458ab7a4e10af94d1cc625a7eb69bb89`**, package
**1.0.491-beta**, DATA protocol **29**; [run43352756475](https://github.com/nikoma/bicdb/actions/runs/43352756475).
Core **131647532734**: **10 passed, 0 failed, 0 ignored, 980 filtered, 22.45s**.
All-target/isolated-consumer **131647509029** and Formatting **131647508972**
pass; the focused workflow succeeds. Full raw logs, all ten exact executed names
and ten complete terminal reports are retained and rederived by the verifier.
This is ten distinct component tests, not a broad native suite or deployment.

The original required genuine-fifth two-fence test passes, followed by nine
separate negative/ambiguity controls. Every fixture first establishes two real
mTLS CommittedHandoff fences at Registered n4, for range1/2 epoch2 in committed
Cgen5 from n1/n2. The same target DB open, birth, protected metadata Store and
range Store slot remain; it has no INITIAL ranges or writer policy. Four
original source writer guards and two full external source ACKs are retained.
Across ten different fixtures there are twenty source ACKs and twenty successful
fence replies. These counts must not be interpreted as one cluster's ACK ledger.
The target's native points remain **UNKNOWN/None**, not native R0. Core B0 is
not a logical prefix, and the fences grant no APP, Strong, Ready or Serving
permission. No image rows are transferred.

The nine controls cover generic ingress/cold-context refusal; a genuine foreign
INITIAL DB; captured original caller cancellation; original empty-hook ABA;
Store-slot restoration; receiver stop; real fsync then original cancellation;
real write/IO ambiguity plus same-live exact refsync; and real durable native
invalidation. The Foreign case now proves both the target and actual foreign
DB unchanged, with full B/birth/open/schema/namespace/native-points/slots/rows/raw
catalogue inventories. Foreign and target births/opens differ; the actual test
also asserts distinct DB Arcs. The error must be exactly:

```text
cluster error: native range image: native recovery birth/bootstrap/local member binding mismatch
```

## Exact comparison with the preserved failed run

The [earlier da56 packet](../native-registered-fifth-closed-fence-2026-11-13/README.md)
remains **9 passed / 1 failed**, 17.87s, at its original source and run. That
Foreign case already had a valid target capability but its expected-error
assertion rejected the actual earlier birth/local-member refusal. It never
reached the foreign afterstate proof now present. Its failure is not erased.

Between da56 and this source, Git reports exactly two changed files: the test
child (**25 added / 5 removed lines**) and its source-audit document (**35 added
lines**). `source-correction.patch` and `source-comparison.json` preserve that
whole diff. No production runtime, deadline, worker, transport, quorum or
required-test assertion changes. All original two-fence/source-ACK/original-
writer checks remain. Optional `--repo` verifies this exact two-path diff
against the actual historical Git objects; without it the packet checks its
retained patch bytes and result consistency, not repository-wide ancestry.

## Raw and private custody

Core raw: **4,364,622 bytes**, SHA256
`204810d935bdb5c43d693eb0908376823c6d50d5c9eb2986d98accbb13a2b739`.
Complete compile and formatting logs and byte/source/job indices are retained.
EOF/BrokenPipe diagnostics remain in the logs; no zero-error or causal claim
is inferred from the passing test summary.

Restricted artifact **14005071824**: **2,446,401 bytes**, SHA256
`153b78bca27b1c6fc92052dc491465323ebd6d9bf5e9b07876dc2502354725c6`,
**1,043 ZIP entries**. Ten actual custody manifests verify **960/960 files,
29,152,656 bytes**, without missing files. All five metadata/service/DB/native-
journal guards were held in every capture; engine owners were alive, listeners
were not drained and cleanup reports no errors. This is guarded observed-state
custody, not atomic power-loss capture, native-page decoding, engine reopen,
WAL replay or post-crash reconciliation.

The ZIP contains native DB and mesh-signing files and is **not published**.
The public packet includes no private DB/signing/TLS payload. It keeps safe
logical diagnostics, aggregate custody hashes/counts and full raw job logs.
Optional private verification reads hashes and exact terminal/manifest bytes
without extracting or opening an engine. Verifiers establish retained-byte
integrity and internal consistency, not independent execution authenticity.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-registered-fifth-closed-fence-corrected-2026-11-13/verify.py --repo /root/bicdb
PYTHONDONTWRITEBYTECODE=1 python3 docs/evidence/native-registered-fifth-closed-fence-corrected-2026-11-13/semantic_tamper.py
# Restricted optional check: add --private-zip PATH to verify.py.
```

This passes the required **Registered closed-fence admission** prerequisite
only. Assignment is explicit Core consensus orchestration, not automatic
public relocation. No OLD closed DATA quorum, remote image stream, materialized
target, NEW DATA quorum, target ACK/Strong, automatic convergence, 8GiB capacity,
cold DATA recovery or independent-VM qualification is established. The original
Azure FAILED verdict and a98 required **0/1** remain separate and unchanged.
