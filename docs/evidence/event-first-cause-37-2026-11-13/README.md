# Client first-cause evidence: compile failure and corrected 37-case pass

The corrected source passed **37 tests, 0 failed, 0 ignored**, with 1075
filtered out, in 12.48 seconds. Its separate workspace all-targets and
workflow-consumer checks passed. This is client/RPC-evidence regression
coverage, not database durability, HA, scale-out or production qualification.

| Observation | Original qualifier | Corrected qualifier |
| --- | --- | --- |
| Runtime source | `cd3660eafb3ea3c6ea5b15d3c8bd3e33d124e00f` | `0c9602c570083b2c5f26a4d98ac6ba2f9a672fb6` |
| Workflow source | `521243e597c2d833be840773acac728a85161969` | `6dd0313c7e41a276960c725978927fcfcc7cdd94` |
| Run | [43542004108](https://github.com/nikoma/bicdb/actions/runs/43542004108) | [43542846413](https://github.com/nikoma/bicdb/actions/runs/43542846413) |
| Core job | 132284483732 | 132287156501 |
| Workspace job | 132284456735 | 132287134837 |
| Result | Both failed compilation: two E0308 diagnostics each; **zero tests executed** | **37 passed**; workspace and consumer checks passed |

Both sources inherit 1.0.505-beta. The qualifier workflow and guard are
byte-identical between runs. Each qualifier's actual Core tree equals its
runtime source's Core tree. The corrected Core tree is
`091a94aac3f7f3ab8deb56710b0ee55c5dacdcc6`. Actual checkout hashes are
verified from all four complete raw logs, separately from API head metadata.

The original compilation failure came from two heterogeneous arrays of
references to `Result<ClusterDataResponse, _>` and `Result<(), _>` in a new
test. The correction compares their actual `Option<&Error>` values. It
continues to reject every success and require the exact error kind,
uncertainty and cause. Production code and the original 34 test bodies
were unchanged by that correction. The original run finished naturally;
it was not canceled or treated as a runtime failure.

## Actual exercised contract

The fixed union contains the unchanged 34 client/evidence cases plus three
new first-cause controls. All 37 actual full names match the selected list;
the hosted guard checked the fixed expected names. Raw Git object paths
and retained source bytes verify the unchanged files, the original parent
test file's exact prefix, and the absence of a production-code change in
the test type correction. This is more specific than counting a passing
suite with different source or selection.

The new controls use **actual local legacy TLS exchanges and synthetic
test-service protocol responses**, not committed database transactions:

- Callback failure plus caller cancellation before dispatch preserves its
  exact cause and reports no dispatch. Failure after receiving a complete
  synthetic `Pong` preserves uncertainty and rejects the response as success.
- A real second request fails its journal callback before dispatch while
  its producer holds the result. The shared fence already exposes that
  cause; another already-dispatched request raises uncertainty without
  replacing the cause.
- A deliberately panicking callback is caught by the test. Its static
  unwind cause is published before future dispatch is fenced. The raw
  panic line belongs to this **passing caught-unwind control**, not a failed
  test or database crash.

Retained frame bytes are complete test RPC frames, including length headers,
synthetic payloads and test identities. A decoded `Pong` or mock APP `Ack`
is not proof that BicDB committed or preserved a business transaction.
First cause means the first successful cause publication, not a total
wall-clock ordering of concurrent callback failures.

## Original failures and provenance limits

The [original 34-case runtime failure at 7e739749c](https://github.com/nikoma/bicdb/tree/7e739749c2487ed4bedb3780492cd71b2e40b592/docs/evidence/event-inflight-34-failed-2026-11-13)
remains an immutable separate packet. Its 33-pass/1-fail observation is
not overwritten by either run here. The original 521 run then failed at
compilation, so its intended 37 cases were not runtime-tested.

The corrected run's original [dispatch request record](dispatch-request.json)
was retained before its single POST. The old 521 pre-POST request/intent
body was **not saved**. [Old dispatch provenance](old-dispatch-provenance.json)
labels the reported CLI command and count as a reconstruction from Root's
tool transcript, not an original request artifact. API metadata verifies
the old event, source and attempt afterward. Neither record is an
independent exhaustive API audit.

The later coherent delivery candidate `99f1a05bce08ba0ad5f8a777a844317d6c7a6d2b`
has the same Core tree, retained as a **source-only** identity fact. This
packet includes no execution result for its separate broad CI. Source
identity does not by itself qualify a complete release or deployment.

## Retained checks

All four original ZIPs and four complete compressed raw logs are retained
unchanged, with whole-file/member digests and full terminal metadata. The
checker reads ZIP CRCs and text without extraction or database opening.
All seven guard logs are retained: old helper exit 0, old list/all-targets
exit 101, and four corrected guard exits 0. No guard interruption 75,
monitor failure 74 or omitted pressure sample is reported. Samples cover
**build/check/list only**; they do not measure runtime/cgroup pressure,
prove immunity to sudden OOM or explain other historical timeouts.

```sh
python3 path/to/this/packet/verify.py
python3 path/to/this/packet/verify.py --self-test
python3 path/to/this/packet/verify.py --repo /path/to/bicdb
```

Portable verification needs only retained bytes and Python's standard
library, not Git, network, BicDB or original absolute paths. `--repo`
additionally compares actual repository objects. Manifest consistency and
commit pinning are not independent origin authentication. Root's separate
original-artifact verification is retained with its precise scope.

The repository and upstream Actions evidence are public under GitHub's
access and retention rules. Local restricted copies do not make upstream
artifacts confidential. No database/private-key custody archive is included.
The literal key-marker scan is narrow, not exhaustive secret detection.
See [classification and next gates](issues.md).
