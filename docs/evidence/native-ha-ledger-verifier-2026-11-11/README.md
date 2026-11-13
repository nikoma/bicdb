# Independent native HA ledger verification

The original artifact verifier could accept a missing acknowledged row when a
modified snapshot declared zero expected acknowledgements. A synthetic control
recomputed the rowset hashes and observed counts, left the real ACK ledger
intact, and changed the snapshot counters. The verifier incorrectly accepted
that fixture. The original live campaign itself reconciled all ACKs before
saving each snapshot; this finding is a weakness in independent evidence
verification, not evidence of a lost native write.

The corrected verifier derives each snapshot's required ACK set from the
fsynced SQLite ledger's before/successor/after phases. It checks full phase and
voter coverage, cross-checks snapshot counters, verifies attempted values and
phase boundaries, rejects known read-only refusals, and verifies all file
hashes. It does not open BicDB. Error replies with uncertain commit outcomes
are allowed to have a matching attempted row; they are not counted as ACKs or
treated as proven rollbacks.

Five semantic controls retain the original assertions:

| Fixture | Required result | Original verifier | Corrected verifier |
| --- | --- | --- | --- |
| Unchanged completed artifacts | Accept | Accept | Accept |
| Missing ACK with zero declared expectations | Reject | **Incorrectly accepted** | Reject |
| Changed acknowledged value, hashes recomputed | Reject | Reject | Reject |
| A recorded read-only refusal appears in a rowset | Reject | Reject | Reject |
| An attempted write with an uncertain error reply appears | Accept | Accept | Accept |

The complete raw control receipts, original and corrected verifier code, and
the control generator are preserved. The frozen 35-file fixture contains the
two SQLite client ledgers, 24 saved rowsets, eight paged format descriptors and
the original result record. It contains no native database files. Each input
file is hashed in `fixture-manifest.json`.

Replay with Python 3.11 or later and a fresh output directory:

```sh
python3 replay_fixture.py /tmp/bicdb-ha-verifier-replay
```

The replay checks every archived input hash, preserves `original-result.json`,
and changes only filesystem paths in the working result record. It runs the
corrected independent verifier and all five controls without starting BicDB.

This fixture is historical source
`92b8456d795f7576636a1b10269fe2336af92a08`, version **1.0.457-beta**:
80 ACK values, 24 rowsets, 3/5 voters and 456 recorded read-only refusals.
The corrected independent verifier passed on those original artifacts. These
results are **not** current 471 application, HA, VM, physical power-loss, TLS,
cross-host or production qualification. A separate observer applies the
stronger verification and controls after the fresh 471 HA campaign finishes.
