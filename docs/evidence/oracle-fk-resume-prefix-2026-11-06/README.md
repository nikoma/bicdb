# Foreign-key resume catalog-prefix reuse

Resume already validated the complete saved FK prefix against its initial
catalog inventory, then read every completed definition individually again.
The installation loop now begins after the validated prefix. Source/endpoint
identity, exact declaration ordering, complete catalog checks, enforcement
switches and before/after row fingerprints remain required. Writers and schema
changes must stay quiesced under the existing migration contract.

Pending entries retain individual verification. A missing key is read back after
installation; a matching unjournaled key can be adopted. A changed or missing
completed definition still fails before new DDL. No engine or application-name
handling changes are involved.

A native wire-level query trace reproduces four constraint-catalog reads for a
three-key completed prefix before the change and one complete inventory after
it. The same regression exercises a missing pending key, an already-installed
unjournaled key, changed completed definitions and row preservation. All 14
migration tests pass with no skips: five pure tests and nine native integration
tests. [Exact identities and log hashes](validation.json) bind the tool source
and the separately built compatibility server.

These are bounded query-count and correctness controls, not production workload
benchmarks. The running pristine import continues with its frozen earlier tools;
this checkpoint does not restart it or qualify the entire application/programme.

```sh
BICDB_ORACLE_SERVER=/path/to/oracle_server python3 -m unittest discover \
  -s examples/oracle/idempiere -p test_foreign_keys.py -v
```
