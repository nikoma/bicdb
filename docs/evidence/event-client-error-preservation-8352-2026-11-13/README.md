# SDK event-client error preservation

Exact runtime **`8352e3d6bcf30ca5d03576a0ef70f98cc4244fec`**, source
**1.0.500-beta**, passed hosted run
[43528329600](https://github.com/nikoma/bicdb/actions/runs/43528329600):
**12 selected / 12 executed / 12 passed / 0 failed**, 2.67 s, with 1,081 other
Core tests filtered out. Core job `132239531943` and workspace job
`132239499719` passed, including all-target compilation and the workflow
consumer without Core defaults. This is SDK unit and loopback TCP mock
coverage, **not HA qualification**, physical timing or application throughput.

The fix is independently based on main500
`a06e09336c70fecbc38232d677c330984959388f`; it contains no giant native
prototype integration. It retains a routed operation error when later topology
discovery fails. Earlier typed unknown-write outcomes retain their exact
identity fields across subsequent operation refusals, refresh errors and
request deadlines. It does not acknowledge a write, declare rollback, add a
new wire error encoding or change stable keys, retry counts, backoffs, routing
policy or deadlines. Existing optional RPC evidence retains the individual
operation/discovery exchanges; no new evidence authority was introduced.

The six new cases are:

- Append preserves the first actual peer refusal when discovery cannot reach a member.
- Get preserves its actual error when failed discovery exhausts the deadline.
- Discovery without an application operation retains its no-member error.
- A conflicting topology retains its original stop point and the primary operation error.
- A directly constructed typed unknown-write error survives a later operation refusal plus refresh failure or timeout, preserving sequence/transaction/incarnation/detail.
- With no preceding operation, error selection retains the discovery fallback.

The TCP cases use actual loopback sockets with **mock response handlers**. Their
assertions count received application/discovery requests and check the stable
append namespace/key/payload. They do not commit a database transaction or mint
native authority. The typed-error test constructs error values directly; it is
not proof of typed unknown-outcome encoding across a real cluster wire. No
standalone acknowledgment ledger or per-request frame journal was produced.
All six existing retry tests also passed, with their original timing and
acceptance assertions unchanged. Exact qualified names are in `request.json`
and the archived selected/executed logs.

Artifact `14085989657` is the complete original **42,672-byte** ZIP:

```text
9ead8d3d2f9f3f92fe97979b80ab3f348569f653d6e23fad37b3f59b8f8dd1c2
```

It has only `source-revision.txt`, `rustc.txt`, `selected-tests.log` and
`tests.log`. There is no runtime database/key custody, so no projection is
claimed: the reviewed original artifact is included unchanged. GitHub-hosted
artifacts/logs are governed by the public repository's Actions access and
retention. Inspection for literal private PEM markers is not a general secret
scanner or a production-credential assertion.

The packet retains GitHub metadata, the exact request, original artifact/member
hashes, complete compressed Core/workspace logs and the compressed, byte-preserved source-change patch.
A read-only `gh run view --log` Core download returned EOF; that fetch error is
retained separately from the successful direct job-log API fetch. No workflow
was retried to capture this evidence.

Verify offline using Python's standard library:

```sh
python3 -B /path/to/packet/verify.py
```

The verifier checks packet/member hashes, source and run identity, exact twelve
selected/executed names, all six new regressions, whole result and specific
Core/all-target/consumer steps. It does not run Rust, contact a service or open
a database. A tampered-artifact negative control checks refusal of modified
evidence, not database correctness. `SHA256SUMS.json` and `validation.json`
retain those checks.

This preserves earlier failed process/CLI first-ACK evidence unchanged and
makes no retrospective claim about its underlying cause. The qualified source
here is `8352`; later cherry-picks, version bumps, main delivery, release builds
and deployments are separate states and were not executed by this packet.
No failover, read consistency, rebalance, cold recovery, RPO/RTO, cross-range
SQL or sustained horizontal scaling guarantee was established.
