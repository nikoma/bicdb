# HA recovery integration review — November 13, 2026

This is a source integration checkpoint, not release or production
qualification. The verified delivered baseline at this checkpoint is
1.0.505-beta, `e25b3ba194c6f3ad73e2f393b68b175fbf1edd92`. The code described
below is an isolated successor batch. The original Azure expansion remains a
failed scale-out qualification; no replacement physical experiment has run.

| Finding or change | Evidence and classification | Remaining gate |
| --- | --- | --- |
| Native catalogue repair cohort | **Failed:** run 43561072006, source `a5911ec99d0564c011d916309bda64b6ae62a9dc`, 79 passed and eight failed. Seven failed at CLOSED historical re-observation; one at actual metadata-store contention. | Original failures remain retained; successor source must execute the same cases. |
| CLOSED historical scope and metadata contention repair | **Not tested:** frozen successor `f677436330c3158e5cd12f1ddcb8f465682ea419`. Independent source review found no confirmed blocker in the examined authority, exact native outcome and original-OWN wait paths. | Compilation, original cases, three added real-fixture controls and combined regression coverage. |
| Error evidence formatting | **Not tested:** f677 stops appending a diagnostic prefix after UTF-8 truncation while still hashing every subsequent chunk. The split-chunk regression is authored, not executed. | Existing required CI must exercise the regression. No data-loss effect is claimed. |
| Joined image final and native base | **Not tested:** source `12a74f59369eafb07d137ec1aa10f9889e62ef52` and `05d0427009d9517a5bb53d33993992f4e1c7aaa0`. Independent source review checked issued-xid/byte/outcome custody and exact committed CURRENT/archive relations. | Actual image, uncertain-outcome, cold recovery and second populated range cases. |
| Joined selected catalogue and materialized owner | **Not tested:** source `c8c04b38734808eaf82d736e0e26415f058e0f14`. Its same-candidate retry and known-success paths are distinct; ordinary durability guards remain strict. | Physical-lock callgraph review, compilation and real multi-range fixture. |
| Joined joint-continuation custody | **Not tested:** source `368fbcbf17cc4fbd98a377103f83a7d9dd10e3ff`, refined at `b811b56f65a6243f30f0ceba5029586e1f795a9d`. Host retains the same actual A before fallible postchecks; independently fresh joint authority is separate from expired snapshot authority. | Actual NEW observation, native decision, explicit release and final Serving producer. A custody getter is not a serving grant. |
| Remote decision reader without TLS | **Passed, narrow scope:** corrected source `2ac5c38ad8b0e39439d927053931776f1f09d2bb`, run 43566245411, one actual reader-compatibility unit passed in 0.05 s. It refuses an unsupported semantic profile. | No remote committed record, failover, physical or scale-out guarantee was tested by this unit. Original 7afb no-TLS compilation failure remains a failure with zero runtime cases. |

## Findings that must not disappear during integration

**Not tested for repeated movement; unsupported by this producer.** The initial remote OLD publisher source
`65a4cea3a16fd51bc2e22ed51277895e31359857` is limited to the actual INITIAL
actor and its immutable from-birth predecessor configuration. It does not prove
that a later received Joined range can become an OLD participant, or that the
same range can move repeatedly after its Serving policy has changed. The
immutable policy cannot be substituted for the actual historical Serving audit.
The next producer needs the real original State, protected predecessor audit,
native OLD closure, actual range exclusion and independently fresh authority.
It must not request effective ordinary Serving after closure.

**Failed source validation; runtime reproduction not tested.** Independent review also found that a remote reply could validate an OLD
decision receipt without explicitly correlating its node ID with the reply's
authenticated participant snapshot. The required deterministic negative control
starts with a genuine successful OLD reply, changes the receipt node to another
OLD member, recomputes the outer checksum and requires rejection. This is a
source protocol-validation defect, not evidence of an observed wrong publication
or lost acknowledged commit. Its correction must remain separate from the
original failed qualification evidence.
Source successor `d3b7242368329960274b67d7637a9a3063095ec5` adds that
explicit same-recipient association and the genuine-reply negative control.
The correction and new control are **not tested** pending combined CI.

**Failed source lock invariant; runtime reproduction not tested.** The Joined
materialized-owner and original Joined mutation-scope callgraphs require an
explicit borrowed-hook identity path. A full identity helper that reacquires a blocking hook
read while a Joined mutation scope already holds that read can deadlock behind
a queued writer. The required correction must reuse the actual borrowed hook
and original identity; dropping exclusion or weakening the identity check is
not an acceptable workaround. This is a source availability concern pending
correction and deterministic runtime reproduction, not a measured Azure cause.

## Integration and production gates

For an already installed Joined writer, the actual image and native decision
can feed the real CLOSED-domain producer. Subsequent joint preparation can then
retain that real CLOSED owner and original writer State. This avoids treating
the not-yet-serving new range as ordinary Installed Serving and avoids creating
a fictitious FIRST registration. The concrete NEW/decision sinks must still
retain original image/A proof and independently fresh authority.

Source review and formatting do not establish runtime correctness. None of
these changes establishes normal-controller integration, ordinary PostgreSQL
cross-range SQL, bounded physical failover, converged four-to-five-node
expansion or sustained throughput gain. Production admission remains unchanged:
zero lost acknowledged commits, correct read consistency and routing, bounded
fault-safe rebalance, and demonstrated sustained throughput gain after complete
convergence. Preserve every failed and uncertain operation in the external
ledger when the physical experiment is repeated.
