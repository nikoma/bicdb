# Protected joint DECISION: retain actual OWN at fixture callers

Required CI run `43572675143`, native Core job `132380250977`, executed
candidate `d8115fedf13b0bd7107cfddd2664f2c6ea2f261f` (1.0.510-beta).
Its result was **273 passed, 47 failed, one ignored**. Twenty-two failures
shared `protected native DECISION requires actual fresh OWN/current-MStore
version`: 21 stopped at `complete_joint_tests.rs:598`, and one stopped at
`fifth_data_path_tests.rs:955`. The original failures remain failed evidence.
The [source index](native-joint-decision-owned-context-source-index.json)
pins their names, source blobs and original failure-log digest.

These fixtures called `NativeJointDecisionPublicationRound::publish_actual_member`
with a real recovery Role but without an OWN observation. That method creates
an admission whose `metadata_point` is `None`. The protected-init1 native
publisher correctly refuses that absence. The error does **not** establish
that a supplied MStore observation became stale: these callers supplied none.
It also does not establish an application commit loss. The cases stopped at
the common DECISION prerequisite, before their later activation/Open/release
or post-handoff acceptance assertions; their prior setup evidence is retained
at its actual scope.

The correction uses the existing `publish_actual_owned_member` entry point.
Each fixture keeps its actual original Role, uses that Role's business request
ID, and obtains a separate authenticated HostOWN through the **recipient's own
transport**. Acquisition and publication use a child of the original caller
capped by that same Role's deadline. The five-second acquisition ceiling is
unchanged; it cannot extend the Role or its original cancellation checks.
There is no retry loop, reissued Role, metadata-leader identity substitution,
new transaction, topology copy converted to authority, or manufactured point.

The existing owned publisher validates Role against OWN and captures the real
attached local MStore point outside service/engine guards. The real listener
attachment, protected Store/open identity, native publication-version gate and
quorum/native durability checks remain mandatory. Fixture metadata is created
through `initialize_native_owned`; the transport listener installs its actual
metadata-ingress registration. No production implementation or refusal is
weakened by this test-only caller correction.

The coherent source successor starts at
`9feb898176e2c9c58c7df509784077e538c4e330`. Existing test names, fault cuts,
assertions and budgets are preserved. Direct `rustfmt --check`, version
consistency and diff checks passed. No Cargo invocation, compilation, test,
workflow dispatch or physical experiment was run for this successor. Existing
required repository CI must execute the corrected batch. A later pass cannot
retroactively exercise the cuts skipped by the original failures, or qualify
operator scale-out, cold Joined recovery, throughput, RPO/RTO or deployment.
