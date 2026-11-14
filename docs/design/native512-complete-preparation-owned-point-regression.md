# Native512 preparation fixture: actual MStore point

The required native run `43581326157`, job `132406489716`, failed with
280 passed, 43 failed and 1 ignored. Eighteen cases failed at the shared
`complete_joint_tests.rs:681` preparation prerequisite with:

> protected native joint publication requires an actual current-MStore version captured outside engine locks

The fixture had a genuine recipient OWN observation. Its generic committed
decision observer supplied `None` for the attached MStore point; the later
protected native preparation correctly refused. This was missing context,
not evidence that an actual supplied point was stale.

The fixture now captures the actual point from the same service and original
OWN **before** taking its service latch, then passes both to the explicit
observer. The related unchanged-OLD-writer negative control supplies the same
real prerequisite and keeps its exact descriptor-refusal assertion. Original
role/session ownership, request budgets, fault assertions and APP/Strong
acceptance remain unchanged. Production guards and the legacy wrapper remain
strict.

The original eighteen cases reached native decision publication and metadata
staging, but failed before their later named controls and final APP/Strong
acceptance. No precontrol acknowledgment count is inferred here. The other
25 failures are retained separately: ten recovery-role scope/expiry refusals,
two hot-path timeouts, and thirteen original Registered hook/open refusals.
Their causes are outside this leaf.

[The source index](native512-complete-preparation-owned-point-source-index.json)
retains all 43 original failure identities, exact errors, source and log hashes.
The original failed run stays failed. Direct formatting, diff and version
checks passed; this source repair has not been compiled or executed. It is
not an HA or scale-out qualification result.
