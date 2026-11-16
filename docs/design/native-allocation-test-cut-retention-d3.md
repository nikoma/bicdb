# Allocation test cuts: retain the actual selected boundary and original XID

This source-only correction starts at public **1.0.520-beta /
`d3af39c107951802a612d31b42e70fd4bc90e71c`**. Its live normal CI is unchanged.
The two findings below come from source inspection, not an observed CI failure
or a diagnosis of the earlier runtime timeouts. No Cargo/compiler, Rust test,
CI action, version bump, push or physical experiment was performed.

The existing [atomic allocation-publication component](native-allocation-publication-atomic-refusal-source-unit.md)
arms fault mode 4 before a normal allocation. The audit's pre-IO test selector
previously preserved only mode 2 and cleared every other value. Consequently it
cleared mode 4 before the later `compare_exchange(4, 0)` at the real held
publication-version boundary. The correction preserves modes **2 and 4** until
their original later producers consume them. Modes 1 and 3 remain consumed at
the pre-IO/native-fault boundary. No production retry, health setter or new
authority is introduced; a missing or expired health cut still fails the
unchanged strict assertions.

The same test-only audit previously assigned the latest attempted native XID to
any retained `AllocationCut` after a native call. A legitimate fresh worker
attempt after a mode-1 pre-IO cancellation can still retain that original cut,
while issuing its own real native XID. Updating the old cut would rewrite its
original XID 0 and could falsely fail the original pre-IO assertion. The new
stamp requires **this invocation's selected mode 3 and the retained cut's mode
3**. That cut is installed immediately before the corresponding native-fault
call, while the same allocation-attempt mutex serializes the original attempt;
the actual fault object remains local to that call and its fired count is
retained separately. A mode-0 retry cannot rewrite mode 1's original diagnostic.
Mode 2 still stamps its own actual Known-return cut, and mode 4 stamps its own
actual held-version cut. No XID, outcome, native IO count or deadline is invented.

Only `cfg(test)` statements/comments change. The release-code projection and
the whole file after reversing these two edits are compared against d3. All
existing control bodies, fault sites, live-window checks, first result/error,
candidate/Known/Unknown handling, cache/pending assertions, SDK two-second
window and native/Strong tails remain unchanged. The original source-only
atomic-leaf review remains historical; it did not establish that this selector
would deliver its intended cut. These corrections add no executed result.

The next validation is the unchanged normal control definitions on an exact
corrected candidate: mode 4 must reach its real held-version/live-parent/OWN
cut and return the exact atomic health refusal after a real Known XID; mode 1
must retain its original pre-IO XID 0 even if a later actual worker attempt
finishes. Existing native-fault mode 3 must retain its actual original XID and
Unknown/fault-count assertions. A timeout, an unreached cut or another error is
not accepted as the intended result. Standalone formatting/source comparisons
are not compiler, runtime, latency or HA qualification.
