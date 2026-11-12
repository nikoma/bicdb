# Native Decimal predicate generation transition

Production implementation qualified here: `1c943d476516b3c4cb295ce873b881c9bc135fe6`.
Its 1.0.483-beta package identity is inherited from the unmerged ERP development
base. This is not a published version, final composed delivery, release or
verified deployment.

The real main482 creator omitted an acknowledged amount
`0.5000000000000000000001` from a partial unique index with predicate
`amount > 0.5`. Changing the shared comparator without rebuilding that physical
generation lets the upgraded reader miss the row and accept a duplicate.
The original failure is retained separately under
`native-decimal-predicate-upgrade-2026-11-12` in the public index delivery branch.

The correction fences old readers, persists an owned repair plan in the atomic
Native index catalog, builds and verifies independent physical generations,
and atomically publishes each replacement with its plan progress. All required
generations must complete, verify and checkpoint before the current comparison
certificate is published and a public Core handle returns. Embedded indexes are
reconstructed and verified. Newly conflicting unique keys refuse admission;
neither acknowledged row is deleted. Disabling secondary indexes or fsync
cannot bypass legacy admission. Interrupted builds resume from their verified
logical clock, while the existing monotonic clock check remains enforced.

The existing main482 Native validator permits partial predicates only on
B-trees and forbids partial exclusion indexes. Its exact function is unchanged;
see `decimal-supported-kind-contract-source-equivalence.json`. Other index
kinds do not require new predicate support to implement this transition.

At the source above, **12 focused tests pass**: three Core unit controls and
nine integrations. The ignored unit entry point is explicitly invoked by the
parent process-cut controls. Coverage includes eight real SIGABRT boundaries,
two sequential index publication cuts, four corrupt-plan cases, and **16 EIO
failures** across four catalog publications and write, file sync, rename and
directory sync. Every successful recovery retains the acknowledged data,
Native uniqueness and two subsequent clean reopens. Actual old fixtures cover
paged, embedded, multiple nested predicates and newly conflicting keys;
independent Native row inspection retains both conflicting acknowledged rows.

`old-reader/result.json` records an independent executable control: the new
reader repairs a copy of the actual old store, verifies it and rejects a
duplicate through two clean reopens. The actual old reader refuses the required
`decimal_predicate_comparison_v1` flag without changing any upgraded bytes.
The original closed old fixture remains unchanged.

Earlier failing receipts are retained: stage1 had four passes and the mandatory
upgrade positive failed; the expanded duplicate/fault controls exposed a
resume-clock defect, fixed at the qualified source. The initial additional
fixture creator had a compile error; that build ran no product tests.
Successful example-build receipts are builds, not test passes.

Full regression of the final ERP/SUM composition, final source identity,
backup/application acceptance, merge and remote-main verification remain
required. This evidence establishes neither physical power-loss durability nor
mixed-version cluster qualification. Fixtures omit an unrelated generated mesh
private key; original private fixtures remain preserved on the build host.

## Pre-recovery reader fence follow-up

Final review identified that a reader fence must precede Native page recovery,
which can replay index mutations before the later private rebuild. Source
`f0c780f4ccdf3699d92506f8217f4446b4039d39` moves that fence into preflight.
Its new process cut verifies that the fence is durable while all old paged
bytes and the definition catalog remain unchanged. All **13 focused controls**
pass, including nine process boundaries and the existing 16 catalog I/O faults.
The independent old-reader executable control also passes at f0. See
`pre-recovery-fence-f0c780f4/summary.json`.

The previous a7/1c revision completed the full Core unit suite (537 passes) and
all 125 integration targets. Required helper setup failures and the exact
source-identical resume are preserved under `previous-a7-complete-core-broad`.
A full suite on actual f0 is active separately; earlier broad passes are not
attributed to the ordering change or to the future ERP/SUM composition.

## Completed Core regression on f0

The full Core run on actual `f0c780f4ccdf3699d92506f8217f4446b4039d39`
completed successfully. The required chaos, durability and HA-state helpers
were built from that exact source before the run. All 125 integration targets,
the full unit suite and the explicit doctest command completed; exact counts
and per-target results are in `final-f0-complete-core-broad/summary.json`.
This result supersedes the earlier pending observation above. It does not
qualify a future ERP/SUM composition or a published binary/deployment.
