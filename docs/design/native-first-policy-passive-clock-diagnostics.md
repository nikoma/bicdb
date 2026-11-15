# Passive first-policy and local OLD retirement diagnostics

This source-only unit is based on `8bbbb144f683f6442493610b94ef7e893b1441f7`.
No Rust build or test has run. It changes neither production policy behavior nor
the original tests, assertions, phase windows, native IO, errors or retries.
The immutable initial `ceced59da` trace incorrectly typed its DATA range as
`u64`; source inspection confirmed the actual `RangeId` newtype. Its corrective
child uses that existing type directly, with no conversion or authority change.
This was a source-found compilation hazard, not an executed compiler result.

Public `6de646beb603accf39358017d8e0c83c39c741b1` executed 375 Native passes,
37 failures and one ignored case. Fourteen failures returned QueryTimedOut from
the first Registered policy helper; one timed out on its fresh same-original
candidate retry; one large-history case timed out during local OLD observation
retirement. The actual install/native origin and local retirement paths remain
unchanged in the future base. Later replacement-listener routing and retained
Installed drain repair do not establish closure of these earlier failed calls.
The historical exact timeout substage remains unknown. The independent source
scope receipt is `native520-6de-registered-hot-future-scope-audit.json`, SHA256
`c5abcb11be1bd575726aed7cfc73dc281096908a1d087181e2bc3fc3932eda0d`.

Production diagnostic macros expand to the original expression or block, once,
without a closure. In test builds, the real root operation returns its original
Result before the trace flushes. Original error objects and native ambiguity
are returned unchanged. A panic leaves the root-return field absent; it never
becomes successful diagnostic evidence. Actual IO/stderr and joins are not hard
preemptible.

The thread-local trace retains at most 96 events, with an explicit omitted
counter. Error projection is a contiguous 768-byte UTF-8 prefix, with truncation
and formatting flags, original discriminant, and actual IO kind/OS code where
the error retains them. No records, payloads, per-range histograms or authority
objects are serialized. Native DATA contains only the real retained attempt
enum, native XID, registration/coordinator identity and actual DB open. The
latest native state and actual root result remain separate from event truncation.

Entry/return spans cover actual full admission validation, real install-table
busy refusal, fresh metadata read, origin preparation, coordinator/attempt,
native transaction helper/put expression, readback, publication, post-install
MStore validation, original listener drain and Closed bind. Issued XID and
candidate retention are recorded at their actual source cuts. Put-expression
timing includes original argument encoding. Native helper return describes its
actual API result, not a new WAL/fsync or outcome-resolution certificate. A
successful put alone cannot establish commit; NativeUncertain stays uncertain.

Return timestamps are sampled immediately after the original operation, before
error formatting. Parent, admission, ALL-release-phase, post-OWN and fresh
origin-OWN remaining times are signed, so overdue windows are visible. The local
ALL-release admission has no separate Role object: its Role clock is explicitly
unavailable. ALL-phase timing is the actual combined phase cancellation deadline,
which already incorporates its caller/member limits. The remote local-policy
backing reports its actual privately retained Role/OWN scalar deadlines and
leaves an unavailable separate ALL-frame deadline null. No clock creates scope,
authority, a renewed window, a caller token getter, or Ready permission.

Only bounded DATA is appended during guarded work. No JSON, stderr or full
record formatting occurs under the actual journal, metadata or short version
guard. Root flushing follows unwinding of its operation's guards. Error-prefix
formatting on a failed nested call is bounded and occurs outside the short
version guard. Test instrumentation still consumes time; measurements include
observer overhead and are not an uninstrumented latency qualification.

Local OLD retirement records its real original-source validation, actual
post-Open scope, current-member checks and table-removal point. Its phase can
precede actual release, so `all_phase_remaining_us` is a phase clock and never
an assertion that the members have released. A failed earlier
check does not become a table-retirement success. No new retirement proof,
local/remote quorum conversion, native replay or generic Unknown reset exists.
The next normal CI run must establish actual stages and clocks before a
validation-order or full-scope performance change is proposed.
