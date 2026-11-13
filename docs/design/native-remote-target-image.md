# Remote image target: source facts and target permission

This unmerged leaf composes actual main `e37c666660c2` (1.0.492-beta), the
reviewed completed-decision recovery candidate, and source-frame/OLD-step
leaves `1696b039`, `90241d80`, `a2243ff2`, `11414e3d`. No target row is installed
by this leaf. It has not yet been compiled or executed.

`distribution_range_image/remote_target.rs` introduces a distinct immutable
`NativeRemoteImageFact`. Its only constructor consumes the opaque result of
the actual pinned TLS/native-source transport, while the original Manifest
request remains live. An expected manifest DTO, checksummed number or source
path cannot construct it. It retains that actual transport object rather than
constructing `SourceRangeImageLineage`, a local certified source cut, or an OLD
quorum from the decoded manifest.

The manifest remains historical data after issue. It is not a claim that the
remote process or its pin remains live forever. Each history/rows/finish step
must separately come from the actual authenticated transport, match the same
issued manifest/source session/attempt and native identity, and retain its own
original bounded request. No network is performed by these data wrappers.

The existing genuine fifth-node source-frame fixture now consumes this type
for the actual history, rows and final seal. Its original duplicate-row and
full acknowledged-value assertions remain. An actual Rows response cannot
construct a Manifest fact. A genuine second-range frame cannot bind to the
first range's fact. These are unexecuted source assertions, not reported test
results. The original source-session-cancellation case remains unchanged.

The required target permission is deliberately separate. Actual target
Registered or Assigned ownership, original caller and fresh original role/OWN
observation must bind to the same protected MStore, DB open, Store slot, hook,
namespace IDs and owned Frozen/Installing state. Both row construction and the
actual transaction must recheck that permission. An absent local point is not
index zero: the first Registered lane needs a genuine selected-range no-domain
proof, followed by the actual retained Installing intent. An Assigned target
must preserve other populated ranges, their policy/state, history and all
outstanding commit/abort/no-data obligations.

Still required: the actual remote target row/transaction grant; bounded
history staging and copy; native verification/finalization; the resulting real
Image lease; NEW joint quorum/Open, policy continuation and request-specific
Strong readiness; a second range on that same already-serving target; crash
and restart recovery; automatic controller integration; and an isolated 8-GiB
four-to-five physical experiment with the complete external ledger. The source
frame fixture starts from a genuine **local** OLD quorum and must not be
described as a remote OLD collector or independent-host qualification.
