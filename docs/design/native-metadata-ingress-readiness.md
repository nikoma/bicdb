# Native metadata scheduling and actual incoming ingress

The unmerged `a4bc6a324` origin cohort failed 7/4 in
[run43362317386](https://github.com/nikoma/bicdb/actions/runs/43362317386).
The native-CQ2/native1 case reached its actual unknown-write and listener-loss
prerequisites, but did not reach the completed-command materializer. The paused
node's original metadata supervisor could still campaign and send outgoing
heartbeats. Retained actual bootstrap replies changed from leader n0 to n2,
whose incoming listener was closed. The origin remained at its earlier native
point with a pending command and could not acquire the authority needed to
finish recovery. This is a component availability failure, not ACK loss or
evidence establishing the cause of the original Azure failure.

Test-only `ce3cb18475b9130fea9af61ff3914e70bd42d1c4` isolates that condition:
three actual born-enforced stores and pinned TLS, ordinary votes elect n2,
then only its incoming listener is stopped. Its original worker and actual
service/metadata/database owners remain. The required result is a reachable
leader and fresh metadata quorum on n0/n1. Rebinding the same owner must allow
the *original* n2 worker to win a later real election without token replacement.
This source was dispatched separately and its outcome must remain separate
from the corrective successor. The original twelve-second fixture election
bound and request budgets are unchanged; these are not physical RTO results.

Runtime `f7076c217` adds a necessary local scheduler refusal. The actual service
open owns a bounded weak registry of actual accept-loop registrations. Every
registration binds the attached metadata Arc and revocable current range-store
slot. Only the actual accept thread marks start. Handle stop/Drop and accept-
loop exit/unwind irreversibly revoke it; a late thread start cannot revive a
revoked registration. Old and replacement listeners have distinct registrations.

Before a metadata scheduling round, the protected owner/StoreUUID is still
validated through the original snapshot. If no matching actual ingress is live,
the worker stops issuing new votes/replication, relinquishes its current role,
and postpones campaigning. The supervisor and DATA process are not canceled.
A same-owner rebind can therefore resume that same worker and original process
lifetime. No diagnostic status, listener bit or registry count becomes a vote,
quorum, lease, role, data pin or serving grant. No wire or durable format changes.

The check is at round entry: shutdown racing an already-running round may allow
that original bounded round to finish before the next check. This is not an
instantaneous per-RPC fence. A locally live listener is also **not** proof that
remote peers or clients can reach it. Externally asymmetric firewall partitions
need their own required tests and protocol treatment; this local fix must not
be advertised as resolving that entire failure class.

At this documentation cut, static source review, formatting and version checks
pass. Corrected runtime execution, the unchanged native-gap acceptance case,
full metadata regressions, repeated recovery, actual data relocation and physical
qualification remain required. The experimental candidate composes current main
1.0.493-beta (`7c00dd50190ff789ea25cb5a0aea7d3bf37f7b9e`); it is not merged,
released or deployed.
