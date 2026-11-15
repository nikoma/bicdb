# Original host-poll bounds for remote handoff (source only)

This correction is based on local integration `9579d7f2d` of the reviewed
remote preparation/Open/release components. Public PR207 source7c does not
contain this remote consumer; this finding is not an explanation of the
original failed Azure experiment.

The original `poll_finite_remote_serving` checked its parent only at entry,
then consumed every retained phase using each RPC's independent original
server token. Canceling/expiring the actual poll could leave its work running
inside a still-live RPC window. A busy first queue could also repeatedly end
polling before other original phases were visited.

The correction retains a separate private poll-parent observation in each
taken ingress. All actual Prepare, Bridge, Open and Release effect requests
inherit that parent in addition to their unchanged original server, process,
Role and OWN bounds. Memory/final checks also observe the poll parent. The
original RPC token remains unchanged: reply waiting, original server-cancel
faults and Unknown records still use its original absolute deadline. No
mutation is replayed, uncertainty cleared, source freeze recreated or authority
minted by this additional cancellation binding.

Before each queue attempt the caller is checked and a DATA-only rotating cursor
advances. Busy/slow earlier phases therefore do not remain first on every next
poll. A taken operation is completed through its same queue even if binding or
execution fails; its original result/error is retained before the outer caller
postcheck. No queue is taken after the caller ends. This does not forcibly
preempt kernel IO, reconstruct a crashed owner or supply an automatic scheduler.

Two new normal-host/TLS control definitions select a real Prepare after actual
FIRST/native decision, catalogue certification and frozen membership:

- Cancel the actual host-poll caller after queue take/binding, while the
  original server, Role and OWN remain live.
- Wait for an actual short host-poll deadline at that same cut, while those
  original authorities remain live.

Both require the original server deadline to remain unchanged, the original
server not to be canceled, exact typed remote denial body and actor/effect
errors, a healthy actual native
owner, unchanged full native/WAL/catalogue observations and no new preparation
continuation. Failure to reach the actual cut is a failure, not a passing skip.
The existing helper uses a distinct five-second poll child for old cases, whose
already-created maximum-five-second RPC expires earlier; original RPC windows,
assertions and test wrapper bodies are retained. Only the new expiry case
requests a 100 ms host-poll window. No application ACK or Strong-read result is
inferred from these pre-effect controls.

Status: standalone formatting, source preservation and version checks only.
**Not compiled or executed.** Busy/slow multi-phase fairness, cancellation after
an actual native/drop effect, automatic control, cold recovery, repeated moves,
cleanup and the physical four-to-five qualification still need actual tests.
