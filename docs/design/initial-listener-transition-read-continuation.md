# INITIAL readonly continuation across listener transition

Source baseline: `326c363477477950f76bdb51fe6425cab85dc362` (candidate 520).
This independent source component has not been compiled or executed. It does
not change a live candidate, source version, workflow, or test deadline.

## Observed failure and source boundary

The frozen baseline Native run failed 372 passed / 27 failed / 1 ignored.
Seventeen FIRST setup cases stopped before the exercise: sixteen at the normal
INITIAL action join and one cross-protocol setup. The actual event stream records
n0 returning successfully before the first observed failed join in each case.
Those first failed joins are twelve n1 resets, three n2 resets, one n3 reset and
one n1 refusal. The fixture joins in node order and returns on the first error;
these are not an inventory of every concurrent action's outcome.

The actual handler frames authenticated semantic refusals as remote errors.
`request_shutdown` revokes ingress and ends the accept loop; it does not directly
cancel accepted worker sockets. `stop_and_drain` waits for those real workers.
An authenticated accepted read must therefore be distinguished from a connected
socket still waiting in the old accept backlog, and from a connect during the
close/rebind gap. The captured errors do not establish which original wire read
failed, or establish the listener schedule as the observed failures' cause.
Local controller bootstrap is not a network RPC. The first wire operation in
writer enrollment is the readonly HostOWN acquisition; later genesis reads
remain separate and unchanged.

## Narrow production change

Only the HostOWN exchange before INITIAL writer enrollment may repeat. It uses
the same actual launch, original service/open and Store-slot guards, and the
original caller's absolute action deadline. Each read has a new acquisition and
business context and must complete the existing authenticated HostOWN validation.
Connect, TLS, exchange and short pacing consume that same original window.

A repeat is allowed only for an actual transport-private socket failure marked
at the real connect or exchange: connection reset, refused, aborted, broken pipe,
or unexpected EOF. Error strings and arbitrary application IO cannot mint this
classification. Timeout, cancellation, semantic refusal, certificate/identity,
JSON, storage and evidence errors remain fatal. Success rechecks the original
request, open, slot and actual database. No prior response is a fresh grant.

Policy preparation, hook installation, retained enrollment, native mutation,
listener advancement and genesis recovery are outside this loop. Their existing
ordering and uncertain outcomes remain unchanged. This is not an action retry or
a mutation-replay facility.

## Actual control definitions

The two new controls use the existing three retained native INITIAL control
objects, distinct actual databases and mutually authenticated TCP listeners in
one Rust process. They call the original bounded `open_initial_data` producer.
They are not five independent processes, VMs, a complete DATA-ready cluster, a
Serving test, or physical HA qualification.

* Accepted/authenticated: pause a real HostOWN handler after peer authorization,
  begin the real leader's control-to-DATA drain, verify the real active worker
  remains counted, then release it. The actual framed/success result is retained;
  the control does not fabricate or demand a socket reset.
* Pending/before accept: pause the real old accept loop, establish an actual
  client TCP connection before TLS, drain and close that listener, and continue
  the real exchange. Require an actual privately classified socket failure and
  a later successfully authenticated read under the same original action token.
  The failed try is explicitly not described as TLS-authenticated.

Both retain actual action returns before assertions. The backlog control
requires a new authenticated readonly request through the same retained owner
while the original action remains live, and checks that continuation does not
repeat installed enrollment. Later unrelated genesis readiness may still fail;
no extra post-action deadline is introduced to collect READ evidence. Hook waits use original request windows.
RAII releases all synchronization and cancels the original requests before
scoped thread joins on unwind. Existing native DATA Drop/reaper and listener
shutdown retain their existing cleanup semantics; successful cleanup calls use
the original stop API. The controls do not claim synchronous complete cleanup
on a failed stop or panic. Missing cuts fail the control.

The test-only trace retains at most 32 scalar/JSON read-return facts and an
explicit omitted count; it retains no Error, Role, OWN, native owner, Frozen,
service, or raw request token. Synchronization controls expose no authority.

## Evidence states

| Scope | State | Limit |
| --- | --- | --- |
| Baseline Native candidate | Failed | 27 failures preserved; this source was not in that run. |
| Accepted-worker versus unaccepted-socket source distinction | Passed | Source inspection only; original observed schedule remains inconclusive. |
| Original setup failure's precise socket/operation cause | Inconclusive | No original transport-stage marker at the failing call. |
| New helper and two real control definitions | Not tested | Standalone formatting/static preservation checks are not Rust compilation or execution. |
| Full FIRST/Serving, cold recovery and automatic relocation | Not tested | Not established by a readonly INITIAL continuation. |

Canonical failure evidence remains under
`/root/bicdb-ha-private-evidence/visibility520-pr207-326c36347-run43682851011/`.
Native raw SHA256: `3deb1461fa4fb269c54e3a6adacafa91623e8f6c69ba221fae7777ca06e38046`.
The immutable failure/control projection SHA256 is
`a609ee7311bad5f2289f5b671a51ab2abb74d1a43d97f345309d92972fef94ba`.
