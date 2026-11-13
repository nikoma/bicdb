# Event client operation errors during route discovery

At main500 `a06e09336c70fecbc38232d677c330984959388f`, an append or typed Get
can receive an actual operation error, then replace it with a generic
no-member error when later route discovery fails. A discovery error propagated
through `?`, or a Get deadline check after that discovery, can also hide the
operation's original diagnostic. That prevents reliable first-error analysis.

The client now retains routed-operation errors separately from discovery
fallbacks. Discovery keeps its original stop/continue policy and request
budget. If an operation has failed, later failed discovery or deadline exits
return the retained error without wrapping/retyping it. With no preceding
operation error, the original discovery/timeout fallback remains. Public
`refresh()` still reports no reachable member when discovery finds none.
Existing optional RPC evidence records retain the individual operation and
later discovery exchanges; this change adds no tracing or protocol authority.

For writes, an actual `CommitOutcomeUnknown` retains its exact sequence,
transaction ID, incarnation and detail across later operation refusals as well
as discovery/deadline errors. Dispatched RPC evidence uncertainty likewise
stays uncertain. A real successful response may complete the stable operation;
a later refusal does not prove the earlier attempt rolled back. Generic
transport errors are not declared definitely safe or rolled back. The event
namespace/key/value, command construction, Get business ID/challenges, retry
counts, route policy, backoffs, deadlines and successful read checks are
unchanged.

New regressions use the existing loopback TCP mock-response harness to preserve
an append/Get's first peer refusal through all failed discovery, keep the
conflicting-topology stop point, and retain no-member behavior for discovery
without any application operation. They count actual application and discovery
requests and check the stable append key/payload. Separate error-selection
unit tests retain typed unknown-outcome fields through a later operation
refusal plus refresh failure or timeout. These are routing/error unit tests,
not real database commits, authenticated native admission or HA qualification.

The leaf has passed formatting, diff and version consistency checks only; it
has not been compiled or run. Existing retry-test assertions are unchanged.
The earlier foreign process/CLI first-ACK failures remain failed evidence;
this observability correction does not establish their cause or fix a
production recovery, failover or scaling guarantee.
