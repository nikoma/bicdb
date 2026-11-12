# Initial native startup handoff

This prototype adds a bounded continuation of the common initial-configuration
preparation barrier. The source has not been compiled or executed locally.

`NativeInitialConsensusLaunch` retains the actual owned metadata store, original
data service/range store, actual TCP transport and shared metadata provider. Its
new listener remains permanently restricted to metadata endpoints. The original
provisional listener remains permanently provisional; an expired weak provider
cannot broaden either allowlist.

`elect_initial_metadata` obtains real pinned TLS votes, persists the leadership
barrier and sends bounded Append batches through actual peer listeners.
`heartbeat_initial_metadata` is an explicit bounded batch, not a background
election scheduler. Both preserve the caller's original cancellation/deadline;
network calls hold no service, DB or metadata mutex. These methods provide no
fresh read or data authority merely by returning a commit index.

`enroll_initial_writer` obtains a separately fresh Host MQ observation, validates
the actual local native birth/open and consumes that observation into the existing
sealed same-service writer-policy producer. The returned owner retains the real
coordinator and installed hook guard. A policy is not range readiness, recovery,
Serving, membership activation or an acknowledgement of an application write.

The additive four-process control uses four independent actual native factories
and mutual TLS sockets, with parent commands selecting bounded phases. It checks
stable service/store ownership through preparation and listener drain, real
election and Host MQ provenance, policy installation on every node, and continued
refusal of ordinary application requests. Process-local files select actions and
record results; no file or JSON DTO constructs consensus or writer authority.
This test source is unexecuted. The earlier factory-only process test is retained.

The next required transition is a separately owned, stopped-and-drained change
from metadata-only listeners to ordinary data listeners, after all actual node
policies and genuine local R0 lineages are verified. It must retain the same
service/store/provider and perform native recovery and the first actual data ACK
under current metadata and data-quorum proofs. No existing listener may broaden
its captured allowlist, and no policy or initial C can substitute for that proof.
Cold restart after consensus exposure still requires the strict owned metadata
backend, preserving term/vote; this patch adds no full restart supervisor.

The complete from-birth abort protocol remains a separate dependency. This patch
does not copy a capability bit, enable a reader flag in isolation, or claim that
the current startup source satisfies that unfinished protocol. The wider M1
operator scenario, fifth-node enrollment and joint-data transitions remain open.
