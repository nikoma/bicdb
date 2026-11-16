# Initial genesis first metadata read continuation

This unit is source only, based on frozen `6dec7347ef47a53da4cadda0190a17ab4e89b5d7`.
It has not been compiled or executed. It does not qualify DATA readiness,
Serving, SDK ACK/Strong behavior, physical HA, or cold/controller recovery.

The preserved public `6de646beb603accf39358017d8e0c83c39c741b1` Native run
executed 413 cases: 375 passed, 37 failed and one was ignored. Nineteen FIRST
setup cases emitted 47 `INITIAL_NATIVE_READ_FAILURE` events at `first_metadata`:
46 ConnectionReset and one ConnectionRefused. Every event retained the original
uncanceled parent with 4,944,640–4,958,254 microseconds remaining. Their requested
FIRST fault cuts were not reached. Another setup failed at Strong dispatcher
publication; the Over512 case failed during its original pre-relocation full
Strong reads. Those are separate failures. The fourteen Registered first-policy
timeouts, one same-original policy retry timeout, and one large-head OLD
observation retirement timeout remain separate, with no causal fix claimed here.

The retained raw log SHA256 is
`aec6a2b77679171285c9210039b7359df71339c604f0f189d1bef588bb27754d`;
gzip SHA256 is
`3ab11e1e4093f1b3a5d9689b21f342bae840e0bc41a5733565e7bfb87c15215b`.
The independent source/case projection is
`native520-6de-first-policy-and-setup-independent-causal-audit.json`, SHA256
`2f67aba6ab96755aa8264222a600f79f966c21d12aec8017be117399ed5e1d76`.
These describe the failed original source, not execution of this unit.

## Production boundary

Parallel actual INITIAL opens may stop/drain and replace the metadata leader's
listener while another actual local range leader obtains its first genesis
metadata read. That source schedule is possible; the historical server-side
cause of the observed resets has not been established.

The existing pre-enrollment HostOWN read already has its own sender-local socket
provenance and continuation. Genesis uses a different `MetadataReadAcquire`
operation. This unit opts only its first read into the same private transport
socket-failure provenance. The public metadata-read API, final genesis read,
ordinary recovery, HostOWN, semantic refusals, TLS/pin/identity checks and native
mutations keep their original one-shot behavior.

The original actual range reservation remains held. Before any new wait, an
actual finite parent is required. Each attempt checks the same original parent,
runtime/service Arc, live actual open, original Store-slot guard, borrowed DB,
installed host, and native genesis/range/epoch against the actual local topology.
All service/DB/topology guards end before network IO. A fresh acquisition and
business identity are created for each read; a successful observation is used
only with its own business identity. No old observation is renewed.

The immutable initial `fc517d70c` implementation had a source-found boundedness
gap: its new original-owner check called the legacy coordinator validator,
which can reacquire routing before the explicit routing try-read. The corrective
child obtains the actual routing guard with the original parent, releases DB
and service before pacing on routing contention, and uses the existing borrowed
topology validator plus genesis check under that same guard. Busy is never
absence/currentness. This correction does not alter the original post-first
read tail or claim a historical cause or an executed regression result.

Only locally produced ConnectionReset, ConnectionRefused, ConnectionAborted,
BrokenPipe or UnexpectedEof provenance permits another first read. Timeouts,
arbitrary application IO, framed semantic errors, malformed responses and
identity failures do not enter that continuation. Pacing and every exchange
consume the original absolute parent. The full original actual native-majority
candidate, genesis comparison, final metadata read, protected publication and
post-publication rollback checks follow successful first-read acquisition
unchanged. Socket status never establishes Ready or APP permission. Enrollment,
native WAL and host installation do not occur inside this loop.

## Genuine source controls, not executed

Three new normal-host definitions call the actual `OpenInitialData` producer.
The follower has already actually enrolled and started its DATA socket when the
new passive cut pauses its first genesis read. The leader's actual original
listener is paused before drain and before accept. The follower connects its
real `MetadataReadAcquire` socket to that old backlog; only after the original
leader reports shutdown and accept-loop termination is the connected socket
allowed to continue. The old-backlog socket must genuinely fail through the
private sender-local provenance. No error or response is injected.

The success case requires a subsequent actual authenticated first read for the
same range/epoch with distinct acquisition/business identities and remaining
original-parent time. Actual retained enrollment installation must still equal
one. Later native-majority/genesis/final-read/dispatcher results, including any
errors, are retained explicitly; this control does not substitute read success
for full `OpenInitialData` success.

The cancellation and expiry cases pause only after a real retryable socket
failure. They cancel the same original parent or allow its original five-second
deadline to expire. The actual public call must return QueryCanceled or
QueryTimedOut respectively, without a successful first-read return, selected
genesis readiness or APP publication. Original HostOWN transition controls and
all earlier FIRST/hot assertions are preserved.

Every handshake has an actual reached-cut condition and a finite token. On
action failure, releases and original-token cancellations precede worker joins.
Normal hook release precedes explicit joins; the RAII cancellation happens
after those joins. All worker panic and cleanup outcomes are retained, and the
original action failure remains primary. Kernel IO and thread joins are not
hard preemptible. These controls are definitions until normal CI executes them.
