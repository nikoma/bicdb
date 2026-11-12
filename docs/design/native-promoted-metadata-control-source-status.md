# Registered member metadata control successor

This successor is source-only. No local Cargo was run. The predecessor's two
promotion controls passed at Root `d194594b1`; that result does not validate this
changed backend/listener path. Existing manual CLI and supervisor receipts remain
scoped to their exact earlier sources.

## Confirmed source gap and implementation

The predecessor could promote the fifth and run its actual metadata supervisor,
but its listener still captured the old metadata-only allowlist. It had no
registration queue/controller. Therefore local Voter state or even a successful
election did not imply that it could serve Bootstrap/Register/verified schema for
the next joining member. The successor does not restrict the fifth's campaigns.

`NativeMetadataControlBackend` has separate sealed constructors for an ACTUAL
initial launch and an ACTUAL protected Registered member. The member constructor
requires the strict owned-member observation, independently fresh OWN Host MQ,
matching local committed C/term/index, actual DB open/birth/restore/namespaces,
same metadata StoreUUID, same service, and the actual bound metadata provider.
It never fabricates an InitialLaunch or grants a blank Voter factory permit.
The common backend supplies the original finite real-vote election, accepted
per-peer Append16 heartbeat, enrollment and registration operations. Initial
caller APIs delegate to it so bounded-frontier and midround-retirement rules do
not diverge between old and newly promoted leaders.

Fresh and cold retained owners stop/join their one supervisor, transfer the
existing listener into a retained control-transition owner, actually stop/drain
it, and bind a NEW immutable control listener. The original listener does not
change allowlists. All fallible validation precedes moving its handle; errors
retain the same backend, StoreUUID, old/drained/bound listener and controller.
The supervisor is restarted with the original process lifetime only after the
control listener is owned. Idempotent control checks do not repeatedly restart
the supervisor. Stop order is supervisor, control listener, previous listener.
A bound listener is retained before the final cancellation check.

A metadata-only registered voter can export its verified schema mirror. This is
schema DATA, not data-replica or read authority. The source must be an actual
current registered metadata Voter with matching native birth/restore/schema;
Registered-member original namespaces/StoreUUID are rechecked under the actual
DB borrow. The distinct post-export OWN MQ and schema generation/namespace
rechecks remain mandatory. A copied fingerprint/config DTO cannot supply the
actual DB bundle or currentness. No source-node root/TLS settings are adopted by
the joiner, and no private key bytes are returned.

## Status and commands

Learner status separates configured metadata role, actual local voters, actual
metadata listener ownership and `metadata_control_owned`. The operational phase
`owned_metadata_voter` now additionally requires the new control listener;
between installed promotion and control it reports
`owned_metadata_voter_awaiting_control`. Strict local currentness remains
mandatory; lower Registered catch-up permission cannot report voting readiness.
The default join loop enables control, maintains CONFIG membership only when
actually leader, and keeps the general supervisor separate. Explicit diagnostic
`elect` now supports the retained promoted-member owner and uses actual votes
and a current-term committed barrier. No term/role/counter setter is exposed.
The manual diagnostic scheduling mode remains separate.

## Regression sources

The existing genuine-store/TLS promotion positive now first verifies that the
old listener still refuses metadata snapshot control, then enables the new
control listener on the SAME actual backend, verifies Bootstrap, and elects the
fifth through real TLS votes while preserving its protected StoreUUID. Its
original cancellation/decoded-ACK/installed-voter assertions remain.

The default automatic CLI process case still starts the fifth with only
`join-fresh` and requires actual local installed Voter state. It then invokes
public diagnostic election on that fifth. A sixth REAL backend starts with only
`join-fresh` and retains the original seed0 hint. Registration follows actual
current metadata authority after the fifth's real election; autonomous elections
may subsequently change that leader. The CLI currently selects its schema source
using the first actual range's DATA leader, not the metadata leader. Thus CLI
success does not prove schema export by the fifth metadata-only mirror. This is
an availability dependency on an initial data leader. The deterministic Core
successor instead issues a real sixth-client registration and schema RPC directly
to the elected fifth, proving that exact receiver without new tracing fields.
The sixth CLI backend has its own root, TLS files, namespaces, birth and StoreUUID;
no private proposal or hidden learner protocol step supplies the positive. All
six actual application Scan requests remain refused. The previous manual/cold
CLI test stays separate and unchanged in scheduling mode.

Suggested remote checks: Core `native_metadata_promotion_`,
`native_initial_metadata_actual_tls_` (17/65 frontier and retirement),
`native_owned_metadata_supervisor_`, `native_registered_learner_enrollment_`,
and CLI target `native_metadata_operator_cli`. All-target and isolated consumer
checks should still run independently. None has run on this successor yet.

## Explicit remaining scope

This is metadata membership/control, not data Serving, new data placement,
NativeReady, data activation, SQL, multi-VM or physical qualification. The
Registered-only behind-C/cold-after-promotion continuation is a separate source
track and must retain the strict installed-member readiness check. A stopped
control owner deliberately retains its actual MStore/provider until the opaque
host is dropped; callers must not assume the old pre-control Arc count when
claiming all owners have closed. No UUID reset or root recreation is supported.


### Existing observability successor (source-only)

The test-only successor records the actual committed registration history actor
and generation. The native registration producer fills that actor from its
actual local metadata store after fresh OWN leader MQ; it is not a caller's
asserted handler list. The automatic case permits legitimate later elections and
checks that the audited actor is an actual admitted metadata voter. It preserves
the fifth's actual election/control/StoreUUID assertions. The fresh learner
manifest records factory OWN MQ context/term/readIndex and schema SHA. Those
fields are not registration-event term or schema issuer and are not relabelled
as such. ClusterSchemaBundle contains no receiver/issuer audit.

The deterministic Core control adds a distinct sixth TLS identity, actual
registration directly to fifth leader4, exact committed actor4/generation,
actual pinned schema export to leader4, and a stale old-leader MQ refusal after
that old leader has actually applied the sixth registration. A new fresh OWN MQ
and exact-fifth schema call then succeed under the SAME original budget; the
expired/refused attempt never renews it. This Core case registers a sixth CONFIG
identity, not a sixth storage backend. The independent CLI case creates the
backend. Poll loops require error-free current status before accepting ready
fields so transient cached-error status cannot end the retry window early.

First successor compilation at Root fe43 failed before tests because the new
listener address parse needed an explicit SocketAddr type. Root retains that
failure and supplies the separate 3b75b095b type-only correction. No local builds
or runtime results are claimed for these added assertions.
