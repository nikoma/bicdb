# Native metadata Append ACK provenance

The current committed native birth must qualify replication progress before
`record_append_response` updates `match_index`. An ACK from an unbound legacy
learner cannot become proof of owned native catch-up merely because enrollment
commits before that delayed reply arrives.

The direct-delivery regression actually failed at source
`cdc5203854bab7f5db194d44e8085977acef5b3c`, GitHub run `43166739631`, focused job
`131010271504`. The genuine old learner persisted the pending enrollment entry
while its committed configuration was still unbound. After old voters committed
enrollment, recording that held reply returned `Ok(false)`, but promotion then
accepted log index 4. The safety assertion failed. This was actual fsynced
in-process metadata-store delivery, not an authenticated network attack or an
Azure cause. The full focused job had 227 passing, 4 failing and 2 ignored tests;
its other failures are separate. The retained raw log SHA-256 is
`3c61ae5aeaf30d8f4fc7ccf65e280c1609cf87ee04b749f6a5819fcf22b9b396`.

The correction adds optional receiver identity data containing the complete
actual native birth and the protected metadata StoreUUID. The local producer
runs only after successful owned, fsynced Append persistence. Its StoreUUID was
captured from actual protected Registered initialization/reopen, not adopted
from the JSON state envelope. It also attaches private, non-serialized
provenance binding the complete reply body and actual actor pins.

The network producer consumes a seal made only by the original authenticated
RPC exchange. That seal retains the actual TLS peer, original Append request,
and expected native destination from the same topology snapshot used for the
address/pins. The response envelope must name that actual birth. The received
owned identity must match it before the transport can attach private local
provenance. There is no decoded-response resealing API. This retains the CFT
assumption that the authenticated correct receiver implements owned persistence;
it is not a remote WAL inspection or transferable signature.

Before counting a successful ACK, the leader checks that provenance and full
reply digest (including the protected StoreUUID), then compares the actual
incarnation/birth and pins against its CURRENT committed native member. A serialized identity declaration alone
has no such provenance. JSON equality describes the wire body and deliberately
does not imply a countable ACK. Changing a held reply body invalidates its
private digest. Cloning an unchanged durable reply remains valid: the receiver
need not stay alive after the fsynced ACK.

This requirement applies to the existing enforced native metadata contract
(writer-enforcement capability 1). Legacy absent identity fields are omitted,
and pre-enforcement birth data retains its earlier metadata CFT contract. No
abort capability is required. Whole birth equality includes whichever optional
capabilities were actually bound. The coherent private wire becomes 27;
older private codecs must refuse it. There is no rolling-upgrade claim.

Controls retain the original premature-promotion negative and genuine owned
learner catch-up/promotion/vote positive. Additional assertions reject a JSON
round-trip and a changed match index before progress/state mutation, preserve
legacy six-field response serialization, and accept a durable reply after the
actual receiver owners end. The real mTLS destination control also checks that
an original authenticated owned reply is countable while its plain decoded
copy is not. These correction controls are source-only until separately run;
the baseline failure above is not their validation.

This change does not protect same-store old term/vote-envelope rollback, create
metadata enrollment authority from birth data, or complete operator join/data
readiness. The initial configuration and native data recovery gates remain
separate.
