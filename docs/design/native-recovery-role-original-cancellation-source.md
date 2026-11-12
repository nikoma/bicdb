# Recovery-role acquisition cancellation: source prerequisite

Source only, inherited full prototype version `1.0.489-beta`. No local build,
executed regression, merge, deployment or timing result is claimed.

`MetadataRecoveryRoleObservation` previously retained its validated response and
original capped deadline, but not cancellation of its acquisition parent.
Retaining that observation inside a later image transaction therefore did not
preserve cancellation of a separately acquired step-role request.

The two sealed production minting paths now retain their actual request token:
`MetadataReadProvider::witness_recovery_role` supplies its original bounded token
to the local-authority constructor; the authenticated transport supplies its
original bounded acquisition token to the transport constructor. These tokens
retain their actual upstream cancellation chain. Neither path retains the
internal `acquire_authority` `CancelOnReturn` child, which deliberately stops
quorum workers on return. Response validation, request/challenge/recipient/TLS
binding and existing deadline calculations are unchanged. Constructors and
`require_live_request` additionally check that retained original request.

All six existing test transport wrappers pass the same parent used for their
actual acquisition. A new actual-store provider regression exercises both the
local sealed mint and a test transport wrapping real quorum evidence: each
observation is live first, its actual parent is canceled before its original
deadline, and its unchanged request must then fail with `QueryCanceled` without
altering that deadline. Filter:
`recovery_role_observations_retain_local_and_transport_acquisition_cancellation`.
The test transport is a component CFT seam, not an actual TLS attack test.

This prerequisite changes only recovery-role observations. Other metadata
observation classes retain their existing, independently reviewed lifetime
contracts; this patch does not claim a cancellation audit or repair for them.
