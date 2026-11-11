# Standalone source delivery scope

Source 1.0.468-beta delivers the lower metadata-consensus repair independently of the larger image/handoff work. The parent is `ecf6f60a2ae4433c02bc4352331b53c23dcd9c9d`, source 1.0.467-beta (PR #143). That parent's failed Azure experiment and qualification findings remain unchanged.

The engine change is confined to `distribution_consensus.rs`: historical committed restore validation for reopen/snapshot, monotonic committed decision guards, and protocol regressions. New restore decision admission retains exact topology validation. No schema, namespace-ID, image-install, handoff coordinator or metadata-acquisition provider is enabled by this delivery.

The archived `1586ba82` RED and `8bf444ae` passing results were collected on a separate source-466 foundation worktree. Those exact archived source snapshots include other unmerged foundation APIs. Their metadata-acquisition DTO checks belong to that source only; those APIs and their checks are intentionally absent from the standalone delivery. They must not be reported as validation of 1.0.468-beta. `delivery-validation.json` records the separate final versioned delivery checks once run.

The test protocol operates three real, fsync-on local stores with actual vote replies, quorum commits, a distribution load-update operation and a leader-produced compacted snapshot. Its trusted-caller store boundary does not exercise TLS authentication, VM crashes or physical storage power loss. Source, test evidence, a published binary, deployment and production qualification remain separate states. No runtime binary/release/deployment is produced by this merge.
