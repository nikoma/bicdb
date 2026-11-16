# Preserve the original finite bootstrap request under service contention

Base source: `4f01b680462e164ee821c93d7e5acf5625cc1521`.

The observed current-main REST trial failed while another process fetched native
bootstrap routing data: `NativeMetadataRegistrationController::bootstrap` used
instantaneous service `try_lock`, even though the actual transport supplied a
finite original request. A busy service could therefore reject discovery before
that original budget elapsed. This isolated patch addresses that source path;
it does not claim to have independently rerun the failed process trial.

The only production change selects the existing
`ClusterDataNodeService::capture_actual_listener_identity_waiting` when the
original caller already has a deadline. That helper uses the same token, checks
cancel/expiry around bounded service slices, returns actual live listener identity
and drops the service guard before metadata acquisition. The remainder of bootstrap
still calls actual `lock_metadata`, requires the actual listener/database binding,
reads committed topology/status, validates the returned snapshot and checks the
same original caller. Unbounded callers retain the original instantaneous
`native bootstrap service busy` rejection. No timeout, RPC retry, discovery route,
proposal, cached snapshot, authority fallback or placement logic changes.

Four source-defined controls reuse genuine `Five::start` normal host objects
(distinct native roots and TLS endpoints, one Rust process). Test-only DATA
facades call the actual controller bootstrap once. Existing real-service wait
controls hold the actual source service and cut after a failed bounded service
slice: release, live original caller cancellation, or original deadline expiry.
The same finite 500 ms component token reaches the production helper; no new
window is created. The fourth control holds the actual service and calls with an
unbounded token, requiring the unchanged exact busy error. It does not assert a
physical realtime bound or replay the public process scenario.

Controls preserve actual open/store/service binding and capture metadata C/status/
StoreUUID and native clock diagnostics before/after. They do not assert global
clock or topology equality while unrelated maintenance may legitimately advance.
The successful returned snapshot validates and comes from the actual current
metadata read. C/clock diagnostics are evidence, not a substitute for actual read
or a source of permission. No existing CLI assertions or request windows change.

Preservation checks reverse the production bootstrap substitution and remove only
the additive test wrappers/module declaration to restore all four preexisting
files byte-for-byte. In particular metadata status, registration, registry and
other bootstrap paths remain unchanged. New accessors are confined to existing
cfg(test) modules and return serialized DATA; no controller, factory, service,
native admission or mutable grant is exported. The actual snapshot type is
publicly exported and serializable; assertions avoid requiring Debug on hidden
owners. Error text includes the existing BicDbError::Cluster display prefix.

Standalone recursive rustfmt and whitespace checks passed. Four tests are defined
but have not been compiled or run. No Cargo/compiler, Rust tests, SDK/database,
CI action, push, Azure, version reservation or production action was performed.
Independent source review, complete preceding failure archive verification and
root approval remain necessary before composition/publication. Page checked-DATA,
cold-open, Joined metadata/inventory, ordinary PostgreSQL guard, full Joined target
release/controller and physical scale-out work are excluded from this patch.
