# Fresh metadata read-authority acquisition

This source adds an opt-in authenticated metadata service. It does not add a
strong row-read endpoint, a range-image handoff, or a production qualification.
Ordinary `EventGet` retains its existing contract.

A current range leader can call
`TcpClusterRelocationTransport::acquire_metadata_read_observation` on the actual
metadata leader. The request binds a fresh acquisition nonce, logical request
UUID, range ID/epoch, caller ID/incarnation and finite budget. The metadata leader
validates the authenticated caller against its committed range descriptor,
begins a fresh current-term read-index round, probes its old and pending voter
configurations, records transport-verified responses and finishes only after both
fresh majorities apply the captured committed index. It rechecks leadership,
voter identities and the exact range descriptor before returning.

The returned `MetadataReadObservation` is opaque and cannot be deserialized,
cloned or converted into a local `MetadataReadAuthority`. Its request-bound
committed topology includes relocation records and node identities; getters
require the original logical request UUID and a live original deadline. The
transfer DTO describes the leader's authenticated quorum observation; voter
fields are not independently transferable voter signatures. A local authority
also retains the original begin-round deadline, with `remaining_budget` and
`is_current_budget` checks for consumers. Completion never starts a new lease.

The optional
`start_cluster_data_server_with_metadata_read_provider` wrapper preserves the
legacy listener API. An unwired listener refuses acquisition. Unsupported
responses, stale epochs, wrong callers, changed configurations, unavailable
quorums and expired contexts produce errors. There is no cached-topology,
ordinary-Append-acknowledgment or eventual-read fallback.

Admission is bounded to four contexts and four workers per context, with at most
64 metadata voters, four attempts per voter by default, bounded queues and a
2 MiB serialized proof bound. On fresh quorum the service cancels sibling work
and returns without joining an unreachable voter. Detached workers retain their
context admission permit until they drain. Consensus latches are released before
network RPCs. Metadata persistence still uses ordinary non-preemptible OS file
I/O; a deadline cannot interrupt an in-progress local filesystem fsync.

These new acquire/probe interfaces require numeric `SocketAddr` destinations.
They deliberately refuse DNS names so a synchronous resolver cannot escape the
absolute parent network budget. TLS CA validation, node certificate pins,
member incarnations, cluster/source/request envelopes, cancellation and the
original absolute deadline remain mandatory. Other existing transport methods
retain their existing address behavior.

Focused source tests use fresh temporary loopback mTLS identities and actual
consensus stores. They exercise a range leader different from the metadata
leader, a silent third voter, held data-service latches, unwired listeners,
caller/epoch rejection, leadership/configuration changes, request nonce and
proof validation, deadline retention and bounded admission. These are local
software tests, not independent-host HA, strong-row-read or physical
qualification evidence. No Azure resources are used by these tests.

Validation of this implementation batch, based on
`226d9fd80ecd9dbf249fd4fee5984b61311fed47` (1.0.466-beta):

- `cargo test --release --locked -p bicdb-core --config
  profile.release.package.bicdb-core.opt-level=0 --config
  profile.release.package.bicdb-core.debug=0 --lib distribution_` passed 148
  tests, including eight acquisition-provider tests.
- Compilation used one job with incremental compilation disabled and an active
  12 GiB available-memory guard. No guard pause or resource interruption occurred.
- This result applies to the provider source batch, not an earlier Azure binary
  and not a deployed production configuration. The optional runtime must still
  be integrated with certified image capture and application visibility gates.
