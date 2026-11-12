# Metadata failover fixture listener identity investigation

This source-only correction is based on `0a75fdc0821c223c23dd7a1aa9b1ffa608570a39`.
That exact read-delivery candidate's PGwire library run43247621424,
job131284426434 failed 150 passed / 1 failed / 0 ignored in29.87s. The failure was
`metadata_quorum_elects_and_commits_after_leader_failure`, before its deliberate
leader stop: not every member heartbeat reached committed metadata. The useful
SQL/workspace checks on that candidate remain independent and must finish.

The original job log is retained privately as
`0a7-0a7-read489-pgwire-job131284426434.log.gz`; uncompressed SHA-256:
`65737bd21c77d10d8a83c06c9e389904319fcba4227050c2e721b32321c9ca10`.
The failing test stdout first records n3 listening at127.0.0.1:46007 and n1 at
127.0.0.1:38627, then `Address already in use (os error98)`, no n2 listening
marker, and repeated n2 vote/Append response-identity refusals. Thus a listener
bind collision is observed; delayed heartbeats alone do not explain it. The
original log does not record the failed requested address or which response
identity predicate failed, so duplicate address selection within this fixture
versus a different parallel/stale listener is not distinguishable. No missing
native fault-state archive or exact occupied-socket owner is invented.

`unused_loopback_address` returned the address of a bind0 socket after dropping
it, leaving a real allocation-to-bind race. The corrected failover fixture owns
three kernel-bound loopback listeners concurrently and passes duplicate socket
descriptors into the actual common metadata/data serving startup path. It never
closes a selected socket to rebind its address. Reservations remain owned across
the stopped-member interval and the same strict storage-root restart. During
that interval the reserved endpoint has no serving acceptor or reply authority;
it is unavailable rather than reassigned to an unrelated test. Pending clients
may time out; the original election/heartbeat/convergence/strict-reopen assertions
and deadlines remain unchanged. A panic-only owner guard requests shutdown on
all surviving actual supervisors, instead of leaving an earlier failed fixture
running into another cluster. The failed member is removed from that guard so
its original DB really closes before its strict reopen.

The Core socket wrapper uses the same private startup body as address binding:
configuration/TLS validation, original-open and attached metadata identity,
authenticated ingress, runtime wiring, handler counting and shutdown are shared.
A descriptor is only endpoint ownership; it supplies no native/data/MQ permission.
No metadata receiver, Raft frontier, response identity check, nonce or deadline is
weakened. No global test serialization, random-port retry or deadline extension
is introduced. Only the PGwire test entry passes a reserved listener; production
supervisor startup continues binding its configured address.

Identity errors preserve the original refusal prefix and now include bounded,
escaped expected/actual protocol, cluster, source node and request ID, optional
native-binding predicate results, caller, configured address and resolved socket.
They contain no application response body, TLS private key or complete birth
record. Listener startup errors include actual node/cluster/address.

The deterministic Core control uses an actual shared-startup receiver on a
reserved socket, proves competing bind exclusion, successfully pings its real
identity, then deliberately routes a different node or cluster claim to it. It
asserts exact source-only versus cluster-only mismatch predicates and the actual
endpoint. These are negative routing claims, not synthesized server replies or
fabricated authority.

Only rustfmt and diff/source checks were run locally. No Cargo, passing runtime
result, version reservation, merge, binary, release, deployment or physical/HA
qualification is claimed. Required next checks: actual Core wrong-route control,
the exact PGwire failover test with default test concurrency, then the unchanged
full PGwire suite and final versioned candidate's required checks.
