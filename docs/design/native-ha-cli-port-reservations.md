# Native HA CLI fixture endpoint custody

The original PR185 workspace job `132321987763`, run `43553796767`, failed the
`killing_the_primary_loses_no_acknowledged_write` test at
`ha_failover_cli.rs:219`: no new primary after the original 45-second wait.
The sibling planned-switchover test passed. The HA integration target returned
one pass and one failure in 49.82 seconds under four Rust test threads.

The recorded checkout was merge `f43526cf8c0bde8969b6b30f25a15c5cb0a002bd`,
whose tree equals PR head `2ddfef5f9f54ab8cef29c3b1d7fec6e33f92567e`.
The CLI fixture was byte-identical to main505
`e25b3ba194c6f3ad73e2f393b68b175fbf1edd92`. The full original raw job log is
84,126,216 bytes, SHA-256
`225544aa5e17625e550f4b338ebd7285cbea9815c3522bde096dfd02a20fa9b2`.
It is preserved separately; this successor does not relabel that failed run.

The failed case's n2 child reported its SQL listener at `127.0.0.1:44827`,
then exited with `Address already in use (os error 98)`. After n1 was killed,
n3 remained a read-only standby. Thus the test injected its fault without a
healthy three-process starting group. The exact conflicting port and competing
owner are not established: the original test printed only the last 40 lines
per child and did not retain its six allocated endpoints. It selected each
port by binding `:0`, reading the address and immediately dropping that socket;
there was neither a retained reservation nor a uniqueness check within or
across the two parallel groups.

At least one pre-kill write was acknowledged: the original nonempty-ACK-set
assertion passed. The exact count and IDs are not retained in the original
failure output. No new-primary read, acknowledged-set reconciliation, old
primary rejoin or final fencing assertion was reached in the failed case.
This is not a demonstrated acknowledged-data-loss result or a healthy-group
election failure. No original raw DB, native state or RPC artifact was uploaded
by this older fixture. Passing a successor cannot establish additional facts
about that original missing state.

## Test-harness successor

Every group's actual SQL and replication listeners remain held until that
specific child's immediate spawn. Other not-yet-started nodes and parallel
groups retain their sockets. The child must remain alive according to
`Child::try_wait`, identify its configured node over the real SQL listener,
and answer a genuine replication protocol request before the next child
starts. The readiness request deliberately uses a foreign cluster ID, which
is refused before peer/epoch/contact handling; it neither renews a primary
heartbeat nor asks for an election vote. All three owned processes/listeners
are rechecked before the original kill or switchover.

These are process/listener prerequisites, not a certificate of data catch-up,
quorum freshness or strong-read readiness. No additional wait forces both
standbys to contain every acknowledged write before the kill. Original
synchronous standby count, 3-second HA/sync settings, 45-second promotion wait,
write workload, loss assertions and fault remain unchanged. There is no
suite-wide serialization, restarted failed fixture or port reallocation.

Complete child logs, endpoints, PIDs and observed exits are included on failure,
including a final read after owned teardown, before the temporary directory
can disappear. Two deterministic regressions check twelve simultaneously held
endpoints from parallel groups and a real CLI child refused an actually held
replication port before fault injection. They are source additions until
hosted compilation and execution report their own exact revision.

A small release-to-child-bind race against unrelated external processes remains;
the CLI does not support inherited listener transfer. Such a startup error must
now fail the prerequisite with its evidence, rather than becoming a misleading
failover result. Restarting the killed node keeps the configured peer addresses;
this fixture does not claim to reserve a stopped production node's address or
qualify physical fault recovery.
