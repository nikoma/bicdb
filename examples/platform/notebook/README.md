# Field Notebook

A small shared notebook for field notes, handoffs and observations. A browser
saves each note and its pending upload together in IndexedDB. It retries the
same event ID and content after reconnecting, then marks the note synced after
acknowledgement. Other devices receive new notes through bounded polling.

The backing records use `env.EVENTS`, so notes can occupy different range
leaders as a cluster grows. This demonstrates the explicit immutable event
contract; it does not demonstrate distributed SQL transactions.

For a local demonstration with already-built binaries:

```sh
python3 examples/platform/notebook/demo.py \
  --bicdb target/debug/bicdb --bic target/debug/bic
```

This creates new local state, starts a loopback-only RF=1 cluster, allocates the
binding and deploys the app. Open the printed URL; Ctrl-C stops its services
and retains data/logs at the printed path. It demonstrates functionality, with
no node-failure resilience. Use the following operator steps for an RF=3 cluster.

1. Build `bicdb` and `bic`: `cargo build --release --locked -p bicdb-cli -p bicdb-app-host`.
2. Bootstrap a cluster with `--application-events`, join members and wait for
   healthy placement. A one-member local demonstration can use RF=1; an RF=3
   resilience demonstration needs at least three members.
3. Allocate the `notebook` namespace in the operator's event configuration,
   start `bic serve --events-config /path/to/events.json`, and run
   `bic deploy examples/platform/notebook/bic.json`.
4. Open `http://127.0.0.1:8788/apps/notebook/`. For remote browsers use HTTPS so
   service workers and device cryptography are available.

See [configuration and transaction contracts](../../../docs/application-cluster-events.md).
No npm packages, build hooks or external scripts are required.

The first visit creates a 256-bit room capability in the link fragment. Copy
the link to another browser to share that notebook. The capability is sent in
an authorization header for API calls; its hash scopes record IDs, and only
that room's notes are returned. A different room link and a different allocated
application cannot read those notes. Anyone with a room link can read and
append; there are no user accounts, individual revocation or mutable edits in
this reference application. Keep links private. Use your application's real
identity policy when adapting the example for account-based access.

To demonstrate offline use, wait for the first connected page load, disconnect
the browser, add a note, and reload. The cached application shell and IndexedDB
queue should retain it. Reconnect and watch the pending count return to zero;
the other browser should show one copy. Notes use append-only reconciliation,
with no silent last-writer overwrite. Clearing device storage before pending
notes upload loses that device's unsynchronized copies.

The example keeps browser history without pruning, and each range page scans
the application's event namespace before filtering rooms. It is intended as a
small working reference. Large deployments need retention, per-room indexing
and authorization appropriate to their users. Cluster backup is separate from
the application's local SQL backup.

The queue test injects a lost response after the server commit, reconstructs
the client, retries, and verifies that one event remains:

```sh
node --test examples/platform/notebook/*.test.mjs
cargo test -p bicdb-app-host --test events
```

Independent usage, separate-machine performance and large-scale certification
are not established by this example. The [cluster harness](../../../scripts/platform/README.md)
records scaling/failure evidence without conflating local processes and
independent machines.
