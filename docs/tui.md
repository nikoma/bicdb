# BicDB TUI

Start the local terminal console with:

```bash
bicdb --tui ./testdb
```

`bicdb --tui` opens the current directory as the database path. Encrypted
databases can be opened with `--tui-key` or `--tui-key-env`.

The TUI is an embedded local database console. It opens the BicDB path directly;
it is not a pgwire client and does not require server mode.

The console is also an administration shell. BicDB databases are directories, so
database administration commands create, open, and inspect local database paths
instead of connecting to a PostgreSQL cluster catalog.

## Panels

- `Overview`: database path, size, record counts, storage overhead gauge,
  collection storage chart, and query latency chart.
- `Collections`: collection table, mode, row counts, vector dimensions, segment
  size, overhead, and field discovery from previewed metadata.
- `Data`: first records for the selected collection with id, timestamp, vector
  dimension, payload bytes, and metadata preview.
- `Broker`: live stream-broker observability. Queue table (retained messages,
  sequence counter, in-flight/pending/DLQ totals, config marker), consumer
  groups for the selected queue (lag, in-flight, pending redelivery, settled
  floor, DLQ depth), a live tail of the newest retained messages (sequence,
  id, age, delayed-availability, payload preview), the selected group's dead
  letters (reason, attempts, last error), and the queue's durable
  configuration including retention bounds and role ACLs. Up/Down selects the
  queue, Left/Right the consumer group. The Overview panel carries a one-line
  broker summary.
- `SQL`: SQL console and result table. Use raw SQL or `:sql <query>`.
- `Activity`: query latency chart, sparkline, and recent query history.
- `Ops`: local operations command list and live operational metrics.
- `Help`: keybindings and command reference.

## Keybindings

- `Tab` / `Shift-Tab`: switch panels.
- `1` through `8`: jump to a panel.
- `Up` / `Down`: select collections/queues or scroll SQL results.
- `Left` / `Right`: select the consumer group on the Broker panel.
- `/` or `:`: enter command mode.
- `Esc`: leave command mode.
- `F1`: help.
- `F2`: SQL.
- `F3`: data.
- `F4`: operations.
- `F5`: refresh.
- `q`: quit.

## Commands

Commands are entered from the bottom input line.

```text
:refresh
:check
CREATE DATABASE example
:createdb example
:opendb ./otherdb
:databases
:create-collection patients
:create-collection wearable timeseries
:drop-collection patients force
:sql SELECT * FROM patients LIMIT 10
:queues
:publish commerce.orders {"order": 1}
:drain commerce.orders workers 10
:queue-config commerce.orders {"retention_max_messages": 100000}
:trim commerce.orders
:redrive commerce.orders workers
:purge-dlq commerce.orders workers
:explain SELECT * FROM patients WHERE id = 'p1'
:backup ./testdb-full.bicbackup passphrase
:verify-backup ./testdb-full.bicbackup passphrase
:restore ./testdb-full.bicbackup ./restored passphrase force
:use patients
:kill
```

Plain SQL without a leading `:` executes directly.

`CREATE DATABASE name` is intercepted by the TUI before it reaches the local SQL
engine. It creates a sibling BicDB database directory and opens it immediately.
Use `:opendb <path>` to switch to an existing database path.

## Charts

The TUI currently renders:

- collection segment bytes as a chart;
- query latency over time;
- a recent latency sparkline;
- storage overhead as a gauge.

The activity charts are populated by SQL executed inside the TUI session.

## Query Cancellation

The TUI tracks active and historical TUI-issued queries and exposes a `:kill`
command slot. In the first implementation, SQL execution is still synchronous
inside the embedded process, so `:kill` reports when no TUI-launched query is
active and does not cancel remote pgwire clients. Server-wide query cancellation
belongs to the pgwire/server administration path and should be wired through an
admin API before the TUI can safely kill external client queries.
