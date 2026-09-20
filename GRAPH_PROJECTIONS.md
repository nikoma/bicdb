# BicDB Graph Projections

BicDB graph support is derived, not canonical. Records and events remain the
source of truth. Graph sidecars can be rebuilt from collections and event
streams at any time.

## Model

```rust
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub properties: serde_json::Value,
}

pub struct GraphEdge {
    pub id: String,
    pub from: String,
    pub to: String,
    pub label: String,
    pub properties: serde_json::Value,
    pub timestamp: Option<i64>,
}
```

Node IDs use `Label:id`, for example `Patient:p1` or `Doctor:d7`.

## Projection Builder

```rust
use bicdb_core::{BicDb, GraphProjection};

let mut db = BicDb::open("./clinicdb")?;

let graph = db.build_graph_projection(
    GraphProjection::new("clinical_graph")
        .nodes_from("patients", "Patient")
        .nodes_from("doctors", "Doctor")
        .nodes_from("devices", "Device")
        .edge_from_field("appointments", "patient_id", "doctor_id", "VISITED")
        .edge_from_field("measurements", "patient_id", "device_id", "MEASURED_BY"),
)?;

# Ok::<(), bicdb_core::BicDbError>(())
```

For record-as-node relationships such as Patient -> Appointment -> Doctor:

```rust
let projection = GraphProjection::new("clinical_graph")
    .nodes_from("patients", "Patient")
    .nodes_from("appointments", "Appointment")
    .nodes_from("doctors", "Doctor")
    .edge_to_record("appointments", "patient_id", "Appointment", "HAS_APPOINTMENT")
    .edge_from_record("appointments", "Appointment", "doctor_id", "WITH_DOCTOR");
```

Event streams can also feed graphs:

```rust
let projection = GraphProjection::new("event_graph")
    .nodes_from_events("clinical-events", "PatientCreated", "Patient", "patient_id")
    .edge_from_event_fields("clinical-events", "PermissionGranted", "from_id", "to_id", "GRANTED");
```

## Queries

```rust
let graph = db.graph_projection("clinical_graph")?.unwrap();

let neighbors = graph.neighbors("Patient:p1");
let edges = graph.edges("Patient:p1");
let path = graph.path("Patient:p1", "Doctor:d7", 3);
let reached = graph.traverse("Patient:p1", "HAS_APPOINTMENT", 2);
```

`neighbors` and `path` treat edges as navigable in either direction. `traverse`
is directional and follows outgoing edges matching the supplied label.

## CLI

The CLI currently ships a built-in `clinical_graph` projection:

```bash
bicdb graph build ./db --projection clinical_graph
bicdb graph rebuild ./db --projection clinical_graph
bicdb graph verify ./db --projection clinical_graph
bicdb graph query ./db "NEIGHBORS Patient:p1"
bicdb graph query ./db "PATH Patient:p1 Doctor:d7 DEPTH 3"
bicdb graph query ./db "TRAVERSE Patient:p1 HAS_APPOINTMENT DEPTH 2"
```

Run the healthcare graph demo:

```bash
cargo run -p bicdb-core --example healthcare_graph
```

## SQL Views

Graph sidecars are exposed as SQL-readable virtual tables:

```sql
SELECT * FROM bicdb_graph_nodes;
SELECT * FROM bicdb_graph_edges;

SELECT id, label
FROM bicdb_graph_nodes
WHERE id = 'Patient:p1';

SELECT "from", "to", label
FROM bicdb_graph_edges
WHERE label = 'VISITED';
```

Node columns:

- `projection`
- `id`
- `label`
- `properties`

Edge columns:

- `projection`
- `id`
- `"from"`
- `"to"`
- `label`
- `properties`
- `timestamp`

## Rebuild And Verify

Graph projections persist under `graphs/*.graph.json`. The sidecar includes the
projection definition, derived nodes, and derived edges.

```rust
let report = db.verify_graph_projection(&projection)?;
assert!(report.valid);

db.rebuild_graph_projection(projection)?;
```

When a graph sidecar exists, BicDB refreshes it after direct writes, deletes,
transaction commits, event imports, and sync imports. The current implementation
rebuilds the whole projection for correctness; incremental graph maintenance is
future work.

## Benchmark

```bash
bicdb bench graph --patients 100000 --edges 1000000
```

The benchmark reports:

- graph build time
- neighbor lookup time
- path query time
- edge scan time
- node count
- edge count
- graph sidecar size
- database size

## Limitations

- Graphs are derived sidecars, not authoritative storage.
- The CLI has one built-in projection today: `clinical_graph`.
- There is no Cypher/Gremlin parser.
- Traversal is in-process and local-only.
- Graph refresh is whole-projection rebuild for v0.1.
- There is no graph-specific index beyond in-memory maps in the sidecar.
