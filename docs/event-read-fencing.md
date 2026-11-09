# Checked event and broker reads

A handle with uncertain durable state requires reopen and verification before
business reads resume. Event snapshots, replay, projection rebuilds, queue
consumption and broker inspection now return `Result<T>` and report
`BicDbError::WriteFailed` when fenced. Empty streams, zero requested messages,
and absent groups follow the same rule. Database sync snapshots and realtime
queue callers propagate the error before constructing a response.

This intentionally changes the public Rust source API. Callers that previously
used `db.events().read("orders")` must now use
`db.events().read("orders")?`. Broker `peek`, `stats`, `group_info`,
`queue_config`, dead-letter reads and delivery availability likewise return
checked results. `delivery_snapshot` already returned `Result` and keeps its
signature. `export_events_since`, `pending_changes`, `sync_status`, and aggregate
projection `source_position` also require error handling.

Subscription registration now returns `Result<SubscriptionId>`, and callbacks
receive `Result<StoredEvent>`. Handle `Err` as a terminal recovery requirement.
The native async worker checks the shared health before and after dequeue;
events queued before a fence are refused. Idle workers check every 100 ms and
report the fence. Its receiver is private; no public raw receiver or iterator
bypasses these checks. A callback already entered before a concurrent fence may
finish; the API cannot retract data already delivered. Synchronous callbacks
receive checked outcomes, and failed subscriptions are removed.

Queued PostgreSQL notifications also check the database before draining and
before writing business payloads to the socket. A failed handle terminates that
read path instead of serializing a success or forwarding an old queued message.

The low-level stored-event and parser access used to rebuild recovery state
remains crate-private. There is no public unchecked business-read alternative,
empty-result fallback, or panic on a durability failure. Reopen verifies the
retained durable state before a new healthy handle permits reads.
