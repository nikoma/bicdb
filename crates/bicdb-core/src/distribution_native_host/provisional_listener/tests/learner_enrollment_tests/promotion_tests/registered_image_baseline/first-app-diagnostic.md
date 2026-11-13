These two diagnostics investigate the first ordinary APP prerequisite that failed
in the retained `6d795aac644a71b36ffef395a649ffc21b62206d` transfer campaign.
The original four traffic-first transfer tests, assertions and request deadlines
remain unchanged. Their three first-APP failures remain failures before transfer
prerequisites; these diagnostics do not replace that evidence.

- `native_registered_fifth_remote_transfer_observed_traffic_first_diagnostic`
  captures state and actual transport attempts before the first ordinary APP,
  without adding any Strong precondition.
- `native_registered_fifth_remote_transfer_after_ordinary_strong_absent_diagnostic`
  additionally requires actual ordinary Strong ABSENT responses on fresh,
  never-written keys in both source ranges. Unsupported or unavailable Strong
  probes remain errors, captured through the real SDK; no cached status grants
  readiness. This is a setup-qualified diagnostic, not startup availability
  qualification.

Both retain the existing INITIAL-open absolute twenty-second deadline; neither
creates a second setup window. Each real SDK call keeps the original two-second
request timeout. The diagnostics refuse to begin a call without two seconds
remaining in that existing setup budget. Each APP helper is invoked exactly once
per source range; actual SDK routing, discovery and retries remain unchanged.
No manual maintenance, synthetic Ready state, routing hint or new owner is used.

`first-app-pre-probe.json`, `first-app-pre-append.json` and post-APP snapshots
record all four actual original guards and coordinator identities, current
service slot/store-open observations, native range prefixes, INITIAL DATA status
and metadata leader/term/commit. Observations are sequential and may report busy
locks or actual errors. They are not an atomic snapshot, quorum proof or permission.
No engine, service or metadata latch crosses a client or control-host call.

`first-app-rpc.jsonl` durably retains intent, actual existing TransportHook frames,
addresses, request/attempt IDs, error chains and API replies. Logical caller
context is correlation; each raw RPC retains its own actual identity. Journal IO
is bounded to 10,000 records/96 MiB and can consume request time or fail closed
through the existing observer contract. It therefore changes diagnostic timing.
The original logical acknowledgment ledger and actual API error are retained;
API errors establish uncertainty, not rollback. No benchmark, hardware recovery,
serving fifth node or scale-out qualification follows from either test.
