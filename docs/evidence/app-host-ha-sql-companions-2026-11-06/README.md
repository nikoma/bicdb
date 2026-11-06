# Authenticated SQL and stateless companions in native HA

The application host previously refused its authenticated loopback SQL listener
when its database used a native state group. Attaching a separate shared server
would bypass the group's write lease and synchronous acknowledgement state.
The listener now attaches to the exact authenticated embedded HA server. Native
role changes close old sessions and reopen it; a lost listener fences the host.

A group can allocate stateless local HTTP companions. Their desired records and
immutable releases are replicated in the owner's catalog, their execution is
fenced by its lease, and routes follow its primary. Disabled owners refuse new
companion activation before replacing the valid deployment. Local readiness
requires the owner and every companion; it does not forward. Private state
forwarding trusts the configured group CA and presents the member certificate.

At implementation commit `618fec07061b41e6769f6fb46d35fe95d5c9540f`:

- Entire app-host package: 58 passed, 4 opt-in tests ignored.
- Explicit root/private-UID isolation: 1 passed. The remaining 3 ignored tests
  require pinned KVM images, which this process deployment does not use.
- Native embedded HA: 6 passed, including mandatory authentication/exact server
  attachment, lease fencing, replication shutdown and journal identity recovery.
- Actual app-host SQL/cohort drills with default SQL features enabled: 2 passed.
  Both host death and a live network partition preserve acknowledged extended
  writes, transactions and COPY, tenant isolation, exact versioned HTTP code,
  SQL reconnection, old-writer refusal, readiness, maintenance stop/start and a
  second successive failover. The isolated companion PID is physically retired.
- Removing the replicated-SQL attachment causes the new host-loss test to fail
  with the original refusal. The exact source is restored; the fixed tests pass.
- The actual private HTTPS test accepts the group CA/member identity and refuses
  an anonymous client or a client without private trust.

The runtime is based on the current upstream `5d9374c`, including its recent SQL
heap-row locking/profile changes. All commands run on synthetic fixtures. Early
fixture/missing-standby-cache failures and an overly specific survivor-election
expectation remain in the build host's earlier v1/v2/v3 logs; final logs here
record the corrected fixtures and final implementation. This is source/runtime
regression evidence, not a full EHR HA deployment or production acceptance.
