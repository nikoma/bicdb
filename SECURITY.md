# Security

## Reporting a vulnerability

Report suspected vulnerabilities privately through
[GitHub Security Advisories](https://github.com/nikoma/bicdb/security/advisories/new).

Please do not open a public issue for an unfixed vulnerability.

Include whatever you have: the affected version, a description of the boundary
you believe is crossed, and a reproduction if you have one. A reproduction is
useful but not required — a precise description of the code path is often
enough to act on.

You should expect an acknowledgement within a few working days. BicDB is
maintained by a small team; we would rather tell you honestly that triage will
take a week than leave a report unanswered.

## Scope

BicDB is beta software under active development. The boundaries we treat as
security-relevant are:

- **Multi-tenant isolation** — cross-tenant reads or writes, RLS bypass,
  privilege escalation between SQL roles.
- **Authorization** — ownership, GRANT/REVOKE, definer semantics for views and
  routines, and the authenticated surfaces of pgwire, the broker, the sync
  server, and the application runtime.
- **Availability from untrusted input** — process crashes, unbounded memory or
  CPU consumption reachable by an authenticated client. A stack overflow is a
  process abort, not a catchable error; we treat it as a denial of service.
- **Cluster and replication trust** — what an authenticated peer node can
  assert about its own identity or another node's.
- **Data integrity** — durability, recovery, and corruption reachable through
  supported interfaces.

Out of scope: findings that require an already-compromised host or filesystem
access to the data directory, and behaviour of deployments that disable the
documented authentication mechanisms.

## What we consider a legitimate finding

A missing authorization check is a finding even without a working exploit, if
the code path is unambiguous. We would rather receive a precise code-reading
than nothing.

We ask that reports distinguish what was *verified* from what was *inferred*.
That distinction is the most useful thing a report can carry.

## Supported versions

Only the latest beta release receives fixes. Version numbers are
`1.0.x-beta`; see [CHANGELOG.md](CHANGELOG.md).

## Disclosure history

BicDB maintains a public record of resolved findings and their remediation in
[docs/security-audit.md](docs/security-audit.md), including issues found by our
own review. Recurring classes are written up separately — see
[docs/ambient-authority-bug-family.md](docs/ambient-authority-bug-family.md).

We publish this history deliberately. A database with no visible security
history has not been examined, not proven safe.
