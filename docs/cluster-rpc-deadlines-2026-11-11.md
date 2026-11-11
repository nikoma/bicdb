# Cluster RPC deadlines and relocation scheduling

The TCP data client now uses one absolute network deadline for connecting,
TLS handshake, request transmission and response reception. Successful partial
reads or writes do not renew it. The configured `io_timeout_ms` now also bounds
this complete network exchange; it was previously a socket inactivity timeout.
This is a tighter failure boundary and can produce retryable timeouts for a
large or busy request that previously completed by continuing to make progress.
A relocation RPC also observes the earlier parent cancellation deadline. All ten
physical relocation calls use that path. TLS certificates, membership pins,
envelope identities and opaque metadata read acknowledgments keep their checks.

The socket wrapper sits below rustls and checks cancellation between bounded
socket waits. A final check rejects cancellation or expiry during response
parsing. A caller's cancellation error is preserved at the call boundary.
Per-request sockets are discarded after failure; partially sent/applied work is
an **uncertain outcome**, requiring the existing idempotent recovery protocol.
A timeout never establishes rollback.

A loopback reproduction linked to immutable source
`d49a391b3c6157f90f29e9f2f5214143c60f9f4b` (1.0.463-beta) received a valid
response in 301.40 ms while its socket inactivity timeout was 80 ms. That was
permitted by the old inactivity semantics; it demonstrates the missing
whole-exchange bound. The candidate rejected the same slow-progress exchange in
80.51 ms. Six isolated socket checks cover healthy reads, silent/slow peers,
manual/parent cancellation and a blocked writer. These are local component
checks, not physical failover, RTO or production qualification. Exact source
identities and raw results are retained in
[evidence](evidence/cluster-rpc-deadline-2026-11-11/summary.json).

Important remaining limits:

- Synchronous hostname resolution, lock acquisition, CPU work and storage fsync
  cannot be preempted by this wrapper. Numeric endpoints avoid DNS work in the
  physical experiment. A late result is rejected; these other operations do not
  have a newly established wall-clock bound.
- TCP connect is capped by the remaining deadline and connect timeout. A manual
  cancellation flag raised during that call waits for the connect to return;
  the 25 ms polling applies to socket read/write waits.
- The separate bootstrap client retains its earlier transport implementation.
- Metadata context validity deadlines still need to be carried into the future
  strong-read provider's cancellable transport calls. A metadata proof does not
  by itself establish bounded elapsed network time or strong row reads.
- Several RPCs can run in one relocation step, and several steps in one tick.
  The production host currently passes an uncancelable controller token. This
  change alone does not bound whole-tick latency, relocation completion or
  convergence under sustained load. The independent fair scheduling fix bounds
  turns, not those durations.

Actual mTLS deadline regressions and final combined validation are tracked
separately. Neither this network change nor the scheduling reproduction proves
the cause of the original Azure 300-second stall. The original failed
qualification evidence and its runtime remain unchanged.
