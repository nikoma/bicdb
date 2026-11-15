# Original-request MStore admission for native source discovery

This unversioned source batch is based on `9190c4154eb88212e9edc8b47690dfd60bd544a1`.
It is uncompiled and unexecuted. It changes neither the active CI heads nor any
production deadline, Role, OWN, source-session, native writer or quorum producer.

Main517's actual postmerge run failed S2 retirement before the intended remote
NEW after-take control: `SourceDiscoveryAdmission::require_current` returned
`source discovery metadata busy at boundary`. The same source tree's premerge
success remains separate evidence. The observed refusal establishes contention
at an immediate `try_lock`; it does not establish scheduler or storage causality.

The two instantaneous recheck acquisitions now wait in slices at most 5 ms,
bounded by the SAME original caller and OWN deadlines. Each attempt checks the
original caller, original OWN request/business and sealed member version before
and after acquisition. A cancellation, expiry, dirty authority gate or changed
version still refuses, including when the mutex becomes available concurrently.
No new request or OWN is issued by this helper.

The full original OLD observation/plan checks, listener/database binding,
protected metadata member checks, actual DB-open check and final request/process
checks remain. The first and second MStore guards remain separate. Service and
DB guards drop between them. The generic metadata/currentness APIs are unchanged.
All private callers were inspected: source discovery, S2 export retirement and
participant decision provenance run before the transport's ordinary service
latch. Their source-discovery claim is atomic logical ownership, not a retained
engine mutex. Registry/source-session helpers return before these rechecks;
network work is outside all of these guards. No publication-version guard is
held while waiting.

Four new genuine source-fixture definitions retain the original real TLS/S2
bootstrap and target image:

- At each of the two actual recheck sites, a competing thread takes the actual
  donor MStore only AFTER real OWN/member capture. The real helper must observe
  a timed lock failure. The first-busy callback releases that exact guard. The
  original retirement and unchanged remote NEW after-take cancellation/retry
  assertions must then complete.
- A negative cancels the actual original server caller after observed contention,
  with its original OWN live immediately before that cancellation and positive
  original OWN deadline remaining afterwards. Its original cancellation ancestry
  is preserved; the actual helper must return QueryCanceled.
- A negative holds that same mutex through the actual original OWN deadline. The
  actual helper must return QueryTimedOut. Both server and OWN clocks are recorded:
  near-coincident deadlines can make this a coupled-expiry observation rather
  than independent proof of OWN-only expiry. No shortened or renewed token is
  used to manufacture separation.

Negative controls await the observed exit of the SAME original recipient work
through the existing separate diagnostic scope, never reissuing business work.
They require original exporter/pin/manifest/native records and target state to
remain unchanged. Original RPC outcomes and bounded actual hook facts are saved
before assertions. Missing hook, actual contention, original error-class or
recipient completion evidence fails the intended control. Client timeout alone
does not count as proof that server work finished.

The per-registry hooks are cfg(test), one-shot callbacks outside registry and
engine guards. They receive original cancellation and clock DATA, not a Tx,
writer, Frozen permit, fabricated grant or mutable metadata store. The actual
competing holder uses the fixture's existing source MStore. At most a few facts
are retained; no per-probe polling trace or Error/guard is retained.

Static formatting/predicate preservation is not compilation, runtime acceptance,
timing improvement, cold continuation, operator completion or HA qualification.
The legacy concurrent path-based backup failure is separate and remains open;
its original test and production checks are unchanged by this batch.
