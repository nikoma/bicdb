# Native metadata destination identity

This source-only change reserves unpublished cluster-data protocol 24. It is
not a rolling-upgrade protocol or an executed stale-instance finding. Published
mainline/older protocol evidence remains at its original version.

Countable metadata Vote, Append, fresh read probe/acquisition, Host MQ and
recovery-role acquisition requests carry an expected destination native birth,
including its incarnation and bootstrap domain. The sender captures that data
from the same topology snapshot as the address and TLS pin, and verifies its
incarnation matches the declared member. This is routing/refusal data, never a
constructor for quorum or membership authority.

The listener compares the request against an immutable identity captured from
the actual native DB while opening the service. This happens before topology,
service, database or metadata latches. An enforced actual database refuses a
countable metadata request that omits native identity. A copied topology cannot
change the captured physical birth, and the existing actual-open/health fence
also remains mandatory. Initial CONFIG/birth discovery keeps its distinct
provisional purpose and cannot expose votes or a general service by weak expiry.

Replies carry the actual captured receiver birth. The client compares it with
the original expected destination before constructing a transport participant;
reusing the same TLS certificate cannot relabel an old physical incarnation as
the expected replacement. Legacy unbound behavior is retained when native
identity is absent and the actual receiver is unenforced.

At attachment, an enforced listener rejects a generic metadata backend and a
backend owned by another actual database/open. Vote/Append/probe processing
rechecks this association under its actual metadata guard. A metadata provider
must share that same consensus Arc and binds the actual-open fence once; every
later acquired provider guard verifies the association. No metadata lock takes
a database read/write lock to perform these checks. This covers a foreign inner
metadata-store replacement before it can produce a countable result.

The new mTLS control is unexecuted source. Four real metadata voters start with
n0 metadata-only and data voters n1/n2/n3. A real quorum commits n0 retirement,
actual higher-incarnation factory enrollment and subsequent promotion while the
old n0 receives no later metadata. A separate actual replacement endpoint uses
the same certificate. After its healthy vote response, a test delivery redirect
sends the genuine current birth-bound request to the retained old endpoint while
its service, DB and metadata latches are held. The expected result is an early
identity refusal and unchanged old metadata bytes. The control also covers an
unspecified destination identity, and generic/foreign attachment refusals.

This adds no data Ready, Serving, Strong-read, physical-failure, Azure-causality
or operator-completion claim. Bootstrap enrollment controller authorization and
the unfinished abort protocol remain separate dependencies.
