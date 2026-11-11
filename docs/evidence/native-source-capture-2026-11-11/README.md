# Native source capture component evidence — 2026-11-11

Status: **PASSED component test; end-to-end handoff NOT TESTED**.

Exact tested private source: `1d6dbe40de2c4e3cfbd7f2fad7c8a4a02bf8e9fc`
(nominal package version `1.0.466-beta`, not a new published build). The final
source tree was clean during the test. Later integrated source changes are not
covered by this result.

The focused test uses an actual fsync-enabled `server_paged` database and native
empty-range genesis. It commits a metadata learner transition after a load-only
update, obtains durable old-voter handoff promises, validates the native lineage
against that transition, and invokes the production `RangeImageCapture::begin`,
export, seal and completion paths. Negative assertions reject changed bootstrap,
span, epoch, voter role, credentials, schema, restore lineage and source identity.
The tentative staging authorization borrows the live capture. Completion retains
the database/namespace/lineage pins and exclusive range freeze: an ordinary
mutation is refused until the completed source owner is dropped.

The run passed **1 test**, with 571 filtered out. It took 62.016 seconds including
compilation; minimum observed available memory was 23,980,879,872 bytes. There
were no resource pauses or interruptions. The exact command, source identity,
raw output and resource samples are retained here. Core used opt-level 0/debug 0
with release dependencies, jobs=1 and incremental compilation disabled; this is
not a performance measurement.

The range is empty at certified index zero. The test's distinct durable
RangeWriteStores provide component promise evidence; this is not a complete
networked ClusterDataNodeService handoff. Nonempty native certified-prefix
production, target row installation/verification, current-quorum recovery,
activation/cleanup, source failure, strong-read readiness and physical scale-out
remain separate unpassed gates. A deserialized seal cannot construct the opaque
completed source owner, and neither source authorization grants target readiness
or membership. The original Azure qualification remains FAILED.
