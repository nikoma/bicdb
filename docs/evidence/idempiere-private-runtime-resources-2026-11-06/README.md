# Private application resource preparation — 2026-11-06

The original client-setup test could not find its accounting CSV beneath
`IDEMPIERE_HOME`. The built OSGi product and the upstream server-feature source
layout are separate. `prepare_runtime_resources.py` prepares a fresh private
home with the exact tracked `data`, `utils.unix` and `utils.windows` resources
from a required upstream commit. It excludes generated, untracked seed archives.
Application classes, tests, database contents and the upstream checkout are not
rewritten. Existing destination homes are refused rather than merged or removed.

The [qualification record](summary.json) identifies the helper source and exact
identical delivered contents, six passing unit tests, 171 original tracked
resource files verified after copying, and the original accounting CSV hash.
Tests cover binary fidelity, source independence, existing/related path
refusals, missing prerequisites, symlink rejection and generated archive
exclusion. An initial control rejected the generated seed archives before the
tracked-only selection was implemented. That rejection remains recorded.

This resolves the resource-preparation prerequisite. The original client-setup
application test must still run with the private home and retained provider /
test class provenance; the earlier failed test is not relabelled as passing.
No full application, database or production qualification follows from a file
copy. Follow the [runtime installation guide](../../oracle-idempiere-runtime-installation.md).
