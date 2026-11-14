# Release capability declarations and placement preflight

`capabilities/catalog.json` declares narrowly scoped implementation capabilities
for legacy shipping, native streaming HA, distributed ranges, Cell HA and
application fleets. `scripts/package-cli-release.py` now includes
`CAPABILITIES.json` in newly packaged CLI archives. This does not modify already
published releases, existing binaries or deployment agents.

Each assertion keeps four independent fields:

- `implemented`: `supported`, `unsupported` or `conditional`, with hashed source
  paths, scope and conditions.
- `tested`: executed evidence for the exact artifact, not the existence of test
  definitions.
- `qualified`: completed production qualification for that artifact, topology,
  configuration and failure model.
- `deployed`: an independently observed operational deployment identity.

The initial v1 generator writes **`not_established`** with empty evidence for
all of the last three fields. It lists test definitions separately. It never
converts a passing source test, historical benchmark, plan, or public API into
qualification or deployment. Its validator rejects attempts to assert positive
qualification/deployment through this v1 format. A future externally authenticated
evidence format is necessary to grant those statuses; a local JSON edit is not
an authority mechanism. This first implementation is a conservative release
preflight foundation, not a complete placement controller or evidence registry.

## Artifact identity and trust

Generation requires a clean tracked checkout and an exact full HEAD revision,
a tracked catalog and matching binary/source version. It records source revision,
version, target, binary SHA-256 and catalog SHA-256. Source evidence must be regular committed blobs at that exact revision;
untracked files and symlink escapes are refused. Their committed bytes are
hashed. Packaging generates the manifest **after stripping** the staged binary,
so `BUILD.json` and `CAPABILITIES.json` refer to the bytes actually archived.
Generate standalone declarations from a committed clean tree:

```sh
python3 scripts/capability_manifest.py generate \
  --binary /path/to/immutable/bicdb \
  --revision FULL_40_HEX_SOURCE_COMMIT \
  --output /path/to/new/CAPABILITIES.json
```

Existing-output replacement is refused. The generator checks version and hashes
bytes, but cannot prove that a supplied executable was compiled from the claimed
source or feature set. `source_build_binding` explicitly records
`operator_asserted_not_attested`. Release builders must provide trusted build
provenance; matching `--version` alone is not provenance or a signature. This
catalog targets the ordinary CLI package, not arbitrary embedded/custom builds.
The separate `bic` application-host executable is not packaged with `bicdb`;
worker replacement is therefore unsupported in this artifact even though its
implementation exists in the repository. `component` and `entrypoint` record
this boundary. Architecture `target` is also an explicit operator assertion
(`target_binding: operator_asserted_not_inspected`), not ELF inspection.

A consumer must obtain the expected manifest digest and source revision from a
trusted publisher or independently authenticated release provenance. Reading an
expected hash from an untrusted adjacent file provides no protection against
replacement of both files. The current manifest is unsigned; archive checksums
establish byte identity only relative to the trusted publication they came from.

## Fail-closed requirements check

For example, an application requiring atomic multi-range SQL must fail before
being placed on distributed ranges:

```sh
python3 scripts/capability_manifest.py check \
  --manifest /release/CAPABILITIES.json \
  --binary /release/bicdb \
  --revision EXPECTED_FULL_SOURCE_COMMIT \
  --expected-sha256 TRUSTED_MANIFEST_SHA256 \
  --mode distributed_ranges \
  --require general_cross_range_transactions \
  --level qualified
```

This exits **2** with machine-readable `allowed: false`. Exit 0 means only that
the requested artifact-level declarations passed. Unknown modes/capabilities,
conditional support without verified configuration, mismatched source revision,
modified manifest/binary and missing required evidence refuse admission. The
safe default level is `qualified`; consequently every production-qualification
request is refused by the current evidence-free v1 format. Multiple `--require`
arguments are conjunctive. A caller may explicitly choose `--level implemented`
for development planning, but that never grants production authority.

Carrier, Sage, BicDB Cloud or another orchestrator can call this preflight before
placement and stop on a nonzero result. They are not integrated automatically
by this change. In particular, release declarations cannot verify the live mode,
RF/voter configuration, synchronous timeout policy, recovered range leadership,
Cell evidence lease, resource placement, failure domains or deployed binary.
Those require separate live/configuration checks. Conditional claims are denied
even at the `implemented` level rather than silently trusting supplied prose.

The `distributed_ranges.automatic_range_rebalancing` implementation declaration
covers the legacy range/event controller. It does not cover born-v2 enforced
native DATA relocation: the native guard refuses legacy membership/cleanup,
and the registered replica receiver supplies neither native image/joint
transfer nor an automatic native placement controller. A local component test,
receiver-ready status or implemented one-range write capability cannot establish
that missing path. `native_streaming_ha` describes whole-database single-primary
HA; its declarations do not qualify native range handoff. Transparent SQL,
general cross-range transactions/joins, physical scale certification and
critical-workload admission retain their separate catalog limitations.

The separate `native_automatic_range_rebalancing` and
`native_range_cold_handoff_recovery` declarations are unsupported. A deployment
requiring enforced native movement must request those exact capabilities; the
legacy controller declaration cannot satisfy them. The native handoff driver's
local participant handles and same-process retry custody do not supply a normal
remote operator, restart reconstruction or physical qualification.

`native_range_broker_transactions` is also unsupported. Born-native write
admission rejects pending broker publishes before Prepare/application WAL,
including broker-only and mixed row/broker transactions. This affects workflows
and required mutation-audit paths that buffer broker messages. It does not mean
that a native row acknowledgement included a locally committed, unreplicated
broker publish. Record-audit and SyncOps WAL sidecars are distinct and still need
their own replication/recovery evidence. The catalog lists the existing refusal
test as a definition; it does not claim that this documentation update executed
or qualified the test.

The manifest has no database or key capability. It cannot bypass Cell admission,
writer fencing, normal SQL authorization or the engine's unsupported-operation
checks. Logical locality is an optimization; a placement tool must not advertise
atomic multi-range SQL merely because an app usually accesses one tenant.

## Validation

`python3 -m unittest discover -s scripts/platform -p 'test_*.py'` includes the
manifest regressions in the existing CI workflow. Tests exercise clean-source
identity, missing/conditional/unsupported capability refusal, qualified evidence
refusal, source and binary substitution, manifest tampering, wrong versions,
unsafe evidence paths and production catalog references. No generated fixture
binary or test assertion qualifies a real release.
