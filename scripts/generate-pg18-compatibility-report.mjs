#!/usr/bin/env node

import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const differentialPath = resolve(
  process.env.PG18_TYPE_DIFF_REPORT ||
    join(repoRoot, "target/postgresql-18-type-diff/report.json"),
);
const roadmapPath = join(repoRoot, "docs/postgresql-18-data-type-parity-todo.md");
const fixtureDir = join(repoRoot, "fixtures/postgresql-18/type-diff");
const inventoryPath = join(repoRoot, "fixtures/postgresql-18/type-inventory.json");
const classificationPath = join(repoRoot, "reports/postgresql-18-type-classification.json");
const clientMatrixPath = join(repoRoot, "fixtures/postgresql-18/client-matrix.json");
const clientGauntletPath = resolve(
  process.env.PG18_CLIENT_GAUNTLET_REPORT ||
    join(repoRoot, "target/postgresql-client-gauntlet.json"),
);
const jsonOutput = join(repoRoot, "reports/postgresql-18-type-compatibility.json");
const markdownOutput = join(repoRoot, "POSTGRES_COMPATIBILITY.md");
const clientOutput = join(repoRoot, "docs/postgresql-client-matrix.md");

const sourceVersion = readCrateVersion(join(repoRoot, "crates/bicdb-core/Cargo.toml"));

// Refresh presentation without claiming a new differential or client run.
// Preserve the version and results recorded in the retained evidence JSON.
if (process.argv.includes("--render-retained")) {
  const retained = readJson(jsonOutput);
  writeFileSync(markdownOutput, renderCompatibility(retained));
  writeFileSync(clientOutput, renderClientMatrix(retained));
  process.exit(0);
}

const roadmap = readFileSync(roadmapPath, "utf8");
const inventory = readJson(inventoryPath);
const classification = readJson(classificationPath);
const differential = readJson(differentialPath);
const clientMatrix = readJson(clientMatrixPath);
const clientGauntlet = readJson(clientGauntletPath);
const version = readCrateVersion(join(repoRoot, "crates/bicdb-core/Cargo.toml"));
const roadmapItems = [...roadmap.matchAll(/^- \[([ x])\] (DT-\d+)\b/gm)].map(
  ([, mark, id]) => ({ id, status: mark === "x" ? "complete" : "pending" }),
);
const duplicateRoadmapIds = duplicates(roadmapItems.map(({ id }) => id));

if (duplicateRoadmapIds.length > 0) {
  throw new Error(`duplicate roadmap ids: ${duplicateRoadmapIds.join(", ")}`);
}
if (inventory.oracle_image !== "postgres:18.4" || inventory.server_version_num !== 180004) {
  throw new Error("the checked-in type inventory is not from PostgreSQL 18.4");
}
if (
  classification.target_version !== "18.4" ||
  classification.summary.catalog_rows !== inventory.types.length ||
  classification.summary.classified_rows !== inventory.types.length ||
  classification.summary.unclassified_rows !== 0 ||
  classification.summary.supported_general_purpose_roots !==
    classification.summary.general_purpose_roots
) {
  throw new Error("the PostgreSQL 18 type classification is incomplete");
}
if (clientMatrix.target_version !== "18.4") {
  throw new Error("the client matrix targets a different PostgreSQL version");
}
const declaredCrossTargetClients = clientMatrix.automated
  .filter(({ scope }) => scope === "cross_target")
  .map(({ id }) => id);
if (
  clientGauntlet.target_version !== clientMatrix.target_version ||
  clientGauntlet.status !== "passed" ||
  !sameSet(
    declaredCrossTargetClients,
    clientGauntlet.clients.filter(({ status }) => status === "passed").map(({ id }) => id),
  )
) {
  throw new Error("the client gauntlet report does not pass the declared cross-target matrix");
}

const fixtures = readdirSync(fixtureDir)
  .filter((name) => name.endsWith(".json"))
  .sort()
  .map((name) => ({ name, ...readJson(join(fixtureDir, name)) }));
const fixtureIds = fixtures.map(({ id }) => id);
const caseIds = differential.cases.map(({ id }) => id);

if (duplicates(fixtureIds).length > 0) {
  throw new Error(`duplicate differential fixture ids: ${duplicates(fixtureIds).join(", ")}`);
}
if (!sameSet(fixtureIds, caseIds)) {
  throw new Error("the differential report does not cover the checked-in fixture set exactly");
}
if (
  differential.target_version !== "18.4" ||
  differential.total_cases !== fixtures.length ||
  differential.passed_cases !== fixtures.length ||
  differential.failed_cases !== 0 ||
  differential.expected_difference_cases !== 0
) {
  throw new Error("the PostgreSQL 18 type differential report is not a zero-difference pass");
}
if (fixtures.some(({ expectation }) => expectation !== "match")) {
  throw new Error("the PostgreSQL 18 type matrix contains an expected-difference fixture");
}

const completeItems = roadmapItems.filter(({ status }) => status === "complete");
const pendingItems = roadmapItems.filter(({ status }) => status === "pending");
const inventoryKinds = Object.fromEntries(
  Object.entries(countBy(inventory.types, ({ kind }) => kind)).sort(([left], [right]) =>
    left.localeCompare(right),
  ),
);
const evidence = {
  schema_version: 1,
  bicdb_version: version,
  target: {
    postgres_version: differential.target_version,
    oracle_image: inventory.oracle_image,
    server_version_num: inventory.server_version_num,
  },
  roadmap: {
    total: roadmapItems.length,
    complete: completeItems.length,
    pending: pendingItems.length,
    pending_items: pendingItems.map(({ id }) => id),
  },
  oracle_inventory: {
    catalog_rows: inventory.types.length,
    kinds: inventoryKinds,
    note: "Catalog rows include arrays, pseudo-types, and system relation composites; they are not a count of general-purpose column types.",
  },
  type_classification: classification.summary,
  type_differential: {
    status: "passed",
    fixture_count: differential.total_cases,
    passed: differential.passed_cases,
    failed: differential.failed_cases,
    expected_differences: differential.expected_difference_cases,
    clean_state_shards: differential.shards?.map(({ total_cases }) => total_cases) || [],
    cases: differential.cases.map(({ id, status }) => ({ id, status })),
  },
  support_guarantee: {
    status: pendingItems.some(({ id }) => id.startsWith("DT-16"))
      ? "matrix_passed_acceptance_pending"
      : "accepted",
    scope: "Behavior exercised by the checked-in PostgreSQL 18.4 differential, dump/restore, catalog, storage, and protocol gates.",
    excludes: "Unexercised PostgreSQL SQL, extension, operational, and catalog behavior.",
  },
  clients: {
    ...clientMatrix,
    evidence: clientGauntlet,
  },
};

writeFileSync(jsonOutput, `${JSON.stringify(evidence, null, 2)}\n`);
writeFileSync(markdownOutput, renderCompatibility(evidence));
writeFileSync(clientOutput, renderClientMatrix(evidence));

function renderCompatibility(report) {
  const crossTarget = report.clients.automated.filter(({ scope }) => scope === "cross_target");
  const bicdbOnly = report.clients.automated.filter(({ scope }) => scope === "bicdb_regression");
  const acceptanceStatus =
    report.roadmap.pending === 0
      ? `The implementation roadmap is ${report.roadmap.complete}/${report.roadmap.total} complete. All PostgreSQL 18
type-parity acceptance gates and standing guardrails are closed. BicDB therefore publishes complete
parity for the exercised type surface described by this report; this remains narrower than complete
PostgreSQL implementation parity.`
      : `The implementation roadmap is ${report.roadmap.complete}/${report.roadmap.total} complete. Final
PostgreSQL 18 type certification is still pending these acceptance items:

${report.roadmap.pending_items.map((id) => `- \`${id}\``).join("\n")}

The standing \`DT-0001\` through \`DT-0007\` guardrails remain open until the final gate because they
must be re-verified across every family. Until those items and \`DT-1601\` through \`DT-1607\` close,
describe BicDB as having a passing implemented type matrix, not complete PostgreSQL parity.`;
  return `# BicDB PostgreSQL Compatibility

<!-- Generated by scripts/generate-pg18-compatibility-report.mjs. Do not edit directly. -->

Current source version: **BicDB ${sourceVersion}**. BicDB targets PostgreSQL
${report.target.postgres_version} type behavior within the tested compatibility surface; it is not a
complete PostgreSQL implementation.

## Retained evidence

The matrix below was recorded for **BicDB ${report.bicdb_version}**. Its original
version and results are preserved in the checked-in JSON report. Updating this
page to identify the current source version does not rerun or recertify that
historical matrix. Validate your application against the release you deploy.

Regenerate the presentation without changing evidence with
\`node scripts/generate-pg18-compatibility-report.mjs --render-retained\`.
A fresh evidence run requires the differential and client-gauntlet reports used
by the generator's default mode.

| Evidence | Result |
| --- | --- |
| PostgreSQL oracle | \`${report.target.oracle_image}\` (\`server_version_num=${report.target.server_version_num}\`) |
| Oracle \`pg_type\` inventory | ${report.oracle_inventory.catalog_rows} catalog rows |
| Classified oracle rows | ${report.type_classification.classified_rows}/${report.type_classification.catalog_rows}; ${report.type_classification.unclassified_rows} unclassified |
| General-purpose type roots | ${report.type_classification.supported_general_purpose_roots}/${report.type_classification.general_purpose_roots} supported |
| Approved internal exclusions | ${report.type_classification.approved_internal_exclusions} |
| Type differential | ${report.type_differential.passed}/${report.type_differential.fixture_count} passed; ${report.type_differential.expected_differences} expected differences |
| Clean-state shard sizes | ${report.type_differential.clean_state_shards.join(" + ")} |
| Type-parity roadmap | ${report.roadmap.complete}/${report.roadmap.total} complete; ${report.roadmap.pending} remain |
| Cross-target automated clients | ${crossTarget.length} against both BicDB and PostgreSQL 18.4 |
| BicDB-only driver regressions | ${bicdbOnly.length} |

The ${report.oracle_inventory.catalog_rows}-row inventory is a catalog oracle, not a supported-type
count. It includes ${report.oracle_inventory.kinds.b} base, ${report.oracle_inventory.kinds.c} composite,
${report.oracle_inventory.kinds.r} range, ${report.oracle_inventory.kinds.m} multirange, and
${report.oracle_inventory.kinds.p} pseudo-type rows, including system relation composites and arrays.

## Published Type Guarantee

BicDB guarantees PostgreSQL 18.4-compatible behavior only where the checked-in gates exercise it.
For a supported type, applicable gates cover declared identity and typmods, typed durable storage,
input and canonical output, casts, comparison and indexing, catalog metadata, text and binary pgwire
formats, arrays and user-defined containers, schema evolution, restart, and dump/restore behavior.

The zero-difference matrix currently covers ${report.type_differential.fixture_count} fixture groups,
including:

- Boolean, integer, floating-point, exact numeric, money, character, bit, and binary families.
- Date, time, time zone, timestamp, interval, UUID, JSON, JSONB, jsonpath, and XML.
- Full-text search, network, geometric, OID/catalog-reference, transaction, snapshot, and LSN types.
- Arrays, composites, enums, domains, ranges, multiranges, shell types, and registered base types.
- Type catalogs, attributes, information schema, casts, common-type selection, statistics,
  operator classes, ALTER COLUMN TYPE, dump/restore, and multi-database OID isolation.

Every matrix fixture has expectation \`match\`; there are no approved type differences hidden in the
passing count. The compact machine-readable evidence is
[\`reports/postgresql-18-type-compatibility.json\`](reports/postgresql-18-type-compatibility.json).
The generated [type classification](docs/postgresql-18-type-classification.md) accounts for every
PostgreSQL 18.4 oracle row and publishes each approved internal-only exclusion.

## Acceptance Status

${acceptanceStatus}

## Client Matrix

The generated [PostgreSQL client matrix](docs/postgresql-client-matrix.md) is authoritative for tested
driver versions, formats, and scope. The cross-target gauntlet covers ${crossTarget.map(({ name }) => name).join(", ")}.
${bicdbOnly.map(({ name }) => name).join(", ")} has a separate in-process BicDB regression. Prisma and
GUI clients remain explicitly outside the passing automated matrix.

## Compatibility Boundaries

- Type parity does not imply PostgreSQL optimizer, replication, high availability, extension, or
  exhaustive SQL-language parity.
- Binary COPY, client-certificate authentication, SCRAM channel binding, and isolation stronger than
  BicDB's documented \`READ COMMITTED\` implementation remain outside this type guarantee.
- Unsupported behavior must fail clearly; it must not silently coerce values to text, compare typed
  values as text, or report OID 25 unless PostgreSQL does so.
- \`vector\` is a BicDB/pgvector-style extension and is not part of the stock PostgreSQL 18 inventory.

## Reproduce The Evidence

\`\`\`bash
CARGO_BUILD_JOBS=10 RUST_TEST_THREADS=1 ./scripts/pg18-type-diff.sh
./scripts/client-gauntlet.sh
PG18_TYPE_DIFF_REPORT=target/postgresql-18-type-diff/report.json \\
PG18_CLIENT_GAUNTLET_REPORT=target/postgresql-client-gauntlet.json \\
  node scripts/generate-pg18-compatibility-report.mjs
./scripts/pg18-dump-restore.sh
\`\`\`

The shared oracle defaults to \`postgres:18.4\` at \`127.0.0.1:55432/compat\` with user and
password \`postgres\`. Override it with \`PG18_IMAGE\`, \`PG18_BIND_HOST\`,
\`PG18_HOST_PORT\`, \`PG18_DB\`, \`PG18_USER\`, \`PG18_PASSWORD\`,
\`PG18_CONTAINER_NAME\`, and \`PG18_COMPOSE_PROJECT\`.

The authoritative implementation and remaining-work ledger is
[\`docs/postgresql-18-data-type-parity-todo.md\`](docs/postgresql-18-data-type-parity-todo.md).
Operational details remain in the focused server, security, row-level-security, constraints,
large-result, and dump/restore documentation under \`docs/\`.
`;
}

function renderClientMatrix(report) {
  const rows = report.clients.automated
    .map(
      (client) =>
        `| ${client.name} | ${client.version} | ${client.scope === "cross_target" ? "BicDB + PostgreSQL 18.4" : "BicDB regression"} | ${client.formats} | ${client.coverage} |`,
    )
    .join("\n");
  const manualRows = report.clients.manual
    .map(
      (client) =>
        `| ${client.name} | ${client.version} | ${client.status} | ${client.coverage} |`,
    )
    .join("\n");
  return `# PostgreSQL Client Matrix

<!-- Generated by scripts/generate-pg18-compatibility-report.mjs. Do not edit directly. -->

This is the retained client surface recorded for BicDB ${report.bicdb_version} against PostgreSQL
${report.target.postgres_version}. Current source version: BicDB ${sourceVersion}; this presentation
update does not constitute a new client run. \`cross_target\` entries run the same assertions against BicDB and
the pinned PostgreSQL oracle. \`bicdb_regression\` entries are repository tests against BicDB only and
must not be presented as a cross-target gauntlet.

## Automated

| Client | Version | Scope | Formats | Coverage |
| --- | --- | --- | --- | --- |
${rows}

## Manual Or Known Gaps

| Client | Version | Status | Coverage |
| --- | --- | --- | --- |
${manualRows}

Run the cross-target matrix with \`./scripts/client-gauntlet.sh\`. Run the tokio-postgres BicDB
regression with:

\`\`\`bash
cargo test -p bicdb-pgwire tokio_postgres_basic_orm_client_gauntlet -- --nocapture
\`\`\`

See [\`docs/postgresql-client-gauntlet.md\`](postgresql-client-gauntlet.md) for prerequisites and GUI
smoke instructions.
`;
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function readCrateVersion(path) {
  const match = readFileSync(path, "utf8").match(/^version = "([^"]+)"/m);
  if (!match) throw new Error(`missing crate version in ${path}`);
  return match[1];
}

function duplicates(values) {
  const seen = new Set();
  const duplicateValues = new Set();
  for (const value of values) {
    if (seen.has(value)) duplicateValues.add(value);
    seen.add(value);
  }
  return [...duplicateValues];
}

function sameSet(left, right) {
  return left.length === right.length && left.every((value) => new Set(right).has(value));
}

function countBy(values, keyFor) {
  const counts = {};
  for (const value of values) {
    const key = keyFor(value);
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}
