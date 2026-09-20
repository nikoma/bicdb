#!/usr/bin/env node

import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const inventory = readJson(join(root, "fixtures/postgresql-18/type-inventory.json"));
const policy = readJson(join(root, "fixtures/postgresql-18/type-classification-policy.json"));
const registrySource = readFileSync(join(root, "crates/bicdb-sql/src/pg_types.rs"), "utf8");
const registry = new Set(
  [...registrySource.matchAll(/pg_type!\(\s*"([^"]+)"/g)].map((match) => match[1]),
);
const fixtures = new Set(
  readdirSync(join(root, "fixtures/postgresql-18/type-diff"))
    .filter((name) => name.endsWith(".json"))
    .map((name) => readJson(join(root, "fixtures/postgresql-18/type-diff", name)).id),
);

if (inventory.oracle_image !== "postgres:18.4" || policy.target_version !== "18.4") {
  throw new Error("classification inputs must target PostgreSQL 18.4");
}

const byOid = new Map(inventory.types.map((type) => [type.oid, type]));
const general = new Set(policy.general_purpose_types);
const system = new Set(policy.system_supported_types);
const excluded = new Map(Object.entries(policy.internal_exclusions));
const rows = inventory.types.map(classify);
const unclassified = rows.filter(({ classification }) => classification === "unclassified");
if (unclassified.length > 0) {
  throw new Error(`unclassified PostgreSQL types: ${unclassified.map(({ name }) => name).join(", ")}`);
}

for (const name of [...general, ...system]) {
  if (!registry.has(name)) throw new Error(`classified supported type missing from registry: ${name}`);
}
for (const [name, evidence] of Object.entries(policy.fixture_evidence)) {
  for (const fixture of evidence) {
    if (!fixtures.has(fixture)) throw new Error(`${name} references missing fixture ${fixture}`);
  }
}
for (const name of general) {
  const family = name.endsWith("range") ? "range_family" : name;
  if (!(family in policy.fixture_evidence)) {
    throw new Error(`general-purpose type has no fixture evidence: ${name}`);
  }
}

const counts = countBy(rows, ({ classification }) => classification);
const report = {
  schema_version: 1,
  target_version: policy.target_version,
  oracle_image: inventory.oracle_image,
  approval: policy.approval,
  summary: {
    catalog_rows: rows.length,
    classified_rows: rows.length - unclassified.length,
    unclassified_rows: unclassified.length,
    general_purpose_roots: general.size,
    supported_general_purpose_roots: rows.filter(
      ({ classification }) => classification === "general_purpose_supported",
    ).length,
    approved_internal_exclusions: excluded.size,
    classifications: Object.fromEntries(Object.entries(counts).sort()),
  },
  approved_internal_exclusions: Object.fromEntries(excluded),
  types: rows,
};

writeFileSync(
  join(root, "reports/postgresql-18-type-classification.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);
writeFileSync(join(root, "docs/postgresql-18-type-classification.md"), renderMarkdown(report));
console.log(
  `PostgreSQL 18 type classification: ${rows.length}/${rows.length} rows, ${general.size}/${general.size} general-purpose roots, ${excluded.size} approved internal exclusions`,
);

function classify(type) {
  if (type.kind === "c") {
    return row(type, "system_relation_composite", false, policy.fixture_evidence.composite_family,
      "A generated pg_catalog relation row type, not a general-purpose scalar type.");
  }
  if (type.kind === "p") {
    return row(type, "pseudo_type_boundary", registry.has(type.name), policy.fixture_evidence.pseudo_family,
      "A PostgreSQL routine-signature pseudo-type with explicit declaration boundaries.");
  }
  if (type.name.startsWith("_")) {
    const element = byOid.get(type.element_oid);
    if (!element) return row(type, "unclassified", false, [], "Array element is absent from the oracle inventory.");
    const root = element.name;
    const internal = excluded.get(root);
    return row(
      type,
      internal ? "internal_array_out_of_scope" : "derived_array",
      internal ? false : registry.has(root),
      internal ? [] : policy.fixture_evidence.array_family,
      internal ? `Derived array of internal-only ${root}: ${internal}` : `Derived PostgreSQL array of ${root}.`,
    );
  }
  if (general.has(type.name)) {
    const key = type.name.endsWith("range") ? "range_family" : type.name;
    return row(type, "general_purpose_supported", registry.has(type.name), policy.fixture_evidence[key],
      "General-purpose PostgreSQL type covered by the parity matrix.");
  }
  if (system.has(type.name)) {
    return row(type, "system_type_supported", registry.has(type.name), policy.fixture_evidence.system_family,
      "PostgreSQL catalog, transaction, or reference type supported for ecosystem compatibility.");
  }
  if (excluded.has(type.name)) {
    return row(type, "internal_out_of_scope", false, [], excluded.get(type.name));
  }
  return row(type, "unclassified", false, [], "No classification policy matched this type.");
}

function row(type, classification, registryBacked, evidence, rationale) {
  return {
    oid: type.oid,
    name: type.name,
    kind: type.kind,
    category: type.category,
    classification,
    registry_backed: registryBacked,
    approved: classification !== "unclassified",
    evidence,
    rationale,
  };
}

function renderMarkdown(report) {
  const summary = report.summary;
  const exclusions = Object.entries(report.approved_internal_exclusions)
    .map(([name, rationale]) => `| \`${name}\` | ${rationale} |`)
    .join("\n");
  const counts = Object.entries(summary.classifications)
    .map(([classification, count]) => `| \`${classification}\` | ${count} |`)
    .join("\n");
  return `# PostgreSQL 18 Type Classification\n\n<!-- Generated by scripts/generate-pg18-type-classification.mjs. Do not edit directly. -->\n\nThis is the DT-1601 classification of the complete PostgreSQL 18.4 \`pg_type\` oracle inventory.\nIt distinguishes application types from derived arrays, pseudo-types, system relation composites,\nand internal index/catalog representations.\n\n## Result\n\n- Catalog rows classified: **${summary.classified_rows}/${summary.catalog_rows}**\n- General-purpose roots supported: **${summary.supported_general_purpose_roots}/${summary.general_purpose_roots}**\n- Unclassified rows: **${summary.unclassified_rows}**\n- Approved internal root exclusions: **${summary.approved_internal_exclusions}**\n\n| Classification | Rows |\n| --- | ---: |\n${counts}\n\n## Approved Internal Exclusions\n\n| Type | Rationale |\n| --- | --- |\n${exclusions}\n\nThe complete row-by-row evidence is in\n[\`reports/postgresql-18-type-classification.json\`](../reports/postgresql-18-type-classification.json).\nThe generator fails if an oracle row is unclassified, a supported root is absent from the canonical\nregistry, or required fixture evidence is missing.\n`;
}

function countBy(values, key) {
  const counts = {};
  for (const value of values) {
    const name = key(value);
    counts[name] = (counts[name] || 0) + 1;
  }
  return counts;
}

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}
