#!/usr/bin/env node

import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

const [outputJson, outputMarkdown, fixturesDir, databasePath, ...shardPaths] =
  process.argv.slice(2);

if (!outputJson || !outputMarkdown || !fixturesDir || !databasePath || shardPaths.length === 0) {
  console.error(
    "usage: aggregate-pg18-type-diff.mjs <report.json> <report.md> <fixtures-dir> <database-path> <shard.json>...",
  );
  process.exit(2);
}

const shards = shardPaths.map((path) => JSON.parse(readFileSync(path, "utf8")));
const first = shards[0];
const cases = shards.flatMap((report) => report.cases);
const ids = new Set();
const expectedIds = readdirSync(fixturesDir)
  .filter((name) => name.endsWith(".json"))
  .map((name) => JSON.parse(readFileSync(join(fixturesDir, name), "utf8")).id);

for (const report of shards) {
  if (report.target_version !== first.target_version) {
    throw new Error("type differential shards target different PostgreSQL versions");
  }
  if (report.postgres_host !== first.postgres_host || report.postgres_port !== first.postgres_port) {
    throw new Error("type differential shards target different PostgreSQL endpoints");
  }
}

for (const testCase of cases) {
  if (ids.has(testCase.id)) {
    throw new Error(`duplicate type differential fixture id: ${testCase.id}`);
  }
  ids.add(testCase.id);
}
if (
  expectedIds.length !== ids.size ||
  expectedIds.some((id) => !ids.has(id))
) {
  throw new Error("type differential shards do not cover the checked-in fixture set exactly");
}

const passedCases = cases.filter((testCase) => testCase.status === "passed").length;
const expectedDifferenceCases = cases.filter(
  (testCase) => testCase.expectation === "expected_difference",
).length;
const report = {
  mode: "postgres_diff",
  target_version: first.target_version,
  postgres_host: first.postgres_host,
  postgres_port: first.postgres_port,
  total_cases: cases.length,
  passed_cases: passedCases,
  failed_cases: cases.length - passedCases,
  expected_difference_cases: expectedDifferenceCases,
  elapsed_ms: shards.reduce((total, shard) => total + shard.elapsed_ms, 0),
  fixtures_dir: resolve(fixturesDir),
  path: resolve(databasePath),
  shards: shards.map((shard, index) => ({
    index: index + 1,
    total_cases: shard.total_cases,
    passed_cases: shard.passed_cases,
    failed_cases: shard.failed_cases,
    elapsed_ms: shard.elapsed_ms,
  })),
  cases,
};

const markdown = [
  "# PostgreSQL 18 Type Differential Report",
  "",
  `- Target: PostgreSQL ${report.target_version}`,
  `- Cases: ${report.passed_cases}/${report.total_cases} passed`,
  `- Expected differences: ${report.expected_difference_cases}`,
  `- Clean-state shards: ${report.shards.map((shard) => shard.total_cases).join(" + ")}`,
  `- Elapsed: ${report.elapsed_ms.toFixed(1)} ms`,
  "",
  "| Fixture | Expectation | Status | Detail |",
  "| --- | --- | --- | --- |",
  ...cases.map(
    (testCase) =>
      `| ${escapeMarkdown(testCase.id)} | ${testCase.expectation} | ${testCase.status} | ${escapeMarkdown(testCase.detail)} |`,
  ),
  "",
].join("\n");

writeFileSync(outputJson, `${JSON.stringify(report, null, 2)}\n`);
writeFileSync(outputMarkdown, markdown);

if (report.failed_cases > 0) {
  console.error(
    `PostgreSQL type differential failed ${report.failed_cases}/${report.total_cases} cases`,
  );
  process.exit(1);
}

function escapeMarkdown(value) {
  return String(value).replaceAll("\\", "\\\\").replaceAll("|", "\\|").replaceAll("\n", "<br>");
}
