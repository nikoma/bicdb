// Multi-tenant RLS sync, end to end: ONE master database with PostgreSQL
// row-level-security policies; two users' browsers each sync exactly their
// RLS view of it.
//
// Proves: per-user visibility (alice never receives bob's rows), shared
// unpoliced tables reach everyone, authorized client writes flow back into
// the master under the user's session, an UNAUTHORIZED write (alice forging
// owner='bob') is rejected by WITH CHECK and visibly reverted on her own
// device, and an ownership transfer propagates as revoke+grant.
// Requires `cargo build -p bicdb-cli`.
import { spawn } from "node:child_process";
import { join, resolve } from "node:path";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { serve } from "./serve.mjs";

const TOKEN = "e2e-rls-token";
const ADMIN = "e2e-rls-admin";
const COMPOSE_MS = 2600; // composer interval is 2s; wait a tick + margin

function assert(cond, message) {
  if (!cond) {
    console.error(`FAIL: ${message}`);
    process.exit(1);
  }
}
const tick = () => new Promise((r) => setTimeout(r, COMPOSE_MS));

const pkgRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const bicdb = resolve(pkgRoot, "../../target/debug/bicdb");
const root = mkdtempSync(`${tmpdir()}/bicdb-rls-e2e-`);

writeFileSync(
  join(root, "compose.json"),
  JSON.stringify({
    tables: [
      { table: "announcements", pk: "id" },
      { table: "docs", pk: "id" },
    ],
    interval_seconds: 2,
  }),
);

const server = spawn(bicdb, [
  "sync-serve", root,
  "--host", "127.0.0.1", "--port", "0",
  "--token", TOKEN, "--admin-token", ADMIN,
  "--rls-compose", join(root, "compose.json"),
]);
const serverUrl = await new Promise((ready, fail) => {
  let out = "";
  server.stdout.on("data", (chunk) => {
    out += chunk;
    const match = out.match(/listening on (\S+)/);
    if (match) ready(`http://${match[1]}`);
  });
  server.stderr.on("data", (c) => process.stderr.write(c));
  server.on("exit", (code) => fail(new Error(`server exited early: ${code}`)));
  setTimeout(() => fail(new Error("server startup timeout")), 15_000);
});
console.log("rls sync server at", serverUrl);

const adminRequest = async (sql) => {
  const response = await fetch(`${serverUrl}/v1/master/sql`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${ADMIN}` },
    body: JSON.stringify({ sql }),
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error);
  return payload;
};
const adminSql = async (sql) => (await adminRequest(sql)).result;

const { server: statics, port } = await serve();
const browser = await chromium.launch();
try {
  // --- master schema, policies, seed data --------------------------------
  await adminSql([
    "CREATE TABLE announcements (id INT PRIMARY KEY, body TEXT)",
    "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, body TEXT)",
    "INSERT INTO announcements VALUES (1, 'welcome everyone')",
    "INSERT INTO docs VALUES (1, 'alice', 'alice private notes')",
    "INSERT INTO docs VALUES (2, 'bob', 'bob secret plan')",
    "ALTER TABLE docs ENABLE ROW LEVEL SECURITY",
    "ALTER TABLE docs FORCE ROW LEVEL SECURITY",
    `CREATE POLICY docs_select ON docs FOR SELECT
       USING (current_setting('carrier.current_user', true) = owner)`,
    `CREATE POLICY docs_insert ON docs FOR INSERT
       WITH CHECK (current_setting('carrier.current_user', true) = owner)`,
    `CREATE POLICY docs_update ON docs FOR UPDATE
       USING (current_setting('carrier.current_user', true) = owner)
       WITH CHECK (current_setting('carrier.current_user', true) = owner)`,
    `CREATE POLICY docs_delete ON docs FOR DELETE
       USING (current_setting('carrier.current_user', true) = owner)`,
  ]);

  const url = `http://127.0.0.1:${port}/test/sync.html`;
  const run = async (context, scope, args) => {
    const page = await context.newPage();
    page.on("pageerror", (error) => console.error("[pageerror]", error));
    await page.goto(url);
    const result = await page.evaluate(
      (a) => window.__sync(a),
      { serverUrl, scope, token: TOKEN, phase: "rls", ...args },
    );
    await page.close();
    return result;
  };
  const DOCS = "SELECT id, owner, body FROM docs ORDER BY id";

  const alice = await browser.newContext();
  const bob = await browser.newContext();

  // First sync creates the scopes; the composer fills them on its next tick.
  await run(alice, "user-alice", { sql: DOCS });
  await run(bob, "user-bob", { sql: DOCS });
  await tick();

  // --- per-user visibility ------------------------------------------------
  const a1 = await run(alice, "user-alice", { sql: DOCS });
  assert(
    JSON.stringify(a1.rows) === JSON.stringify([[1, "alice", "alice private notes"]]),
    `alice sees exactly her doc, got ${JSON.stringify(a1.rows)}`,
  );
  const aAnn = await run(alice, "user-alice", { sql: "SELECT body FROM announcements" });
  assert(aAnn.rows?.[0]?.[0] === "welcome everyone", "alice sees the shared announcement");
  const b1 = await run(bob, "user-bob", { sql: DOCS });
  assert(
    JSON.stringify(b1.rows) === JSON.stringify([[2, "bob", "bob secret plan"]]),
    `bob sees exactly his doc, got ${JSON.stringify(b1.rows)}`,
  );
  console.log("visibility ok: alice ->", JSON.stringify(a1.rows), " bob ->", JSON.stringify(b1.rows));

  // --- authorized client write flows back to master ----------------------
  await run(alice, "user-alice", {
    statements: ["INSERT INTO docs VALUES (3, 'alice', 'written on device')"],
    sql: DOCS,
  });
  await tick();
  const masterInspection = await adminRequest([
    "BEGIN",
    "ALTER TABLE docs NO FORCE ROW LEVEL SECURITY",
    "SELECT id FROM docs WHERE owner = 'alice' ORDER BY id",
    "ALTER TABLE docs FORCE ROW LEVEL SECURITY",
    "COMMIT",
  ]);
  const masterDocs = masterInspection.results[2];
  assert(
    JSON.stringify(masterDocs.rows) === JSON.stringify([[1], [3]]),
    `master accepted alice's device write, got ${JSON.stringify(masterDocs.rows)}`,
  );
  const b2 = await run(bob, "user-bob", { sql: DOCS });
  assert(b2.rows.length === 1, "bob still sees only his doc");
  console.log("device write-back ok: master has alice's doc 3, bob unaffected");

  // --- UNAUTHORIZED write: alice forges a doc owned by bob ----------------
  await run(alice, "user-alice", {
    statements: ["INSERT INTO docs VALUES (4, 'bob', 'forged for bob')"],
    sql: DOCS,
  });
  await tick();
  const a2 = await run(alice, "user-alice", { sql: DOCS });
  assert(
    !a2.rows.some((row) => row[0] === 4),
    `forged row was reverted on alice's own device, got ${JSON.stringify(a2.rows)}`,
  );
  const b3 = await run(bob, "user-bob", { sql: DOCS });
  assert(
    !b3.rows.some((row) => row[0] === 4),
    "bob never received the forged row",
  );
  console.log("forgery rejected ok: WITH CHECK blocked it, alice's cache reverted");

  // --- ownership transfer = revoke (alice) + grant (bob) ------------------
  await adminSql([
    "BEGIN",
    "ALTER TABLE docs NO FORCE ROW LEVEL SECURITY",
    "DELETE FROM docs WHERE id = 1",
    "INSERT INTO docs VALUES (1, 'bob', 'alice private notes')",
    "ALTER TABLE docs FORCE ROW LEVEL SECURITY",
    "COMMIT",
  ]);
  await tick();
  const a3 = await run(alice, "user-alice", { sql: DOCS });
  assert(
    JSON.stringify(a3.rows.map((r) => r[0])) === JSON.stringify([3]),
    `doc 1 revoked from alice, got ${JSON.stringify(a3.rows)}`,
  );
  const b4 = await run(bob, "user-bob", { sql: DOCS });
  assert(
    JSON.stringify(b4.rows.map((r) => r[0])) === JSON.stringify([1, 2]),
    `doc 1 granted to bob, got ${JSON.stringify(b4.rows)}`,
  );
  console.log("revoke/grant ok: alice ->", JSON.stringify(a3.rows.map((r) => r[0])),
    " bob ->", JSON.stringify(b4.rows.map((r) => r[0])));

  // --- schema migration on the master propagates to composed scopes -------
  // Expand-phase migration (ADD COLUMN, nullable, no default) while user
  // scopes already exist: the composer must ALTER the scope tables, then
  // values written under the new column flow both directions.
  await adminSql("ALTER TABLE docs ADD COLUMN priority INT");
  await adminSql([
    "BEGIN",
    "ALTER TABLE docs NO FORCE ROW LEVEL SECURITY",
    "UPDATE docs SET priority = 9 WHERE id = 2",
    "ALTER TABLE docs FORCE ROW LEVEL SECURITY",
    "COMMIT",
  ]);
  await tick();
  const bMigrated = await run(bob, "user-bob", {
    sql: "SELECT id, priority FROM docs ORDER BY id",
  });
  assert(
    JSON.stringify(bMigrated.rows) === JSON.stringify([[1, null], [2, 9]]),
    `bob's cache got the new column + server value, got ${JSON.stringify(bMigrated.rows)}`,
  );
  // Device-side write into the migrated column pushes back under policy.
  await run(alice, "user-alice", {
    statements: ["UPDATE docs SET priority = 1 WHERE id = 3"],
    sql: "SELECT id, priority FROM docs ORDER BY id",
  });
  await tick();
  const priorityInspection = await adminRequest([
    "BEGIN",
    "ALTER TABLE docs NO FORCE ROW LEVEL SECURITY",
    "SELECT priority FROM docs WHERE id = 3",
    "ALTER TABLE docs FORCE ROW LEVEL SECURITY",
    "COMMIT",
  ]);
  const masterPriority = priorityInspection.results[2];
  assert(
    masterPriority.rows[0]?.[0] === 1,
    `alice's device write to the migrated column reached the master, got ${JSON.stringify(masterPriority.rows)}`,
  );
  console.log("migration ok: ADD COLUMN reached both scopes; new-column writes flow both ways");

  console.log(
    "rls-e2e: OK (visibility, write-back, forgery rejection, revoke/grant, migration)",
  );
} finally {
  await browser.close();
  statics.close();
  server.kill();
  rmSync(root, { recursive: true, force: true });
}
