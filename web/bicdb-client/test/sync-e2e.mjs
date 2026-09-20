// End-to-end sync convergence: real `bicdb sync-serve` + two isolated
// browser contexts ("devices" with separate OPFS).
//
// Flow: the server composes a working set (schema + announcement) through
// the admin SQL endpoint -> device A bootstraps it from offset zero, writes
// a note, syncs -> device B bootstraps, must see the announcement AND A's
// note, writes its own, syncs -> device A syncs again and must converge on
// all three rows. Requires `cargo build -p bicdb-cli` beforehand.
import { spawn } from "node:child_process";
import { join, resolve } from "node:path";
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { serve } from "./serve.mjs";

const TOKEN = "e2e-sync-token";
const ADMIN = "e2e-admin-token";
const SCOPE = "hub";

function assert(cond, message) {
  if (!cond) {
    console.error(`FAIL: ${message}`);
    process.exit(1);
  }
}

const pkgRoot = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const bicdb = resolve(pkgRoot, "../../target/debug/bicdb");
const root = mkdtempSync(`${tmpdir()}/bicdb-sync-e2e-`);

// --- spawn the sync server on a free port -----------------------------------
// Retention: messages older than an hour age out; sweep every 5s so the
// test can observe deletion -> compaction -> checkpoint-reset -> re-pull.
const retentionPath = join(root, "retention.json");
writeFileSync(
  retentionPath,
  JSON.stringify({
    defaults: [
      { table: "messages", column: "created_at", max_age_seconds: 3600 },
    ],
  }),
);
const server = spawn(bicdb, [
  "sync-serve", root,
  "--host", "127.0.0.1", "--port", "0",
  "--token", TOKEN, "--admin-token", ADMIN,
  "--retention", retentionPath,
  "--retention-interval-seconds", "5",
  "--event-horizon",
  "--server-write-only", "announcements",
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
console.log("sync server at", serverUrl);

const adminSql2 = async (scope, sql) => {
  const response = await fetch(`${serverUrl}/v1/${scope}/sql`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${ADMIN}` },
    body: JSON.stringify({ sql }),
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error);
  return payload.result;
};
const adminSql = async (sql) => {
  const response = await fetch(`${serverUrl}/v1/${SCOPE}/sql`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${ADMIN}`,
    },
    body: JSON.stringify({ sql }),
  });
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error);
  return payload.result;
};

const { server: statics, port } = await serve();
const browser = await chromium.launch();
try {
  // Working-set composition: the "Hub backend" writes what this user
  // should have cached.
  await adminSql("CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)");
  await adminSql("INSERT INTO notes (id, body) VALUES (1, 'announcement: welcome')");
  // Server-write-only fixture (A6.4): composed by the server, protected
  // from client pushes by --server-write-only.
  await adminSql("CREATE TABLE announcements (id INT PRIMARY KEY, body TEXT)");
  await adminSql("INSERT INTO announcements (id, body) VALUES (1, 'server only')");
  // Retention fixture: one stale message (2h old), one fresh.
  const nowSec = Math.floor(Date.now() / 1000);
  await adminSql("CREATE TABLE messages (id INT PRIMARY KEY, created_at INT)");
  await adminSql(`INSERT INTO messages VALUES (1, ${nowSec - 7200})`);
  await adminSql(`INSERT INTO messages VALUES (2, ${nowSec})`);

  // Wrong token must be rejected before any sync happens.
  const unauthorized = await fetch(`${serverUrl}/v1/${SCOPE}/push`, {
    method: "POST",
    headers: { authorization: "Bearer wrong" },
    body: "{}",
  });
  assert(unauthorized.status === 401, "wrong token rejected");

  const url = `http://127.0.0.1:${port}/test/sync.html`;
  const run = async (context, args) => {
    const page = await context.newPage();
    page.on("pageerror", (error) => console.error("[pageerror]", error));
    await page.goto(url);
    const result = await page.evaluate(
      (a) => window.__sync(a),
      { serverUrl, scope: SCOPE, token: TOKEN, ...args },
    );
    await page.close();
    return result;
  };

  const deviceA = await browser.newContext();
  const deviceB = await browser.newContext();

  const a1 = await run(deviceA, {
    phase: "bootstrap-and-write", noteId: 101, label: "from device a",
  });
  assert(
    a1.bootstrapped.includes("announcement: welcome"),
    `device A bootstrapped the working set, got ${JSON.stringify(a1.bootstrapped)}`,
  );
  console.log("device A bootstrap + write ok:", JSON.stringify(a1.after));

  const b1 = await run(deviceB, {
    phase: "bootstrap-and-write", noteId: 202, label: "from device b",
  });
  assert(
    b1.bootstrapped.includes("announcement: welcome") &&
      b1.bootstrapped.includes("from device a"),
    `device B saw server + device A rows, got ${JSON.stringify(b1.bootstrapped)}`,
  );
  console.log("device B bootstrap + write ok:", JSON.stringify(b1.after));

  const a2 = await run(deviceA, { phase: "converge" });
  const expected = ["announcement: welcome", "from device a", "from device b"];
  assert(
    JSON.stringify(a2.rows) === JSON.stringify(expected),
    `device A converged on all three rows, got ${JSON.stringify(a2.rows)}`,
  );
  console.log("device A converged:", JSON.stringify(a2.rows));

  // The server's scope database is the authoritative twin: it must hold
  // every row the devices produced.
  const serverView = await adminSql("SELECT COUNT(*) AS c FROM notes");
  assert(
    serverView.rows[0][0] === 3,
    `server sees 3 rows, got ${serverView.rows[0][0]}`,
  );

  // --- retention: the stale message ages out everywhere -----------------
  // Wait past a sweep (5s interval), then converge device A again. The
  // sweep deletes on the server, compacts (rewriting event offsets), and
  // resets stored pull checkpoints — this converge exercises that whole
  // path: the full re-pull must dedup cleanly AND deliver the deletion.
  await new Promise((r) => setTimeout(r, 8000));
  const a3 = await run(deviceA, { phase: "converge" });
  assert(
    JSON.stringify(a3.messages) === JSON.stringify([2]),
    `stale message aged out on device A, got ${JSON.stringify(a3.messages)}`,
  );
  assert(
    JSON.stringify(a3.rows) === JSON.stringify(expected),
    `notes intact through retention compaction, got ${JSON.stringify(a3.rows)}`,
  );
  console.log("retention ok: device A messages =", JSON.stringify(a3.messages));

  // --- client-side compaction must not strand subsequent writes ----------
  const a4 = await run(deviceA, {
    phase: "compact-write-sync", noteId: 303, label: "post-compact write",
  });
  assert(
    a4.report.pushedEvents > 0,
    `post-compact write pushed, got ${JSON.stringify(a4.report)}`,
  );
  const postCompact = await adminSql(
    "SELECT body FROM notes WHERE id = 303",
  );
  assert(
    postCompact.rows[0]?.[0] === "post-compact write",
    "server received the post-compact write",
  );
  console.log("client compact + push ok");

  // --- float canonicalization: integral floats must survive the JS hop ---
  // Regression for the payload-hash-mismatch bug: JS JSON round-trips turn
  // 84.0 into 84, so bundles must cross the boundary as opaque bytes.
  // Server -> device: 84.0 composed server-side; device -> server: -84.0
  // written in the browser and pushed back.
  await adminSql("CREATE TABLE geo (id INT PRIMARY KEY, lon FLOAT, lat FLOAT)");
  await adminSql("INSERT INTO geo VALUES (1, 84.0, 13.4)");
  const floats = await run(deviceA, {
    phase: "rls",
    statements: ["INSERT INTO geo VALUES (2, -84.0, 52.5)"],
    sql: "SELECT id, lon FROM geo ORDER BY id",
  });
  assert(
    floats.rows?.length === 2,
    `integral floats synced down and up, got ${JSON.stringify(floats)}`,
  );
  const geoOnServer = await adminSql("SELECT COUNT(*) AS c FROM geo");
  assert(
    geoOnServer.rows[0][0] === 2,
    `server accepted the -84.0 push, got ${geoOnServer.rows[0][0]}`,
  );
  console.log("float-canonicalization regression ok: 84.0 down, -84.0 up");

  // --- telemetry lands in the per-scope JSONL ----------------------------
  const telemetry = await run(deviceA, { phase: "telemetry" });
  assert(telemetry.recorded === 2, `2 telemetry events, got ${telemetry.recorded}`);
  const jsonl = readFileSync(join(root, SCOPE, "telemetry.jsonl"), "utf8")
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  assert(
    jsonl.some((line) => line.event?.type === "cold-open" && line.event?.ms === 123),
    "cold-open event recorded server-side",
  );
  assert(
    jsonl.some((line) => line.event?.type === "sync-report"),
    "sync report recorded server-side",
  );
  console.log("telemetry ok:", jsonl.length, "events in jsonl");

  // --- event-horizon trimming keeps bootstrap bundles bounded ------------
  // Churn one row 30x server-side; after the horizon sweep, a brand-new
  // device's bootstrap pull must carry only the winning event for it, not
  // the 30-deep history.
  for (let n = 0; n < 30; n++) {
    await adminSql(`UPDATE notes SET body = 'churn ${n}' WHERE id = 1`);
  }
  await new Promise((r) => setTimeout(r, 7000)); // past a sweep
  const freshPull = await fetch(`${serverUrl}/v1/${SCOPE}/pull`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${TOKEN}` },
    body: JSON.stringify({
      node_id: "99999999-9999-4999-8999-999999999999",
      checkpoint: { format_version: 1, local_export: { event_offset: 0 }, remote_imports: {} },
    }),
  }).then((r) => r.json());
  const bootstrap = JSON.parse(freshPull.bundles[0]);
  const note1Events = bootstrap.events.filter(
    (e) => e.event.payload.collection === "notes" && e.event.payload.record_id === "1",
  );
  assert(
    note1Events.length === 1,
    `bootstrap carries only the winner for the churned row, got ${note1Events.length}`,
  );
  assert(
    note1Events[0].event.payload.record.metadata.body === "churn 29",
    `winner is the latest version, got ${JSON.stringify(note1Events[0].event.payload.record.metadata)}`,
  );
  console.log(
    `event-horizon ok: 30x churn collapsed to 1 bootstrap event (bundle has ${bootstrap.event_count} events total)`,
  );

  // --- A6.4 direction policy: client pushes to protected collections fail --
  const forbidden = await run(await browser.newContext(), {
    phase: "push-forbidden",
    statements: ["INSERT INTO announcements (id, body) VALUES (99, 'forged announcement')"],
  });
  assert(
    forbidden.error?.includes("server-write-only"),
    `forbidden push rejected with a clear error, got ${JSON.stringify(forbidden)}`,
  );
  const announcements = await adminSql("SELECT COUNT(*) AS c FROM announcements");
  assert(
    announcements.rows[0][0] === 1,
    `server announcements untouched, got ${announcements.rows[0][0]}`,
  );
  console.log("direction policy ok: forged announcement rejected at push");

  // --- A6.5 dual cursors: one worker, two databases, two scopes -----------
  const dual = await run(await browser.newContext(), { phase: "dual" });
  assert(
    dual.notesFromHub >= 3,
    `db A bootstrapped the shared scope, got ${dual.notesFromHub}`,
  );
  const dualWorld = await adminSql2("dualworld", "SELECT v FROM things WHERE id = 1");
  assert(
    dualWorld.rows[0]?.[0] === "from dual-b",
    `db B pushed to its own scope, got ${JSON.stringify(dualWorld.rows)}`,
  );
  console.log("dual cursors ok: two databases synced to two scopes from one worker");

  console.log(
    "sync-e2e: OK (compose, bootstrap, convergence, retention, compact-safety, telemetry, horizon, direction, dual)",
  );
} finally {
  await browser.close();
  statics.close();
  server.kill();
  rmSync(root, { recursive: true, force: true });
}
