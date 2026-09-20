// Headless-Chromium smoke test: BicDB (wasm) over real OPFS.
//
// Phase "write" seeds 50 rows, then the page is fully reloaded — wiping
// wasm memory — and phase "verify" must read the data back through OPFS
// recovery, then compact and read again. Anything that survives the reload
// went through the sync-access-handle pool.
import { chromium } from "playwright";
import { serve } from "./serve.mjs";

function assert(cond, message) {
  if (!cond) {
    console.error(`FAIL: ${message}`);
    process.exit(1);
  }
}

const { server, port } = await serve();
const browser = await chromium.launch();
try {
  const context = await browser.newContext();
  const page = await context.newPage();
  page.on("console", (msg) => {
    if (msg.type() === "error") console.error("[page]", msg.text());
  });
  page.on("pageerror", (error) => console.error("[pageerror]", error));

  const url = `http://127.0.0.1:${port}/test/smoke.html`;
  await page.goto(url);
  const wrote = await page.evaluate(() => window.__run("write"));
  assert(wrote.count === 20, `stars>=3 count 20, got ${wrote.count}`);
  assert(wrote.records >= 50, `>=50 records, got ${wrote.records}`);
  assert(wrote.poolUsed > 0, "pool slots in use");
  console.log("write phase ok:", JSON.stringify(wrote));

  await page.reload(); // drops worker + wasm memory; OPFS is all that's left
  const t0 = Date.now();
  const verified = await page.evaluate(() => window.__run("verify"));
  console.log(
    `verify wall time (worker boot + wasm fetch/instantiate + OPFS recovery +`,
    `4 queries + compact): ${Date.now() - t0}ms`,
  );
  assert(verified.body === "note 42", `row 42 recovered, got ${verified.body}`);
  assert(verified.count === 50, `50 rows after reload, got ${verified.count}`);
  assert(
    verified.countAfterCompact === 50,
    `50 rows after compaction, got ${verified.countAfterCompact}`,
  );
  console.log("verify phase ok:", JSON.stringify(verified));

  // Encryption at rest: write with one key, reload, wrong key must fail,
  // right key must read.
  const encWrote = await page.evaluate(() => window.__runEncrypted("write"));
  assert(encWrote.ok === true, "encrypted write");
  await page.reload();
  const wrongKey = await page.evaluate(() =>
    window.__runEncrypted("wrong-key"),
  );
  assert(wrongKey.opened === false, "wrong key must fail to open");
  const encRead = await page.evaluate(() => window.__runEncrypted("read"));
  assert(
    encRead.note === "sealed at rest",
    `encrypted row recovered, got ${encRead.note}`,
  );
  console.log("encryption ok:", JSON.stringify({ wrongKey: wrongKey.error }));

  // Single-owner Web Lock: a second client for the same database waits for
  // the first to close, then reads consistent data.
  const contention = await page.evaluate(() => window.__runContention());
  assert(
    contention.waitedWhileFirstOpen === true,
    "second open waited on the lock",
  );
  assert(
    contention.countFromSecond === 50,
    `second client saw 50 rows, got ${contention.countFromSecond}`,
  );
  console.log("contention ok:", JSON.stringify(contention));

  // Cache hygiene: pressure-triggered compaction reclaims churn garbage;
  // the blown budget is reported; destroy() wipes the cache.
  const hygiene = await page.evaluate(() => window.__runHygiene());
  assert(hygiene.compacted === true, "pressure check ran a compaction");
  assert(
    hygiene.after < hygiene.before,
    `compaction reclaimed bytes (${hygiene.before} -> ${hygiene.after})`,
  );
  assert(hygiene.state === "cache-full", "blown budget reported as cache-full");
  assert(hygiene.states.includes("cache-full"), "onState saw cache-full");
  assert(hygiene.freshAfterDestroy === true, "destroy() wiped the cache");
  console.log(
    `hygiene ok: ${hygiene.before} -> ${hygiene.after} bytes, states=${hygiene.states}`,
  );

  // Encrypted chunked attachments.
  const attachments = await page.evaluate(() => window.__runAttachments());
  assert(attachments.chunks === 3, `2.5MiB = 3 chunks, got ${attachments.chunks}`);
  assert(attachments.roundtrip === true, "attachment roundtrip intact");
  assert(attachments.plaintextOnDisk === false, "no plaintext on disk");
  assert(attachments.wrongKeyFailed === true, "wrong key cannot decrypt");
  assert(attachments.usage > 2.5 * 1024 * 1024, "usageBytes sees the blob");
  assert(attachments.hasAfterDelete === false, "blob removed on delete");
  assert(
    attachments.manifestCountAfterDelete === 0,
    "manifest row removed on delete",
  );
  console.log("attachments ok:", JSON.stringify({ chunks: attachments.chunks }));

  // A6 substrate: multi-database worker, persistent sessions, generations.
  const substrate = await page.evaluate(() => window.__runSubstrate());
  assert(substrate.crossLeak === false, "databases are isolated");
  assert(substrate.guc === "alice", "session GUC persisted across calls");
  assert(
    substrate.rowsAfterRollback === 1,
    `rollback across calls left 1 row, got ${substrate.rowsAfterRollback}`,
  );
  assert(substrate.rowsGenBumped === true, "generation bumped on write");
  assert(
    substrate.controlHadOwnGenerations === true,
    "each db reports its own collections",
  );
  assert(substrate.controlFresh === true, "destroyed db came back empty");
  assert(
    substrate.dataSurvived === 2,
    `sibling db survived the destroy, got ${substrate.dataSurvived}`,
  );
  console.log("substrate ok:", JSON.stringify(substrate));

  console.log(
    "browser-smoke: OK (write, reload, OPFS recovery, compact, encryption, lock, hygiene, attachments, substrate)",
  );
} finally {
  await browser.close();
  server.close();
}
