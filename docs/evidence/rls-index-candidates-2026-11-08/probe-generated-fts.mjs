#!/usr/bin/env node
// A planner qualification probe, never an application SQL rewrite.
// Requires a fresh isolated BicDB database and an installed pg dependency.
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const ownerUrl = process.env.CARRIER_RLS_PROBE_OWNER_URL;
assert.ok(ownerUrl, "Set CARRIER_RLS_PROBE_OWNER_URL to a fresh isolated database");
const pgPath = process.env.CARRIER_RLS_PROBE_PG_PATH;
assert.ok(pgPath, "Set CARRIER_RLS_PROBE_PG_PATH to pg/lib/index.js");
const { default: pg } = await import(pathToFileURL(resolve(pgPath)));
const owner = new pg.Client({ connectionString: ownerUrl });
const password = randomBytes(24).toString("hex");
const appUrl = new URL(ownerUrl);
appUrl.username = "zf_rls_probe_app";
appUrl.password = password;
const app = new pg.Client({ connectionString: appUrl.toString() });
const report = {
  measurement_kind: "rls_index_candidate_qualification",
  engine_source_commit: process.env.CARRIER_RLS_PROBE_ENGINE_COMMIT ?? null,
  engine_sha256: process.env.CARRIER_RLS_PROBE_ENGINE_SHA256 ?? null,
  policy: "org_id::text = NULLIF((SELECT zf_probe_tenant()), '')",
  rls_enabled: true, rls_forced: false,
  index: "UNIQUE(org_id, number)",
  policy_tenant_function_volatility: "STABLE",
  full_text_index: "generated weighted number(A) + id(B), english",
  samples: [], correctness: {}, environment_values_recorded: false,
};
const implicit = "SELECT count(*)::int AS total FROM zf_probe_orders WHERE number = $1";
const explicit = "SELECT count(*)::int AS total FROM zf_probe_orders WHERE number = $1 AND org_id = $2";
async function count(client, sql, values) {
  const start = performance.now();
  const result = await client.query(sql, values);
  return { total: result.rows[0].total, ms: performance.now() - start };
}
async function seed(tenant, start, end) {
  for (let from = start; from < end; from += 250) {
    const values = [];
    const tuples = [];
    for (let n = from; n < Math.min(end, from + 250); n += 1) {
      const offset = values.length;
      values.push(`${tenant}-${n}`, tenant, `N${n}`);
      tuples.push(`($${offset + 1},$${offset + 2},$${offset + 3})`);
    }
    await owner.query(`INSERT INTO zf_probe_orders(id,org_id,number) VALUES ${tuples.join(",")}`, values);
  }
}
function stats(values) {
  const sorted = [...values].sort((a,b)=>a-b);
  return { count: sorted.length, median_ms: sorted[Math.floor(sorted.length / 2)], max_ms: sorted.at(-1) };
}
try {
  await owner.connect();
  await owner.query(`CREATE ROLE zf_rls_probe_app LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '${password}'`);
  await owner.query("CREATE TABLE zf_probe_orders(id TEXT PRIMARY KEY, org_id TEXT NOT NULL, number TEXT NOT NULL, UNIQUE(org_id,number))");
  await owner.query("CREATE INDEX zf_probe_orders_search_fts ON zf_probe_orders USING gin ((setweight(to_tsvector('english',COALESCE(number,'')),'A') || setweight(to_tsvector('english',COALESCE(id,'')),'B')))");
  await owner.query("CREATE FUNCTION zf_probe_tenant() RETURNS TEXT LANGUAGE SQL STABLE AS $$ SELECT current_setting('app.tenant',true) $$");
  await owner.query("ALTER TABLE zf_probe_orders ENABLE ROW LEVEL SECURITY");
  await owner.query("CREATE POLICY zf_probe_select ON zf_probe_orders FOR SELECT USING (org_id::text = NULLIF((SELECT zf_probe_tenant()), ''))");
  await owner.query("GRANT SELECT ON zf_probe_orders TO zf_rls_probe_app");
  await owner.query("GRANT EXECUTE ON FUNCTION zf_probe_tenant() TO zf_rls_probe_app");
  await app.connect();
  await seed("own",0,100);
  await app.query("SELECT set_config('app.tenant', 'own', false)");
  let previous = 0;
  for (const unrelated of [0,1000,5000]) {
    await seed("other", previous, unrelated);
    previous = unrelated;
    const times = { implicit: [], explicit: [] };
    for (let n = 0; n < 8; n += 1) {
      // Alternate order to reduce warmup/order bias; discard the first pair.
      for (const kind of (n % 2 === 0 ? ["implicit","explicit"] : ["explicit","implicit"])) {
        const result = await count(app, kind === "implicit" ? implicit : explicit, kind === "implicit" ? ["N42"] : ["N42","own"]);
        assert.equal(result.total, 1, `${kind} own-tenant result`);
        if (n > 0) times[kind].push(result.ms);
      }
    }
    const plans = {};
    for (const [kind,sql,values] of [["implicit",implicit,["N42"]],["explicit",explicit,["N42","own"]]]) {
      plans[kind] = (await app.query(`EXPLAIN ANALYZE ${sql}`,values)).rows;
    }
    const entry = { own_rows:100, unrelated_rows:unrelated, implicit:stats(times.implicit), explicit:stats(times.explicit), plans };
    report.samples.push(entry);
    console.log(JSON.stringify({ unrelated_rows:unrelated, implicit:entry.implicit, explicit:entry.explicit }));
  }
  // Same statement and connection, changing context: a plan must not retain tenant values.
  for (const [tenant,expected] of [["other",1],["",0],["own",1]]) {
    await app.query("SELECT set_config('app.tenant',$1,false)",[tenant]);
    const a = await count(app,implicit,["N42"]);
    assert.equal(a.total,expected);
    report.correctness[`context_${tenant || "empty"}`] = a.total;
  }
  // Owner/superuser must retain its bypass semantics; no injected tenant restriction.
  assert.equal((await count(owner,implicit,["N42"])).total,2);
  report.correctness.owner_bypass = 2;
  // Additional permissive policies are OR, so one policy's tenant condition is not necessary.
  await owner.query("CREATE POLICY zf_probe_shared ON zf_probe_orders FOR SELECT USING (number = 'N42')");
  await app.query("SELECT set_config('app.tenant','',false)");
  assert.equal((await count(app,implicit,["N42"])).total,2);
  report.correctness.permissive_or = 2;
  // Restrictive policies remain AND with the permissive union.
  await owner.query("CREATE POLICY zf_probe_restrictive ON zf_probe_orders AS RESTRICTIVE FOR SELECT USING (org_id = 'own')");
  assert.equal((await count(app,implicit,["N42"])).total,1);
  report.correctness.restrictive_and = 1;
  report.measurement_completed = true;
  report.baseline_semantics_passed = true;
  const first = report.samples[0];
  const last = report.samples.at(-1);
  report.policy_only_growth_ratio = last.implicit.median_ms / first.implicit.median_ms;
  report.explicit_control_growth_ratio = last.explicit.median_ms / first.explicit.median_ms;
  report.data_growth_goal_met = last.implicit.median_ms <= Math.max(100, first.implicit.median_ms * 4);
  report.outcome = report.data_growth_goal_met ? "no_growth_budget_failure_observed" : "confirmed_policy_candidate_growth_defect";
  // passed describes completed measurement and semantic controls, not the performance goal.
  report.passed = true;
} catch (error) {
  report.passed = false;
  report.error = String(error.message);
  process.exitCode = 1;
} finally {
  await app.end().catch(()=>{});
  await owner.end().catch(()=>{});
  await writeFile(process.env.CARRIER_RLS_PROBE_REPORT ?? "rls-index-candidates.json",JSON.stringify(report,null,2)+"\n");
}
