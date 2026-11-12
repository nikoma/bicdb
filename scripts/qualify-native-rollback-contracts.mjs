// Authenticated native lifecycle regression. Uses an isolated fixture issuer,
// immutable signed packages and closed-store copies; never a customer store.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createHash, generateKeyPairSync, randomBytes, randomUUID, sign} from 'node:crypto';
import {cp, mkdir, mkdtemp, readFile, rename, writeFile} from 'node:fs/promises';
import {createServer} from 'node:net';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';

assert.ok(process.env.CARRIER_BINARY && process.env.BICDB_BINARY, 'Set exact compiler and native artifact paths');
const compiler = resolve(process.env.CARRIER_BINARY), binary = resolve(process.env.BICDB_BINARY);
const root = await mkdtemp(join(tmpdir(), 'bicdb-rollback-contract-'));
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const environment = {...process.env, CARRIER_BICDB_SIGNING_KEY: randomBytes(32).toString('hex'), CARRIER_BICDB_SIGNING_KEY_ID: 'rollback-fixture'};
const cli = [], http = [], packages = [];
let server, database = join(root, 'database');
async function command(exe, args, name, cwd = root, expected = 0) {
  const chunks = [], child = spawn(exe, args, {cwd, env: environment});
  child.stdout.on('data', x => chunks.push(x)); child.stderr.on('data', x => chunks.push(x));
  const code = await new Promise((yes, no) => { child.once('exit', yes); child.once('error', no); });
  const output = Buffer.concat(chunks).toString();
  await writeFile(join(root, name + '.log'), output);
  cli.push({name, code});
  if (expected !== null) assert.equal(code, expected, output);
  return {code, output};
}
const source = (receipt, alias) => `service RollbackContract {}
auth jwt Auth { issuer: "https://rollback.example.test" audience: "rollback-fixture" algorithm: "RS256" roles_claim: "roles" }
role accountant { title: "Fixture accountant" }
model Entry { id: UUID @primary tenant_id: UUID amount: Decimal ${alias ? 'alias: String?' : ''} }
policy Entry { tenant_field: tenant_id read: roles [accountant] write: roles [accountant] }
crud Entry at "/entries" { create: protect Auth roles [accountant] list: protect Auth roles [accountant] get: protect Auth roles [accountant] }
${receipt ? `enum ReceiptState { retained }
model Receipt { id: UUID @primary tenant_id: UUID entry_id: UUID @belongs_to(Entry) amount: Decimal status: ReceiptState }
policy Receipt { tenant_field: tenant_id read: roles [accountant] write: roles [accountant] }
append_only Receipt { frozen_when: status == retained }
crud Receipt at "/receipts" { create: protect Auth roles [accountant] list: protect Auth roles [accountant] get: protect Auth roles [accountant] }
` : ''}`;
for (const [version, receipt, alias] of [['1.0.0', false, false], ['1.0.1', false, false], ['1.1.0', true, false], ['1.1.1', true, false], ['1.2.0', true, true]]) {
  const project = join(root, version);
  await mkdir(join(project, 'src'), {recursive: true});
  await writeFile(join(project, 'carrier.toml'), `[package]\nname="qualification/rollback-contract"\nversion="${version}"\n[module]\nnamespace="qualification.rollback_contract"\nversion="${version}"\n`);
  const authored = source(receipt, alias);
  await writeFile(join(project, 'src/main.carrier'), authored);
  await command(compiler, ['build', '--target', 'bicdb'], 'build-' + version, project);
  const artifact = join(project, '.carrier/generated-bicdb/application.bicdb.json');
  const bytes = await readFile(artifact), pkg = JSON.parse(bytes);
  packages.push({version, artifact, name: pkg.manifest.identity.name, sha256: sha(bytes), source_sha256: sha(authored)});
}
const key = join(root, '1.0.0/.carrier/generated-bicdb/signing-key.pub');
const common = () => ['app', '--trusted-key', 'rollback-fixture=' + key, database];
const app = packages[0].name;
const upgrade = async version => command(binary, [...common(), 'upgrade', packages.find(p => p.version === version).artifact], 'upgrade-' + version);
const rollback = async (name, allowed) => {
  const result = await command(binary, [...common(), 'rollback', app], name, root, null);
  if (allowed) assert.equal(result.code, 0, result.output);
  else {
    assert.notEqual(result.code, 0, 'Unsafe code downgrade accepted: ' + result.output);
    assert.match(result.output, /removes resource|removes field|persisted mutation|existing unique|immutable|durable audit/);
  }
  return result;
};
const {privateKey, publicKey} = generateKeyPairSync('rsa', {modulusLength: 2048});
await writeFile(join(root, 'jwks.json'), JSON.stringify({keys: [{...publicKey.export({format: 'jwk'}), kid: 'fixture', alg: 'RS256', use: 'sig'}]}));
await writeFile(join(root, 'auth.json'), JSON.stringify({schemes: [{kind: 'oidc_rs256', issuer: 'https://rollback.example.test', audience: 'rollback-fixture', jwks_file: 'jwks.json'}]}));
async function freePort() { const socket = createServer(); await new Promise(r => socket.listen(0, '127.0.0.1', r)); const port = socket.address().port; await new Promise(r => socket.close(r)); return port; }
const httpPort = await freePort(), pgPort = await freePort(), logs = [];
const tenant = randomUUID(), foreign = randomUUID();
const encode = value => Buffer.from(JSON.stringify(value)).toString('base64url');
function bearer(org, roles) {
  const now = Math.floor(Date.now() / 1000), unsigned = encode({alg: 'RS256', kid: 'fixture'}) + '.' + encode({iss: 'https://rollback.example.test', aud: 'rollback-fixture', sub: '9701', tenant_id: org, roles, iat: now, exp: now + 3600});
  return unsigned + '.' + sign('RSA-SHA256', Buffer.from(unsigned), privateKey).toString('base64url');
}
async function request(name, path, method = 'GET', body, status = 200, org = tenant, roles = ['accountant']) {
  const response = await fetch('http://127.0.0.1:' + httpPort + path, {method, headers: {Authorization: 'Bearer ' + bearer(org, roles), 'Content-Type': 'application/json', 'Idempotency-Key': randomUUID()}, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30000)});
  const value = await response.json();
  http.push({name, status: response.status, body: value});
  assert.equal(response.status, status, JSON.stringify(value));
  return value;
}
async function start() {
  server = spawn(binary, [...common(), 'serve', '--http-port', String(httpPort), '--pg-port', String(pgPort), '--pg-require-auth', '--auth-config', join(root, 'auth.json')]);
  server.stdout.on('data', x => logs.push(x)); server.stderr.on('data', x => logs.push(x));
  for (let i = 0; i < 600; i++) {
    assert.equal(server.exitCode, null, Buffer.concat(logs).toString());
    try { await request('ready', '/entries'); return; } catch (error) { if (!(error instanceof TypeError)) throw error; }
    await new Promise(r => setTimeout(r, 100));
  }
  throw Error('Native readiness timeout');
}
async function stop(signal = 'SIGTERM') {
  if (server && server.exitCode === null && server.signalCode === null) {
    const done = new Promise(r => server.once('exit', r)); server.kill(signal); await done;
  }
}
const metadata = {synthetic_verifier: true, real_issuer: false, hub: false, compiler: {path: compiler, sha256: sha(await readFile(compiler))}, native: {path: binary, sha256: sha(await readFile(binary))}, packages};
let passed = false;
try {
  await command(binary, [...common(), 'install', packages[0].artifact], 'install');
  await start();
  const entry = await request('original-fact', '/entries', 'POST', {id: randomUUID(), amount: '9007199254740992.03'}, 201);
  assert.equal(entry.amount, '9007199254740992.03');
  await request('foreign-original-denied', '/entries/' + entry.id, 'GET', undefined, 404, foreign);
  await stop();
  await upgrade('1.0.1');
  await rollback('code-only-rollback-allowed', true);
  await start(); assert.deepEqual(await request('code-only-preserves-fact', '/entries/' + entry.id), entry); await stop();
  await cp(database, join(root, 'pre-traffic-backup'), {recursive: true});
  await upgrade('1.1.0');
  await rollback('omitted-resource-refused-before-use', false);
  await start();
  const receipt = await request('new-immutable-fact', '/receipts', 'POST', {id: randomUUID(), entry_id: entry.id, amount: '25.01', status: 'retained'}, 201);
  await request('unprivileged-new-fact-denied', '/receipts/' + receipt.id, 'GET', undefined, 403, tenant, ['viewer']);
  await request('foreign-new-fact-denied', '/receipts/' + receipt.id, 'GET', undefined, 404, foreign);
  await stop();
  await rollback('omitted-resource-refused-after-use', false);
  await start(); assert.deepEqual(await request('failed-rollback-preserves-new-fact', '/receipts/' + receipt.id), receipt); await stop('SIGKILL');
  await start(); assert.deepEqual(await request('crash-preserves-new-fact', '/receipts/' + receipt.id), receipt); await stop();
  await cp(database, join(root, 'cold-restored'), {recursive: true});
  await rename(database, join(root, 'original-offline'));
  database = join(root, 'cold-restored');
  await start(); assert.deepEqual(await request('restore-preserves-new-fact', '/receipts/' + receipt.id), receipt); await stop();
  await rollback('restored-omitted-resource-still-refused', false);
  await upgrade('1.1.1');
  await rollback('retained-contract-code-rollback-allowed', true);
  await start(); assert.deepEqual(await request('retained-contract-preserves-new-fact', '/receipts/' + receipt.id), receipt); await stop();
  await upgrade('1.2.0');
  await rollback('omitted-field-refused', false);
  const preserved = database; database = join(root, 'pre-traffic-backup');
  await start(); assert.deepEqual(await request('fenced-backup-retains-original', '/entries/' + entry.id), entry);
  await request('fenced-backup-has-old-contract', '/receipts', 'GET', undefined, 404); await stop(); database = preserved;
  passed = true;
} finally {
  await stop();
  await writeFile(join(root, 'server.log'), Buffer.concat(logs));
  await writeFile(join(root, 'results.json'), JSON.stringify({...metadata, passed, cli, http}, null, 2) + '\n');
  console.log(JSON.stringify({root, passed, http_records: http.length}));
}
