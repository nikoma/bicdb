// Source-authored public SDK packages; no generated application bytes or Rust
// test helpers. Run against an explicitly hashed native product binary.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {createHash, createPrivateKey, createPublicKey, randomBytes, randomUUID} from 'node:crypto';
import {makeHttpHarness} from '../native-invariant-rollback/http-harness.mjs';
import {transportModule} from './transport.mjs';

const config = JSON.parse(await fs.readFile(process.argv[2]));
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
assert.equal(sha(await fs.readFile(config.binary)), config.binarySha256);
const out = path.join(config.outputRoot, 'activation-indexes-' + Date.now());
await fs.mkdir(out, {recursive:true, mode:0o700});
await fs.copyFile(import.meta.filename, out + '/driver.mjs');
await fs.copyFile(process.argv[2], out + '/config.json');
const seed = randomBytes(32), key = createPrivateKey({key:Buffer.concat([
  Buffer.from('302e020100300506032b657004220420', 'hex'), seed,
]), format:'der', type:'pkcs8'});
await fs.writeFile(out + '/seed.hex', seed.toString('hex'), {mode:0o600});
await fs.writeFile(out + '/key.pub', createPublicKey(key).export({format:'der',type:'spki'}).subarray(-32).toString('hex'));
const keyId = 'qualification-activation-indexes', database = out + '/database';
const commands = [];
async function cli(args, success = true) {
  const child = spawn(config.binary, args), chunks = [];
  child.stdout.on('data', b => chunks.push(b)); child.stderr.on('data', b => chunks.push(b));
  const code = await new Promise((resolve,reject) => {child.once('exit',resolve); child.once('error',reject);});
  const output = Buffer.concat(chunks).toString();
  commands.push({args, code, output});
  await fs.writeFile(out + '/commands.json', JSON.stringify(commands,null,2));
  if (success) assert.equal(code, 0, output); else assert.notEqual(code, 0, 'Expected real lifecycle refusal');
  return output;
}
const lifecycle = (...args) => ['app','--trusted-key',keyId + '=' + out + '/key.pub',database,...args];
function resource(name, relation) {
  const fields = [['id','uuid'],['organization_id','uuid'],['amount','decimal']].map(([name,field_type]) => ({name,field_type,nullable:false,generated:false,default_json:null}));
  const value = {version:1,name,relation,schema_version:1,primary_key:'id',fields,
    create_fields:['amount'],update_fields:[],required_create_fields:['amount'],server_managed_fields:['organization_id'],
    validation:[],operations:['list','get','create'],filters:[],relation_filters:[],json_path_filters:[],search_fields:[],sort_fields:['id'],
    list_route:'/activation-indexes/' + relation,item_route:'/activation-indexes/' + relation + '/{id}',
    soft_delete_field:null,version_field:null,tenant_field:'organization_id',workspace_field:null,
    policy:{version:1,read:{roles:['probe_operator'],role_match:'any'},write:{roles:['probe_operator'],role_match:'any'}},
    required_roles:[],required_scopes:[],policy_attributes:{},read_roles:{},redacted_fields:[],immutable_fields:['organization_id'],
    idempotency:null,cache:null,audit:{required:true,include_fields:['amount','id','organization_id'],redact_fields:[]},events:[],operation_metadata:[],contract_sha256:'',openapi:{}};
  value.contract_sha256 = sha(JSON.stringify(value)); return value;
}
const original = Array.from({length:4},(_,i) => resource('ActivationRow' + i, 'activation_row_' + i));
async function build(version, resources) {
  const name = 'qualification.activation_indexes';
  const lock = Buffer.from('{"format":1,"packages":[]}'), sbom = Buffer.from('{"bomFormat":"CycloneDX","specVersion":"1.5"}');
  const provenance = Buffer.from(JSON.stringify({builder:'source-authored-node-public-ABI-fixture',source_authored_ABI:true,generated_product_edits:false,purpose:'activation constraint index and compensation regression'}));
  const relations = resources.map(r => r.relation).sort();
  const routes = resources.flatMap(r => ['list','get','create'].map(operation => ({
    name:r.relation+'_'+operation,method:operation==='create'?'POST':'GET',template:operation==='get'?r.item_route:r.list_route,
    export:'carrier_resource_api',resource:r.name,operation,public:false,auth_scheme:'IndexAuth',roles:['probe_operator'],scopes:[],roles_any:true,scopes_any:true,
    max_request_bytes:16384,max_response_bytes:1048576,streaming_request:false,streaming_response:false,sse:false,websocket:false,
  })));
  const migration = (version, rows) => ({version,schema_version:version,contract_versions:Object.fromEntries(rows.map(r=>[r.name,1]).sort()),
    forward:rows.map(r=>({kind:'validate_contract',resource:r.name})),compatibility_checks:[],transformations:[],activation_boundary:'before routes',rollback:[],irreversible:false,dependency_requirements:{}});
  const application = {abi_version:2,application_profile:'bicdb-application-v2',package:{application:name,version,package_sha256:'0'.repeat(64),dependency_lock_sha256:sha(lock),sbom_sha256:sha(sbom),provenance_sha256:sha(provenance),signature_key_id:keyId,signature_algorithm:'ed25519',signature:'unsigned-placeholder'},
    relation_permissions:relations.map(relation=>({relation,actions:['select','insert'],readable_columns:['amount','id','organization_id'],writable_columns:['amount','id','organization_id']})),
    raw_sql:[],service_imports:[],service_exports:[],secrets:[],egress:[],blobs:[],routes,auth_schemes:{IndexAuth:{kind:'oidc_rs256',issuer:'https://issuer.invalid',audience:'native-rollback-probe'}},resources,invariants:[],
    migrations:[migration(1,original),...(version==='1.0.0'?[]:[migration(2,resources)])],workers:[],schedules:[],
    required_features:['trusted_connection_identity','exact_column_authority','oidc_rs256','crud','pagination','http','observability'],max_call_depth:16};
  const manifest = {identity:{name,version,abi_version:2,description:'Native activation index regression'},dependencies:[],capabilities:['http_routes','database','transactions','observability'],
    permissions:{read_relations:relations,write_relations:relations,publish_queues:[],consume_queues:[],network_hosts:[]},limits:{memory_bytes:67108864,fuel:10000000,timeout_ms:10000,max_input_bytes:1048576,max_output_bytes:4194304,max_concurrency:4},functions:[],indexes:[],storage:[],routes:[],subscriptions:[],observability:[],application};
  const unsigned = out + '/source-' + version + '.json', signed = out + '/signed-' + version + '.json';
  await fs.writeFile(unsigned, JSON.stringify({manifest,modules:{[name]:transportModule(manifest)},frontend_assets:{},components:[],scripts:{},dependency_lock:[...lock],sbom:[...sbom],provenance:[...provenance]}));
  await cli(['app-package','sign',unsigned,'--key-id',keyId,'--signing-key',out+'/seed.hex','--out',signed]);
  return signed;
}
async function indexes() {
  try {return JSON.parse(await fs.readFile(database + '/indexes.json')).indexes;}
  catch (error) {if (error.code === 'ENOENT') return []; throw error;}
}
const initial = await build('1.0.0', original);
await cli(lifecycle('install',initial));
const installed = await indexes();
for (const r of original) {
  const actual = installed.find(i=>i.name===r.relation+'_pkey');
  assert.equal(Boolean(actual), config.expectActivationIndexes !== false, r.relation+' primary key before any serve');
  if (actual) {assert.equal(actual.unique,true); assert.deepEqual(actual.fields,[{MetadataPath:['id']}]);}
}
if (config.expectActivationIndexes === false) {
  await fs.writeFile(out+'/qualification.json',JSON.stringify({passed:true,scope:'Reproduced missing activation SQL indexes on old binary; no fix qualification',source:config.source,binary_sha256:config.binarySha256}));
  console.log(out); process.exit(0);
}
const h = await makeHttpHarness({database,binary:config.binary,trustedKeys:[{id:keyId,path:out+'/key.pub'}],evidence:out,readiness:original[0].list_route});
const tenant = randomUUID(), other = randomUUID(), records = [];
async function call(route, method, body, org=tenant, roles=['probe_operator']) {
  const result = await h.call(route,method,body,org,{},roles); records.push(result); return result;
}
try {
  const started = Date.now(); await h.start(tenant); assert(Date.now()-started<30000, 'Fresh startup must remain within original30s');
  assert.deepEqual(await indexes(), installed, 'First serve creates no deferred indexes');
  const created = await call(original[0].list_route,'POST',{amount:'9007199254740992.01'}); assert.equal(created.status,201);
  const snapshot = await call(original[0].list_route,'GET'); assert.equal(snapshot.status,200);
  assert.equal((await call(original[0].list_route,'GET',undefined,other)).body.items.length,0);
  assert.equal((await call(original[0].list_route,'GET',undefined,tenant,['viewer'])).status,403);
  await h.stop();
  const added = resource('ActivationNew','activation_new'), collision = resource('ActivationCollision','activation_collision');
  collision.indexes = [{name:'activation_new_pkey',fields:['amount']}];
  collision.contract_sha256 = ''; collision.contract_sha256 = sha(JSON.stringify(collision));
  const bad = await build('2.0.0',[added,...original,collision]);
  const refused = await cli(lifecycle('upgrade',bad),false);
  assert.match(refused,/schema index `activation_new_pkey` conflicts/);
  assert.deepEqual(await indexes(), installed, 'Failed populated upgrade compensates new SQL and resource indexes');
  const collections = await fs.readFile(database+'/collections.json','utf8');
  assert(!collections.includes('activation_new')&&!collections.includes('activation_collision'), 'Failed upgrade compensates new collections and schemas');
  await h.start(tenant); assert.deepEqual(await call(original[0].list_route,'GET'),snapshot);
  await h.restart(tenant); assert.deepEqual(await call(original[0].list_route,'GET'),snapshot);
  await h.coldRestore(tenant); assert.deepEqual(await call(original[0].list_route,'GET'),snapshot);
  await fs.writeFile(out+'/records.json',JSON.stringify(records,null,2));
  await fs.writeFile(out+'/qualification.json',JSON.stringify({passed:true,source:config.source,binary_sha256:config.binarySha256,source_authored_ABI:true,Node_only:true,
    indexes_before_serve:true,no_startup_index_writes:true,failed_populated_upgrade_compensated:true,tenant_and_role_denials:true,SIGKILL_and_cold_restore:true,
    exact_Decimal_preserved:true,ERP_qualified:false,real_issuer:false,Hub:false},null,2));
  console.log(out);
} finally {await h.stop();}
