// Native-process qualification driven by Node; no Rust tests or generated-output patches.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes,createHash} from 'node:crypto';
import {mkdtemp,mkdir,readFile,writeFile,copyFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const binary=resolve(process.env.BICDB_BINARY??'target/release/bicdb');
const compiler=process.env.CARRIER_BINARY??'carrier';
const fixtures=fileURLToPath(new URL('./fixtures/optimistic-upgrade/',import.meta.url));
const root=await mkdtemp(join(tmpdir(),'bicdb-optimistic-upgrade-'));
const results=[];
const environment={...process.env,CARRIER_BICDB_SIGNING_KEY:randomBytes(32).toString('hex'),CARRIER_BICDB_SIGNING_KEY_ID:'optimistic-upgrade-qualification'};
async function run(executable,args,name,cwd=root,expected=0) {
  const chunks=[];
  const child=spawn(executable,args,{cwd,env:environment,stdio:['ignore','pipe','pipe']});
  child.stdout.on('data',chunk=>chunks.push(chunk));
  child.stderr.on('data',chunk=>chunks.push(chunk));
  const timer=setTimeout(()=>child.kill('SIGTERM'),120_000);
  const status=await new Promise((yes,no)=>{child.on('error',no);child.on('exit',yes);});
  clearTimeout(timer);
  const output=Buffer.concat(chunks).toString();
  await writeFile(join(root,`${name}.log`),output);
  results.push({name,status,expected});
  await writeFile(join(root,'results.json'),JSON.stringify({binary,results},null,2)+'\n');
  assert.equal(status,expected,`${name}: ${output}`);
  console.log(`${name}: expected exit ${status}`);
  return output;
}
const packages=new Map();
for(const version of ['v1','v2','remove-counter','rename-counter','reinterpret-counter']) {
  const project=join(root,version);
  await mkdir(join(project,'src'),{recursive:true});
  const base=version==='v1'?'v1':'v2';
  let config=await readFile(join(fixtures,base,'carrier.toml'),'utf8');
  let source=await readFile(join(fixtures,base,'src/main.carrier'),'utf8');
  if(!['v1','v2'].includes(version))config=config.replaceAll('1.0.1','1.0.2');
  if(version==='remove-counter')source=source.replace('version: Int @version = 1','version: Int = 1');
  if(version==='rename-counter')source=source.replace('version: Int @version = 1','version: Int = 1\n  revision: Int @version = 1');
  if(version==='reinterpret-counter') {
    config=config.replaceAll('1.0.2','1.0.0');
    source=source.replace('version: Int @version = 1','version: Int = 1');
  }
  await writeFile(join(project,'carrier.toml'),config);
  await writeFile(join(project,'src/main.carrier'),source);
  await run(compiler,['build','--target','bicdb'],`build-${version}`,project);
  packages.set(version,join(project,'.carrier/generated-bicdb/application.bicdb.json'));
}
const key=join(root,'v1/.carrier/generated-bicdb/signing-key.pub');
const app=(database,args,name,expected=0)=>run(binary,['app','--trusted-key',`optimistic-upgrade-qualification=${key}`,join(root,database),...args],name,root,expected);
const scenario=(database,name,label)=>app(database,['test','qualification.version-upgrade',name,'--case-index','0'],label);
await app('fresh',['install',packages.get('v2')],'fresh-install');
await scenario('fresh','test_0_draft_version_rejects_a_stale_edit','fresh-stale-edit');
await app('upgrade',['install',packages.get('v1')],'old-install');
await scenario('upgrade','test_0_seed_an_ordinary_draft_before_upgrade','seed-existing-row');
await app('upgrade',['upgrade',packages.get('v2')],'add-counter-upgrade');
await scenario('upgrade','test_1_upgrade_backfills_an_existing_draft_and_keeps_its_data','verify-backfill-and-edit');
await scenario('upgrade','test_2_restart_and_refused_upgrade_retain_the_current_counter','restart-persists-counter');
for(const version of ['remove-counter','rename-counter']) {
  const output=await app('upgrade',['upgrade',packages.get(version)],version,1);
  assert.match(output,/persisted mutation or collection policy/);
  await scenario('upgrade','test_2_restart_and_refused_upgrade_retain_the_current_counter',`${version}-preserves-active`);
}
await app('existing-field',['install',packages.get('reinterpret-counter')],'writable-counter-install');
const refusal=await app('existing-field',['upgrade',packages.get('v2')],'reinterpret-writable-counter',1);
assert.match(refusal,/persisted mutation or collection policy/);
// Reopening the store after each command independently exercises catalog and
// migration persistence. Rollback runs offline, before any customer traffic.
await app('upgrade',['rollback','qualification.version-upgrade'],'operator-rollback');
await scenario('upgrade','test_1_rollback_preserves_the_edited_draft','rollback-preserves-data');
const digest=createHash('sha256').update(await readFile(binary)).digest('hex');
await writeFile(join(root,'qualification.json'),JSON.stringify({passed:true,binary_sha256:digest,results},null,2)+'\n');
console.log(`Evidence: ${root}`);
