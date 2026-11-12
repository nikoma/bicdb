import fs from 'node:fs/promises';
import assert from 'node:assert/strict';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {createHash,randomUUID} from 'node:crypto';
import {makeHttpHarness} from './http-harness.mjs';
const config=JSON.parse(await fs.readFile(process.argv[2],'utf8'));
assert.equal(typeof config.expectProtected,'boolean');
const sha=b=>createHash('sha256').update(b).digest('hex');
assert.equal(sha(await fs.readFile(config.binary)),config.binarySha256);
const bundle=path.dirname(import.meta.filename),packages=JSON.parse(await fs.readFile(bundle+'/package-lock.json'));
const key=bundle+'/packages/signing-key.pub';assert.equal(sha(await fs.readFile(key)),packages.key_sha256);
const objects=new Map();for(const p of packages.packages){const b=await fs.readFile(bundle+'/'+p.file);assert.equal(sha(b),p.sha256);objects.set(p.name,JSON.parse(b));}
const stripped=name=>objects.get(name).manifest.application.resources.map(({schema_version,...r})=>r);
for(const name of ['strong','compatible','changed'])assert.deepEqual(stripped(name),stripped('weak'),'Resource/schema contracts must match except release-derived version');
const application=packages.packages[0].application,run=config.outputRoot+'/rollback-invariants-'+Date.now();await fs.mkdir(run+'/evidence',{recursive:true});
await fs.copyFile(import.meta.filename,run+'/evidence/driver.mjs');await fs.copyFile(process.argv[2],run+'/evidence/config.json');
const records=[],results=[];
async function command(database,args,label,success=true){const b=[],p=spawn(config.binary,['app','--trusted-key',packages.key_id+'='+key,database,...args]);p.stdout.on('data',x=>b.push(x));p.stderr.on('data',x=>b.push(x));const code=await new Promise((r,j)=>{p.once('exit',r);p.once('error',j)}),output=Buffer.concat(b).toString();await fs.writeFile(run+'/evidence/'+label+'.log',output);if(success)assert.equal(code,0,output);return{code,output};}
const file=name=>bundle+'/'+packages.packages.find(x=>x.name===name).file;
for(const test of [{name:'removed',from:'weak',to:'strong',bad:-1,blocked:true},{name:'changed',from:'strong',to:'changed',bad:0,blocked:true},{name:'compatible',from:'strong',to:'compatible',bad:-1,blocked:false}]){
 const database=run+'/'+test.name,tenant=randomUUID();await command(database,['install',file(test.from)],test.name+'-install');
 let h=await makeHttpHarness({database,binary:config.binary,trustedKeys:[{id:packages.key_id,path:key}],evidence:run+'/evidence/'+test.name,readiness:'/health',roles:['probe_operator']});
 async function record(label,r,status){records.push({label,...r});assert.equal(r.status,status,JSON.stringify(r));console.log(label+': '+r.status);return r.body;}
 try{
  await h.start(tenant);const positive=await record(test.name+'-acknowledged-positive',await h.call('/entries','POST',{id:randomUUID(),amount:2},tenant),201);assert.equal(positive.organization_id,tenant);await h.stop();
  await command(database,['upgrade',file(test.to)],test.name+'-upgrade');await h.start(tenant);
  await record(test.name+'-active-guard-refuses-bad-row',await h.call('/entries','POST',{id:randomUUID(),amount:test.bad},tenant),409);
  const initial=await record(test.name+'-before-rollback',await h.call('/entries','GET',undefined,tenant),200);assert.equal(initial.items.length,1);await h.stop();
  const catalogBefore=await fs.readFile(database+'/applications/snapshots/active.json');
  const rollback=await command(database,['rollback',application],test.name+'-rollback',false),shouldBlock=config.expectProtected&&test.blocked;
  if(shouldBlock){assert.notEqual(rollback.code,0,'Unsafe invariant rollback must refuse');assert.match(rollback.output,/invariant|guard|contract/i);}else assert.equal(rollback.code,0,rollback.output);
  if(shouldBlock)assert.deepEqual(await fs.readFile(database+'/applications/snapshots/active.json'),catalogBefore,'Refused rollback changed active/history/staged catalog bytes');
  await h.start(tenant);const status=shouldBlock||!test.blocked?409:201;
  await record(test.name+'-post-rollback-invalid-row',await h.call('/entries','POST',{id:randomUUID(),amount:test.bad},tenant),status);
  const after=await record(test.name+'-after-rollback',await h.call('/entries','GET',undefined,tenant),200);assert.deepEqual(after.items.find(x=>x.id===positive.id),positive);if(shouldBlock||!test.blocked)assert.deepEqual(after,initial);else assert.equal(after.items.length,2);
  await h.restart(tenant);assert.deepEqual(await h.call('/entries','GET',undefined,tenant),{status:200,body:after});
  if(shouldBlock||!test.blocked)await record(test.name+'-guard-after-SIGKILL',await h.call('/entries','POST',{id:randomUUID(),amount:test.bad},tenant),409);
  await h.coldRestore(tenant);assert.deepEqual(await h.call('/entries','GET',undefined,tenant),{status:200,body:after});
  if(shouldBlock||!test.blocked)await record(test.name+'-guard-after-cold-restore',await h.call('/entries','POST',{id:randomUUID(),amount:test.bad},tenant),409);
  results.push({case:test.name,rollback_refused:rollback.code!==0,invalid_write_status:status,acknowledged_row_preserved:true,restart_and_cold_restore:true});
 }finally{await h.stop();}
}
const result={passed:true,expectProtected:config.expectProtected,actualProtection:results.filter(x=>x.case!=='compatible').every(x=>x.rollback_refused),same_resource_contracts:true,standalone_no_dependency_mask:true,official_compiler_packages:true,source:config.source,binary:config.binary,binary_sha256:config.binarySha256,no_ERP_or_Hub:true,node_only:true,rust_tests:false,results,records};await fs.writeFile(run+'/evidence/qualification.json',JSON.stringify(result,null,2));console.log(JSON.stringify({run,...result,records:records.length}));
