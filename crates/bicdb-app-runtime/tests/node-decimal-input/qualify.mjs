// Public, authenticated native regression. Expected money uses independent
// BigInt coefficient/scale arithmetic; no Rust test helper is built or run.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {createHash,randomUUID} from 'node:crypto';
import {makeHttpHarness} from '../native-invariant-rollback/http-harness.mjs';
const config=JSON.parse(await fs.readFile(process.argv[2]));
assert.equal(typeof config.expectExact,'boolean');assert.equal(typeof JSON.rawJSON,'function','Node22+ exact numeric payloads required');
const bundle=path.dirname(import.meta.filename),fixture=JSON.parse(await fs.readFile(bundle+'/fixture.json'));
const hash=b=>createHash('sha256').update(b).digest('hex');
assert.equal(hash(await fs.readFile(config.binary)),config.binarySha256);
const artifact=bundle+'/'+fixture.package,key=bundle+'/'+fixture.public_key;
assert.equal(hash(await fs.readFile(artifact)),fixture.package_sha256);assert.equal(hash(await fs.readFile(key)),fixture.public_key_sha256);
const run=path.join(config.outputRoot,'decimal-input-'+Date.now()),database=run+'/database',tenant=randomUUID(),other=randomUUID();
await fs.mkdir(run,{recursive:true});await fs.copyFile(import.meta.filename,run+'/driver.mjs');await fs.copyFile(process.argv[2],run+'/config.json');
const output=[],installer=spawn(config.binary,['app','--trusted-key',fixture.key_id+'='+key,database,'install',artifact]);
installer.stdout.on('data',b=>output.push(b));installer.stderr.on('data',b=>output.push(b));
const code=await new Promise((r,j)=>{installer.once('exit',r);installer.once('error',j)});await fs.writeFile(run+'/install.log',Buffer.concat(output));assert.equal(code,0,Buffer.concat(output).toString());
const h=await makeHttpHarness({database,binary:config.binary,trustedKeys:[{id:fixture.key_id,path:key}],evidence:run,readiness:'/decimal-input/rows'});
const MAX=79228162514264337593543950335n;
function rational(text){
 assert.equal(typeof text,'string');const match=text.replaceAll('_','').match(/^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/);assert.ok(match&&(match[2]||match[3]),'Decimal oracle syntax: '+text);
 let coefficient=BigInt((match[1]==='-'?'-':'')+(match[2]||'0')+(match[3]||'')),scale=BigInt((match[3]||'').length)-BigInt(match[4]||0);
 if(coefficient===0n)return{coefficient:0n,scale:0n};
 while(coefficient%10n===0n){coefficient/=10n;scale--;}
 if(scale<0n&&scale>=-28n){coefficient*=10n**(-scale);scale=0n;}
 return{coefficient,scale};
}
function exact(text){const n=rational(text);return n.scale>=0n&&n.scale<=28n&&n.coefficient>=-MAX&&n.coefficient<=MAX;}
function equal(left,right){assert.deepEqual(rational(left),rational(right),'Money changed: '+left+' versus '+right);}
function compare(left,right){const a=rational(left),b=rational(right),scale=a.scale>b.scale?a.scale:b.scale;const difference=a.coefficient*10n**(scale-a.scale)-b.coefficient*10n**(scale-b.scale);return difference<0n?-1:difference>0n?1:0;}
const format=(coefficient,scale)=>{const s=coefficient.toString().padStart(scale+1,'0');return scale?s.slice(0,-scale)+'.'+s.slice(-scale):s;};
const valid=['0','-0.00','0.10','+001.2300','1_000.0100','9007199254740992.01',MAX.toString(),'-'+MAX,MAX+'.0','1e-28','1.2300e2','1e28',...Array.from({length:7},(_,scale)=>format(MAX,scale)),format(MAX,28)];
const enhanced=[MAX+'0e-1','-'+MAX+'0e-1','0e-1000','0e1000'];
const invalid=['7922816251426433759354395033.55',MAX+'.1',(MAX+1n).toString(),'1e-29','-1e-29','1.00000000000000000000000000001','1e29','1.01e-28','1e9223372036854775807','1e-9223372036854775808'];
for(const text of[...valid,...enhanced])assert.ok(exact(text),text);for(const text of invalid)assert.ok(!exact(text),text);
const records=[],unsafe=[],validRejected=[];let criticalRounded=false,replay;
async function record(name,response){records.push({name,...response});if(records.length%10===0)await fs.writeFile(run+'/results.json',JSON.stringify({expectExact:config.expectExact,records},null,2));return response;}
async function all(){const rows=[];for(let offset=0;;offset+=100){const r=await h.call('/decimal-input/rows?limit=100&offset='+offset,'GET',undefined,tenant);assert.equal(r.status,200);rows.push(...r.body.items);if(r.body.items.length<100)break;}return rows.sort((a,b)=>a.id.localeCompare(b.id));}
const writeModes=[{name:'typed',url:'/decimal-input/typed',status:200},{name:'model',url:'/decimal-input/raw-model',status:200},{name:'crud',url:'/decimal-input/rows',status:201},{name:'builtin',url:'/decimal-input/parse',status:200}];
function variants(text){const output=[{name:'string',value:text}];if(/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(text))output.push({name:'numeric',value:JSON.rawJSON(text)});return output;}
try{
 await h.start(tenant);
 for(const text of[...valid,...enhanced]){
  for(const variant of variants(text))for(const mode of writeModes){
   if(mode.name==='builtin'&&variant.name==='numeric')continue;
   const id=randomUUID(),body={id,amount:variant.value},key=randomUUID(),response=await record('valid-'+mode.name+'-'+variant.name+'-'+text,await h.call(mode.url,'POST',body,tenant,{'Idempotency-Key':key}));
   if(!config.expectExact&&enhanced.includes(text)&&response.status>=400&&response.status<500){validRejected.push({mode:mode.name,text,legacy_syntax_limit:true});continue;}
   assert.equal(response.status,mode.status,JSON.stringify(response));equal(response.body.amount,text);assert.equal(response.body.organization_id,tenant);
   if(!replay&&mode.name==='typed'&&text==='0.10')replay={url:mode.url,body,key,response};
  }
  for(const location of['query','path']){
   const url=location==='query'?'/decimal-input/query?amount='+encodeURIComponent(text):'/decimal-input/path/'+encodeURIComponent(text),response=await record('valid-'+location+'-'+text,await h.call(url,'GET',undefined,tenant));
   if(!config.expectExact&&enhanced.includes(text)&&response.status>=400&&response.status<500)continue;
   assert.equal(response.status,200,JSON.stringify(response));equal(response.body.amount,text);
  }
 }
 for(const text of invalid)for(const variant of variants(text))for(const mode of writeModes){
  if(mode.name==='builtin'&&variant.name==='numeric')continue;
  const before=await all(),id=randomUUID(),response=await record('invalid-'+mode.name+'-'+variant.name+'-'+text,await h.call(mode.url,'POST',{id,amount:variant.value},tenant,{'Idempotency-Key':randomUUID()}));
  if(response.status>=200&&response.status<300){
   unsafe.push({text,mode:mode.name,encoding:variant.name,returned:response.body.amount});
   if(text===invalid[0]&&mode.name==='typed'&&variant.name==='string'){assert.notDeepEqual(rational(response.body.amount),rational(text));criticalRounded=true;}
   assert.equal(config.expectExact,false,'Unsupported Decimal was accepted: '+JSON.stringify(response));
  }else{assert.ok(response.status>=400&&response.status<500,JSON.stringify(response));assert.deepEqual(await all(),before,'Refused amount changed rows');}
 }
 for(const text of invalid)for(const location of['query','path']){
  const url=location==='query'?'/decimal-input/query?amount='+encodeURIComponent(text):'/decimal-input/path/'+encodeURIComponent(text),r=await record('invalid-'+location+'-'+text,await h.call(url,'GET',undefined,tenant));
  if(r.status===200){unsafe.push({text,mode:location,returned:r.body.amount});assert.equal(config.expectExact,false,'Unsupported query/path Decimal accepted');}else assert.ok(r.status>=400&&r.status<500,JSON.stringify(r));
 }
 for(const route of['typed-batch','raw-batch']){
  const before=await all(),body={rows:[{id:randomUUID(),amount:'0.10'},{id:randomUUID(),amount:invalid[0]}]},r=await record('late-invalid-'+route,await h.call('/decimal-input/'+route,'POST',body,tenant,{'Idempotency-Key':randomUUID()}));
  if(r.status===200){unsafe.push({mode:route,text:invalid[0]});assert.equal(config.expectExact,false,'Invalid late batch amount committed');}else{assert.ok(r.status>=400&&r.status<500,JSON.stringify(r));assert.deepEqual(await all(),before,'Late invalid batch left partial rows');}
 }
 for(const amount of[null,true,'NaN','Infinity','garbage']){
  const before=await all(),r=await record('wrong-decimal-type-'+String(amount),await h.call('/decimal-input/typed','POST',{id:randomUUID(),amount},tenant,{'Idempotency-Key':randomUUID()}));assert.ok(r.status>=400&&r.status<500);assert.deepEqual(await all(),before);
 }
 assert.equal((await record('nullable-null',await h.call('/decimal-input/optional','POST',{amount:null},tenant))).status,200);
 const float=await record('explicit-float-domain-unchanged',await h.call('/decimal-input/float','POST',{amount:1e30},tenant));assert.equal(float.status,200);assert.equal(float.body.amount,1e30);
 assert.ok(replay);assert.deepEqual(await h.call(replay.url,'POST',replay.body,tenant,{'Idempotency-Key':replay.key}),replay.response);
 const snapshot=await all();
 for(const amount of ['0','1e-28','9007199254740992.01','9007199254740992.015',MAX.toString(),'7.9228162514264337593543950335e28'])for(const parameter of ['amount','amount_min']){
  const r=await record('database-filter-'+parameter+'-'+amount,await h.call('/decimal-input/rows?limit=1000&'+parameter+'='+encodeURIComponent(amount),'GET',undefined,tenant));assert.equal(r.status,200,JSON.stringify(r));
  const wanted=snapshot.filter(row=>parameter==='amount'?compare(row.amount,amount)===0:compare(row.amount,amount)>=0).map(row=>row.id).sort();assert.deepEqual(r.body.items.map(row=>row.id).sort(),wanted,'Database Decimal filter changed meaning');
 }
 for(const amount of invalid)for(const parameter of ['amount','amount_min']){
  const r=await record('invalid-database-filter-'+parameter+'-'+amount,await h.call('/decimal-input/rows?'+parameter+'='+encodeURIComponent(amount),'GET',undefined,tenant));
  if(r.status===200){unsafe.push({text:amount,mode:'database-filter-'+parameter});assert.equal(config.expectExact,false,'Unsupported Decimal resource filter accepted');}else assert.ok(r.status>=400&&r.status<500,JSON.stringify(r));
 }
 assert.deepEqual(await all(),snapshot,'Read filters changed rows');
 const foreign=await record('other-tenant-cannot-read',await h.call('/decimal-input/rows/'+replay.response.body.id,'GET',undefined,other));assert.equal(foreign.status,404);
 const denied=await record('wrong-role-cannot-create',await h.call('/decimal-input/typed','POST',{id:randomUUID(),amount:'1'},tenant,{'Idempotency-Key':randomUUID()},[]));assert.equal(denied.status,403);assert.deepEqual(await all(),snapshot);
 for(const phase of['SIGKILL','cold-restore']){
  if(phase==='SIGKILL')await h.restart(tenant);else await h.coldRestore(tenant);
  assert.deepEqual(await all(),snapshot,'Acknowledged rows changed after '+phase);
  assert.deepEqual(await h.call(replay.url,'POST',replay.body,tenant,{'Idempotency-Key':replay.key}),replay.response);
  if(config.expectExact){const r=await record(phase+'-invalid-input-still-refused',await h.call('/decimal-input/typed','POST',{id:randomUUID(),amount:invalid[0]},tenant,{'Idempotency-Key':randomUUID()}));assert.ok(r.status>=400&&r.status<500);assert.deepEqual(await all(),snapshot);}
 }
 const actualExactAdmission=unsafe.length===0;
 if(config.expectExact)assert.equal(actualExactAdmission,true);else{assert.equal(criticalRounded,true,'Original exact typed-input defect was not reproduced');assert.ok(unsafe.length>0);}
 await fs.writeFile(run+'/qualification.json',JSON.stringify({passed:true,expectExact:config.expectExact,actualExactAdmission,critical_rounding_reproduced:criticalRounded,unsupported_accepted:unsafe,legacy_syntax_limits:validRejected,record_count:records.length,binary_sha256:config.binarySha256,source:config.source,fixture_sha256:fixture.package_sha256,node_only:true,rust_tests:false,ordinary_native:true,synthetic_verifier:true,real_issuer:false,Hub:false,main_account_untouched:true,late_batch_atomicity:true,valid_rows_and_replays_survive_SIGKILL_and_cold_restore:true,raw_JSON_numeric_tokens:true,actual_database_Decimal_exact_and_minimum_filters:true},null,2));
 console.log(JSON.stringify({run,actualExactAdmission,expectExact:config.expectExact,records:records.length,unsupported_accepted:unsafe.length}));
}catch(error){await fs.writeFile(run+'/failure.json',JSON.stringify({error:error.stack,records:records.length},null,2));throw error;}
finally{await h.stop();await fs.writeFile(run+'/results.json',JSON.stringify({records},null,2));}
