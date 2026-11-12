// Actual native GET resources, independently checked using BigInt rationals.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {createHash,randomUUID} from 'node:crypto';
import {makeHttpHarness} from '../native-invariant-rollback/http-harness.mjs';
const config=JSON.parse(await fs.readFile(process.argv[2]));
assert.equal(typeof config.expectExact,'boolean');
const bundle=path.dirname(import.meta.filename),fixture=JSON.parse(await fs.readFile(bundle+'/sdk-filters.json'));
const sha=b=>createHash('sha256').update(b).digest('hex');
assert.equal(sha(await fs.readFile(config.binary)),config.binarySha256);
assert.equal(sha(await fs.readFile(bundle+'/'+fixture.package)),fixture.sha256);
assert.equal(sha(await fs.readFile(bundle+'/build-sdk-filters.mjs')),fixture.builder_sha256);
const key=bundle+'/fixtures/signing-key.pub',keyId='qa-decimal-input',tenant=randomUUID(),other=randomUUID();
assert.equal(sha(await fs.readFile(key)),'059aa6c5096b73944c09d562f03786bf5e660f469e7da19495616f838e0355a6');
const run=path.join(config.outputRoot,'decimal-filters-'+Date.now()),database=run+'/database';
await fs.mkdir(run,{recursive:true});await fs.copyFile(import.meta.filename,run+'/driver.mjs');await fs.copyFile(process.argv[2],run+'/config.json');
const log=[],installer=spawn(config.binary,['app','--trusted-key',keyId+'='+key,database,'install',bundle+'/'+fixture.package]);
installer.stdout.on('data',b=>log.push(b));installer.stderr.on('data',b=>log.push(b));
const code=await new Promise((r,j)=>{installer.once('exit',r);installer.once('error',j)});
await fs.writeFile(run+'/install.log',Buffer.concat(log));assert.equal(code,0,Buffer.concat(log).toString());
const h=await makeHttpHarness({database,binary:config.binary,trustedKeys:[{id:keyId,path:key}],evidence:run,readiness:'/sdk-filters/direct_decimal'});
function rational(text){
 const m=String(text).match(/^([+-]?)(\d+)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/);assert.ok(m,'Invalid independent oracle amount '+text);
 let n=BigInt((m[1]==='-'?'-':'')+m[2]+(m[3]||'')),s=BigInt((m[3]||'').length)-BigInt(m[4]||0);
 if(n===0n)return[n,0n];while(n%10n===0n){n/=10n;s--;}return[n,s];
}
function compare(a,b){const [an,as]=rational(a),[bn,bs]=rational(b),s=as>bs?as:bs;const n=an*10n**(s-as)-bn*10n**(s-bs);return n<0n?-1:n>0n?1:0;}
const values=['-79228162514264337593543950335','-0.10','0','0.0000000000000000000000000001','0.10','9007199254740992.01','9007199254740992.02','79228162514264337593543950334','79228162514264337593543950335'];
const invalid=['7922816251426433759354395033.55','79228162514264337593543950335.1','79228162514264337593543950336','1e-29','1.00000000000000000000000000001','1e29','1e9223372036854775807','1e-9223372036854775808','NaN','Infinity','true','null','garbage'];
const records=[],defects=[],snapshots=new Map();
async function call(name,url,method='GET',body,organization=tenant,roles=['probe_operator']){
 const response=await h.call(url,method,body,organization,{},roles);records.push({name,...response});return response;
}
async function checkFilter(name,relation,parameter,value,predicate){
 const r=await call(name,'/sdk-filters/'+relation+'?limit=1000&'+encodeURIComponent(parameter)+'='+encodeURIComponent(value));
 const wanted=snapshots.get(relation).filter(x=>predicate(compare(x.amount,value))).map(x=>x.id).sort();
 const actual=r.status===200?r.body.items.map(x=>x.id).sort():null;
 if(r.status!==200||JSON.stringify(actual)!==JSON.stringify(wanted))defects.push({name,value,status:r.status,expected_ids:wanted,actual_ids:actual});
 if(config.expectExact){assert.equal(r.status,200,JSON.stringify(r));assert.deepEqual(actual,wanted,name);}
}
try{
 await h.start(tenant);
 for(const relation of ['direct_decimal','legacy_decimal']){
  const rows=[];
  for(const amount of values){const r=await call('create-'+relation+'-'+amount,'/sdk-filters/'+relation,'POST',{amount});assert.equal(r.status,201,JSON.stringify(r));assert.equal(compare(r.body.amount,amount),0);assert.equal(r.body.organization_id,tenant);rows.push(r.body);}
  snapshots.set(relation,rows);
  for(const amount of [...values,'0.1','1.0e-1','-0.100','9007199254740992.0100','9007199254740992.015','79228162514264337593543950335.0','7.9228162514264337593543950335e28','792281625142643375935439503350e-1'])await checkFilter('exact-'+relation+'-'+amount,relation,relation==='legacy_decimal'?'filter[amount]':'amount',amount,x=>x===0);
  if(relation==='direct_decimal')for(const amount of ['-79228162514264337593543950335','0','0.0000000000000000000000000001','9007199254740992.015','79228162514264337593543950335','7.9228162514264337593543950335e28'])await checkFilter('minimum-'+amount,relation,'amount_min',amount,x=>x>=0);
  for(const amount of invalid)for(const parameter of relation==='legacy_decimal'?['filter[amount]']:['amount','amount_min']){
   const r=await call('invalid-'+relation+'-'+parameter+'-'+amount,'/sdk-filters/'+relation+'?'+encodeURIComponent(parameter)+'='+encodeURIComponent(amount));
   if(r.status<400||r.status>=500)defects.push({name:'unsupported-filter-accepted',relation,parameter,value:amount,status:r.status});
   if(config.expectExact)assert.ok(r.status>=400&&r.status<500,JSON.stringify(r));
  }
  const foreign=await call('filter-tenant-isolation-'+relation,'/sdk-filters/'+relation+'?'+encodeURIComponent(relation==='legacy_decimal'?'filter[amount]':'amount')+'='+encodeURIComponent(values.at(-1)),'GET',undefined,other);assert.equal(foreign.status,200);assert.deepEqual(foreign.body.items,[]);
  const foreignWrite=await call('foreign-tenant-same-amount-'+relation,'/sdk-filters/'+relation,'POST',{amount:values.at(-1)},other);assert.equal(foreignWrite.status,201);assert.equal(foreignWrite.body.organization_id,other);
  await checkFilter('other-tenant-row-does-not-leak-'+relation,relation,relation==='legacy_decimal'?'filter[amount]':'amount',values.at(-1),x=>x===0);
  const denied=await call('filter-role-denial-'+relation,'/sdk-filters/'+relation,'GET',undefined,tenant,[]);assert.equal(denied.status,403);
 }
 const f=await call('explicit-float-create','/sdk-filters/direct_float','POST',{amount:1e30});assert.equal(f.status,201,JSON.stringify(f));
 for(const parameter of ['amount','amount_min']){const r=await call('explicit-float-filter-'+parameter,'/sdk-filters/direct_float?'+parameter+'=1e30');assert.equal(r.status,200);assert.deepEqual(r.body.items.map(x=>x.id),[f.body.id]);}
 for(const phase of ['SIGKILL','cold-restore']){
  if(phase==='SIGKILL')await h.restart(tenant);else await h.coldRestore(tenant);
  for(const relation of ['direct_decimal','legacy_decimal']){
   const rows=await call('all-rows-preserved-'+phase+'-'+relation,'/sdk-filters/'+relation+'?limit=1000');assert.equal(rows.status,200);assert.deepEqual(rows.body.items.slice().sort((a,b)=>a.id.localeCompare(b.id)),snapshots.get(relation).slice().sort((a,b)=>a.id.localeCompare(b.id)));
   await checkFilter('filter-after-'+phase+'-'+relation,relation,relation==='legacy_decimal'?'filter[amount]':'amount',values.at(-1),x=>x===0);
  }
 }
 if(config.expectExact)assert.equal(defects.length,0);else assert.ok(defects.length>0,'Before-control must reproduce the filter defect');
 const result={passed:true,expectExact:config.expectExact,actualExactFilters:defects.length===0,defects,records:records.length,source:config.source,binary_sha256:config.binarySha256,fixture_sha256:fixture.sha256,source_authored_ABI:true,generated_product_input:false,field_value_type_absent:true,BigInt_oracle:true,actual_database_filters:true,tenant_and_role_denials:true,SIGKILL_and_cold_restore:true,Node_only:true,Rust_tests:false,Hub:false,real_issuer:false};
 await fs.writeFile(run+'/qualification.json',JSON.stringify(result,null,2));console.log(JSON.stringify({run,...result,defects:defects.length}));
}catch(error){await fs.writeFile(run+'/failure.json',JSON.stringify({error:error.stack,records:records.length,defects},null,2));throw error;}
finally{await h.stop();await fs.writeFile(run+'/results.json',JSON.stringify({records},null,2));}
