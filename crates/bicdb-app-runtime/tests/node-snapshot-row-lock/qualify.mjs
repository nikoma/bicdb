import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {createServer as netServer} from 'node:net';
import {createHash,generateKeyPairSync,sign,randomUUID} from 'node:crypto';
import {qualifyColdCopy,assertColdCopySourceUnchanged} from '../node-cold-copy-adoption/adopt.mjs';
const config=JSON.parse(fs.readFileSync(process.argv[2]));
const native=config.native.path,nativeSHA=config.native.sha256;
const sha=file=>createHash('sha256').update(fs.readFileSync(file)).digest('hex');
assert.equal(sha(native),nativeSHA);
const pins=JSON.parse(fs.readFileSync(config.packagePins));
const artifact=config.artifact??pins.artifact,key=config.publicKey??pins.publicKey;assert.equal(sha(artifact),pins.sha256);
const out=path.resolve(process.argv[3]);fs.mkdirSync(out,{mode:0o700});
const durable=(name,value)=>{const fd=fs.openSync(out+'/'+name,'w',0o600);try{fs.writeFileSync(fd,JSON.stringify(value,null,2)+'\n');fs.fsyncSync(fd);}finally{fs.closeSync(fd);}};
const events=[],commands=[],records=[];let holderResponse,holderSignal,waiterSignal,caseActive=false;
const signal=()=>{let resolve;const promise=new Promise(r=>resolve=r);return{promise,resolve};};
const within=(promise,label)=>Promise.race([promise,new Promise((_,reject)=>{const timer=setTimeout(()=>reject(Error(label+' original2s barrier deadline')),2000);timer.unref();})]);
const barrier=createServer(async(req,res)=>{
  if(req.method==='GET'&&req.url==='/health'){res.writeHead(200,{'content-type':'application/json'}).end('{}');return;}
  const chunks=[];for await(const chunk of req)chunks.push(chunk);
  let body;try{body=JSON.parse(Buffer.concat(chunks).toString());}catch{res.writeHead(400).end();return;}
  events.push({at:new Date().toISOString(),path:req.url,body});durable('barrier-events.json',events);
  if(req.url==='/hold'){
    if(holderResponse){res.writeHead(409).end('{}');return;}
    if(!caseActive){res.writeHead(200,{'content-type':'application/json'}).end('{"release":true}');return;}
    holderResponse=res;holderSignal.resolve(body);return;
  }
  if(req.url==='/waiter'){
    waiterSignal?.resolve(body);res.writeHead(200,{'content-type':'application/json'}).end('{"release":true}');
    // The writer still owns the row while the waiter leaves this exact barrier.
    // Release after100ms so the subsequent locking read has to wait for commit.
    if(holderResponse){const response=holderResponse;holderResponse=null;setTimeout(()=>response.writeHead(200,{'content-type':'application/json'}).end('{"release":true}'),100);}
    return;
  }
  if(req.url==='/after-lock'){res.writeHead(200,{'content-type':'application/json'}).end('{"release":true}');return;}
  res.writeHead(404).end('{}');
});
await new Promise(r=>barrier.listen(0,'127.0.0.1',r));
durable('integration.json',{version:1,http:[{application:pins.application,provider:'LockBarrier',base_url:'http://127.0.0.1:'+barrier.address().port,healthcheck_path:'/health',healthcheck_statuses:[200],allow_insecure_http:true,allow_private_networks:true}]});
let database=out+'/database';
const args=(store=database)=>['app','--integration-config',out+'/integration.json','--trusted-key',pins.keyId+'='+key,store];
async function cli(name,argv){const chunks=[],child=spawn(native,argv);child.stdout.on('data',b=>chunks.push(b));child.stderr.on('data',b=>chunks.push(b));const code=await new Promise((r,j)=>{child.once('error',j);child.once('exit',r);});const result={name,argv,code,output:Buffer.concat(chunks).toString()};commands.push(result);durable('commands.json',commands);assert.equal(code,0,JSON.stringify(result));}
const pair=generateKeyPairSync('rsa',{modulusLength:2048});
durable('jwks.json',{keys:[{...pair.publicKey.export({format:'jwk'}),kid:'row-lock-private',alg:'RS256',use:'sig'}]});
durable('auth.json',{schemes:[{kind:'oidc_rs256',issuer:'https://issuer.invalid/row-lock',audience:'row-lock-probe',jwks_file:'jwks.json'}]});
const enc=x=>Buffer.from(JSON.stringify(x)).toString('base64url');
function authorization(tenant='tenant-lock-a',roles=['probe_operator']){const now=Math.floor(Date.now()/1000),claims={iss:'https://issuer.invalid/row-lock',aud:'row-lock-probe',sub:'42',roles,tenant_id:tenant,iat:now,exp:now+3600};const b=enc({alg:'RS256',kid:'row-lock-private'})+'.'+enc(claims);return{claims,bearer:b+'.'+sign('RSA-SHA256',Buffer.from(b),pair.privateKey).toString('base64url')};}
async function port(){const s=netServer();await new Promise(r=>s.listen(0,'127.0.0.1',r));const p=s.address().port;await new Promise(r=>s.close(r));return p;}
const http=await port(),pg=await port();let server;const logs=[];
let requestNumber=0;const activeRequests=new Set();
function call(url,body,tenant='tenant-lock-a',roles=['probe_operator']){
 const promise=(async()=>{const started=Date.now(),requestId=++requestNumber,auth=authorization(tenant,roles);durable('request-'+requestId+'-before.json',{url:'http://127.0.0.1:'+http+url,method:body===undefined?'GET':'POST',body:body??null,bodyJSON:body===undefined?null:JSON.stringify(body),claims:auth.claims,idempotencyKey:null,deadlineMs:30000});let received=null;
  try{const r=await fetch('http://127.0.0.1:'+http+url,{method:body===undefined?'GET':'POST',headers:{Authorization:'Bearer '+auth.bearer,'content-type':'application/json'},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(30000)});received={status:r.status,headers:Object.fromEntries([...r.headers].filter(([name])=>!['authorization','set-cookie'].includes(name.toLowerCase())))};const raw=await r.text();let result;try{result=JSON.parse(raw);}catch{result=raw;}const response={status:r.status,body:result,ms:Date.now()-started};durable('request-'+requestId+'-response.json',{settled:'fulfilled',response});return response;}
  catch(error){durable('request-'+requestId+'-response.json',{settled:'rejected',received,error:{name:error.name,message:error.message,code:error.code??error.cause?.code??null}});throw error;}
 })();activeRequests.add(promise);promise.then(()=>activeRequests.delete(promise),()=>activeRequests.delete(promise));return promise;
}
async function record(name,promise,status){const r=await promise;records.push({name,...r});durable('records.json',records);if(status!==undefined)assert.equal(r.status,status,JSON.stringify(r));return r;}
async function stop(){if(server&&server.exitCode===null&&server.signalCode===null){const done=new Promise(r=>server.once('exit',r));server.kill('SIGTERM');await done;}}
const cases=[],issues=[],knownRows=[];
const requireGate=(condition,message,context)=>{if(!condition){issues.push({message,context});durable('issues.json',issues);}};
async function start(){server=spawn(native,[...args(),'serve','--http-port',String(http),'--pg-port',String(pg),'--pg-require-auth','--auth-config',out+'/auth.json']);server.stdout.on('data',b=>logs.push(b));server.stderr.on('data',b=>logs.push(b));durable('owned-pid.json',{pid:server.pid,database});const until=Date.now()+30000;let last;while(Date.now()<until){if(server.exitCode!==null||server.signalCode!==null)throw Error(Buffer.concat(logs).toString());try{last=await call('/probe/counters');if(last.status===200)return;}catch{}await new Promise(r=>setTimeout(r,100));}throw Error('Original30s readiness limit: '+JSON.stringify(last));}
async function contention(isolation,label){
 const row=(await record(label+' '+isolation+' create',call('/probe/counters',{organization_id:'tenant-lock-a',value:0}),201)).body;
 const waiterPath=isolation==='read_committed'?'/probe/waiter':isolation==='serializable'?'/probe/waiter-serializable':'/probe/waiter-repeatable-read';
 const intents=[{name:'holder',url:'/probe/holder',body:{id:row.id},subject:'42',tenant:'tenant-lock-a',roles:['probe_operator'],idempotencyKey:null},{name:'waiter',url:waiterPath,body:{id:row.id},subject:'42',tenant:'tenant-lock-a',roles:['probe_operator'],idempotencyKey:null}];durable(label+'-'+isolation+'-intents.json',intents);
 holderSignal=signal();waiterSignal=signal();caseActive=true;
 const holder=call(intents[0].url,intents[0].body);holder.catch(()=>{});
 const held=await within(holderSignal.promise,'Holder arrival');assert.equal(held.id,row.id);assert.equal(held.value,0);
 const waiter=call(intents[1].url,intents[1].body);waiter.catch(()=>{});const observed=await within(waiterSignal.promise,'Waiter snapshot');assert.equal(observed.id,row.id);assert.equal(observed.value,0);
 const settled=await Promise.allSettled([holder,waiter]);caseActive=false;const outcomes=settled.map((x,i)=>x.status==='fulfilled'?{...intents[i],settled:x.status,response:x.value}:{...intents[i],settled:x.status,error:{name:x.reason?.name,message:x.reason?.message}});durable(label+'-'+isolation+'-settled.json',outcomes);
 const readback=await record(label+' '+isolation+' authenticated final row',call('/probe/counters/'+row.id),200);durable(label+'-'+isolation+'-readback.json',readback);assert.equal(readback.body.value,1);assert.equal(readback.body.version,2);knownRows.push(readback.body);
 requireGate(outcomes[0].response?.status===200,'Holder must commit once',outcomes);
 const response=outcomes[1].response,internal=events.filter(e=>e.path==='/after-lock'&&e.body.id===row.id&&e.body.isolation===isolation);
 requireGate(!!response,'Transport ambiguity is a failed probe',outcomes);
 if(isolation==='read_committed'){requireGate(response?.status===200&&response.body.before===0&&response.body.locked===1,'RC locking read must return fresh committed value',response);requireGate(internal.length===1&&internal[0].body.locked===1,'RC action callback must see fresh1 exactly once',internal);}
 else{requireGate(response?.status===409,'Snapshot locking read must return conflict',response);requireGate(internal.length===0,'Snapshot conflict must precede ALL post-lock callbacks',internal);}
 cases.push({label,isolation,holder:outcomes[0],waiter:outcomes[1],internalAfterLock:internal.map(e=>e.body),internalStaleLockObserved:internal.some(e=>e.body.locked===0),conflictBeforeAfterLock:response?.status===409&&internal.length===0});durable('cases.json',cases);
}
async function authorizationControls(){const row=(await record('authorization control create',call('/probe/counters',{organization_id:'tenant-lock-a',value:0}),201)).body;await record('foreign tenant GET',call('/probe/counters/'+row.id,undefined,'tenant-lock-b'),404);for(const url of ['/probe/holder','/probe/waiter','/probe/waiter-repeatable-read','/probe/waiter-serializable','/probe/own-write']){const before=events.length;const denied=await record('role denied POST '+url,call(url,{id:row.id},'tenant-lock-a',[]),403);const foreign=await record('foreign tenant POST '+url,call(url,{id:row.id},'tenant-lock-b'));requireGate(foreign.status===403||foreign.status===404,'Foreign tenant lock action must be refused before provider work',foreign);requireGate(events.length===before,'Denied lock POST must emit no barrier event',{url,events:events.slice(before)});}const after=await record('denied controls preserve owned row',call('/probe/counters/'+row.id),200);assert.deepEqual(after.body,row);knownRows.push(row);}
async function ownPendingControls(){const row=(await record('own-write create',call('/probe/counters',{organization_id:'tenant-lock-a',value:0}),201)).body;for(const [kind,id,before,locked]of[['own-write',row.id,0,10],['own-insert',randomUUID(),7,7]]){const response=await record(kind+' locks own pending version',call('/probe/'+kind,{id}),200);assert.deepEqual(response.body,{before,locked});const observed=events.filter(e=>e.path==='/after-lock'&&e.body.id===id&&e.body.isolation===kind.replace('-','_'));requireGate(observed.length===1&&observed[0].body.locked===locked,'Own pending row must remain visible under snapshot lock',observed);const read=await record(kind+' committed state',call('/probe/counters/'+id),200);assert.equal(read.body.value,locked);assert.equal(read.body.version,kind==='own-write'?2:1);knownRows.push(read.body);}}
async function recoveryReads(label){for(const row of knownRows){const actual=await record(label+' owned row '+row.id,call('/probe/counters/'+row.id),200);assert.deepEqual(actual.body,row);}}
try{
 await cli('install signed source fixture',[...args(),'install',artifact]);await start();await authorizationControls();
 for(const isolation of ['read_committed','repeatable_read','serializable'])await contention(isolation,'initial');await ownPendingControls();
 const crashed=new Promise(r=>server.once('exit',r));server.kill('SIGKILL');await crashed;await start();await recoveryReads('SIGKILL');for(const isolation of ['read_committed','repeatable_read','serializable'])await contention(isolation,'after-SIGKILL');
 await stop();const cold=out+'/cold';const custody=await qualifyColdCopy({native:config.native,source:database,target:cold,evidence:out+'/cold-custody',closedSource:{pid:server.pid,exitCode:server.exitCode,signalCode:server.signalCode},openArgs:db=>[...args(db),'list']});database=cold;await start();await recoveryReads('cold');for(const isolation of ['read_committed','repeatable_read','serializable'])await contention(isolation,'after-cold');await stop();await assertColdCopySourceUnchanged(custody);
 durable('qualification.json',{passed:issues.length===0,native:{path:native,sha256:nativeSHA},source:pins.sourceSHA256,artifact:pins.sha256,cases,issues,knownRows:knownRows.length,ERPStoresTouched:false,AuthAccountsChanged:false,mainAccountUntouched:true,RustTests:false,scope:'Actual native source locking API; snapshot conflicts precede callbacks, RC fresh read, authorization/tenant refusal, own pending insert/write, SIGKILL and cold restore; no stock-timeout causality claim'});if(issues.length)process.exitCode=2;console.log('Evidence '+out+' issues '+issues.length);
}finally{await Promise.allSettled([...activeRequests]);await stop();if(holderResponse)holderResponse.destroy();await new Promise(r=>barrier.close(r));fs.writeFileSync(out+'/server.log',Buffer.concat(logs));}
