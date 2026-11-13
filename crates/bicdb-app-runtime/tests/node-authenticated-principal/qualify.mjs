// Actual native qualification, never a substitute for building the runtime.
// Usage: node qualify.mjs config.json new-private-output-directory
// config: {native:{path,sha256},oldNative:{path,sha256}}
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {spawn} from 'node:child_process';
import {createHash,generateKeyPairSync,sign} from 'node:crypto';
import {createServer} from 'node:net';
import {subjects} from './vectors.mjs';
import {qualifyColdCopy,assertColdCopySourceUnchanged} from '../node-cold-copy-adoption/adopt.mjs';
const config=JSON.parse(await fs.readFile(process.argv[2]));
const out=path.resolve(process.argv[3]);
await fs.mkdir(out,{recursive:false,mode:0o700});
const sha=b=>createHash('sha256').update(b).digest('hex');
for(const pin of [config.native,config.oldNative])assert.equal(sha(await fs.readFile(pin.path)),pin.sha256,'Exact native pin');
const records=[],commands=[];
async function command(binary,args,name,expected=0){
  const chunks=[],p=spawn(binary,args);p.stdout.on('data',x=>chunks.push(x));p.stderr.on('data',x=>chunks.push(x));
  const code=await new Promise((r,j)=>{p.once('error',j);p.once('exit',r)}),output=Buffer.concat(chunks).toString();
  const result={name,args,code,output};commands.push(result);await fs.writeFile(out+'/commands.json',JSON.stringify(commands,null,2)+'\n');
  expected===0?assert.equal(code,0,name+': '+output):assert.notEqual(code,0,name+' must refuse');return result;
}
await command(process.execPath,[path.join(import.meta.dirname,'build-sdk.mjs'),config.native.path,out+'/sdk'],'official SDK build');
const sdk=JSON.parse(await fs.readFile(out+'/sdk/vectors.json'));
let database=out+'/database';
const args=(db=database)=>['app','--trusted-key','qa-principal='+sdk.publicKey,db];
for(const vector of sdk.negative){
  const refusal=await command(config.native.path,[...args(),'validate',vector.path],vector.name,1);
  assert.match(refusal.output,/authenticated_principal|auth.current_principal|zero arguments/i);
}
await command(config.oldNative.path,[...args(out+'/old-reader'),'validate',sdk.legacy],'old reader legacy accepts');
const old=await command(config.oldNative.path,[...args(out+'/old-reader'),'validate',sdk.positive],'old reader new feature refuses',1);
assert.match(old.output,/authenticated_principal|unknown variant|unknown.*feature/i);
for(const key of ['legacy','positive','owner','caller'])await command(config.native.path,[...args(),'stage',sdk[key]],'stage '+key);
await command(config.native.path,[...args(),'activate-batch','qualification.principal_legacy','qualification.principal_sdk','qualification.principal_owner','qualification.principal_caller'],'activate exact SDK cohort');
const pair=generateKeyPairSync('rsa',{modulusLength:2048});
await fs.writeFile(out+'/jwks.json',JSON.stringify({keys:[{...pair.publicKey.export({format:'jwk'}),kid:'principal-private',alg:'RS256',use:'sig'}]}));
await fs.writeFile(out+'/auth.json',JSON.stringify({schemes:[{kind:'oidc_rs256',issuer:'https://issuer.invalid/principal',audience:'principal-native-probe',jwks_file:'jwks.json'}]}));
const encode=x=>Buffer.from(JSON.stringify(x)).toString('base64url');
function token(overrides={}){
  const now=Math.floor(Date.now()/1000),claims={iss:'https://issuer.invalid/principal',aud:'principal-native-probe',sub:'00042',roles:['issuer_operator','custodian'],scopes:['probe:principal'],tenant_id:'tenant-a',workspace_id:'workspace-a',iat:now,exp:now+3600,...overrides};
  for(const key of Object.keys(claims))if(claims[key]===undefined)delete claims[key];
  const bytes=encode({alg:'RS256',kid:'principal-private'})+'.'+encode(claims);
  return bytes+'.'+sign('RSA-SHA256',Buffer.from(bytes),pair.privateKey).toString('base64url');
}
const port=async()=>{const s=createServer();await new Promise(r=>s.listen(0,'127.0.0.1',r));const p=s.address().port;await new Promise(r=>s.close(r));return p;};
const http=await port(),pg=await port(),logs=[];let server;
async function request(url,claims={},extra={}){
  const r=await fetch('http://127.0.0.1:'+http+url,{headers:{...(claims===null?{}:{Authorization:'Bearer '+token(claims)}),...extra},signal:AbortSignal.timeout(30000)});
  const text=await r.text();let body;try{body=JSON.parse(text);}catch{body=text;}return {status:r.status,body};
}
async function record(name,promise,status=200){const r=await promise;records.push({name,...r});await fs.appendFile(out+'/records.jsonl',JSON.stringify({name,...r})+'\n');assert.equal(r.status,status,JSON.stringify({name,...r}));return r.body;}
async function start(){
  server=spawn(config.native.path,[...args(),'serve','--http-port',String(http),'--pg-port',String(pg),'--pg-require-auth','--auth-config',out+'/auth.json']);
  server.stdout.on('data',x=>logs.push(x));server.stderr.on('data',x=>logs.push(x));
  await fs.writeFile(out+'/owned-pid.json',JSON.stringify({pid:server.pid,database}));
  const until=Date.now()+30000;while(Date.now()<until){if(server.exitCode!==null||server.signalCode!==null)throw Error(Buffer.concat(logs).toString());try{if((await request('/sdk-principal/current')).status===200)return;}catch{}await new Promise(r=>setTimeout(r,100));}throw Error('Original30s readiness deadline failed');
}
async function stop(signal='SIGTERM'){if(server&&server.exitCode===null&&server.signalCode===null){const ended=new Promise(r=>server.once('exit',r));server.kill(signal);await ended;}}
function expected(subject,claims={}){return {subject,roles:[...(claims.roles??['issuer_operator','custodian'])].sort(),scopes:[...(claims.scopes??['probe:principal'])].sort(),tenant_id:claims.tenant_id===undefined?'tenant-a':claims.tenant_id,workspace_id:claims.workspace_id===undefined?'workspace-a':claims.workspace_id};}
async function identityCases(label){
  for(const subject of subjects){
    const body=await record(label+' exact subject '+JSON.stringify(subject),request('/sdk-principal/current',{sub:subject}));
    assert.deepEqual(body,expected(subject));
    const cross=await record(label+' propagated owner '+JSON.stringify(subject),request('/sdk-principal/cross-owner',{sub:subject}));
    assert.deepEqual(cross.caller,expected(subject));
    // These read-only manifests have equal signed authority; the existing host
    // retains these effective grants. This does not prescribe raw JWT grant
    // propagation for imports whose existing authority attenuates them.
    assert.deepEqual(cross.owner,cross.caller);
    const legacy=await record(label+' legacy '+JSON.stringify(subject),request('/sdk-principal/legacy',{sub:subject}),['00042','42'].includes(subject)?200:400);
    if(['00042','42'].includes(subject))assert.equal(legacy.id,42);
    else assert.match(JSON.stringify(legacy),/numeric BicDB user subject/);
  }
}
try{
  await start();await identityCases('initial');
  await record('protected missing bearer',request('/sdk-principal/current',null),401);
  await record('public accessor requires verified actor',request('/sdk-principal/public',null),403);
  for(const [name,claims] of [['wrong issuer',{iss:'https://wrong.invalid'}],['wrong audience',{aud:'wrong'}],['expired',{iat:Math.floor(Date.now()/1000)-600,exp:Math.floor(Date.now()/1000)-60}],['empty subject',{sub:''}]])await record(name,request('/sdk-principal/current',claims),401);
  for(const roles of [['issuer_operator'],['custodian'],[]])await record('dual grants refuses '+JSON.stringify(roles),request('/sdk-principal/dual',{roles}),403);
  assert.deepEqual(await record('dual grants accepts',request('/sdk-principal/dual')),expected('00042'));
  await record('required scope missing',request('/sdk-principal/current',{scopes:[]}),403);
  assert.deepEqual(await record('untrusted identity headers ignored',request('/sdk-principal/current',{sub:'操作者:α/零'},{'x-user-id':'42','x-tenant-id':'tenant-b','x-workspace-id':'workspace-b','x-roles':'controller'})),expected('操作者:α/零'));
  const nullable=await record('optional tenant/workspace absence preserved',request('/sdk-principal/current',{tenant_id:undefined,workspace_id:undefined}));
  assert.deepEqual(nullable,{subject:'00042',roles:['custodian','issuer_operator'],scopes:['probe:principal'],tenant_id:null,workspace_id:null});
  const alternate={tenant_id:'tenant-b/零',workspace_id:'workspace-b/α'};
  assert.deepEqual(await record('verified alternate tenant/workspace preserved',request('/sdk-principal/current',alternate)),expected('00042',alternate));
  await stop('SIGKILL');await start();await identityCases('SIGKILL restart');await stop();
  const cold=out+'/cold';const custody=await qualifyColdCopy({native:config.native,source:database,target:cold,evidence:out+'/cold-custody',closedSource:{pid:server.pid,exitCode:server.exitCode,signalCode:server.signalCode},openArgs:db=>[...args(db),'list']});database=cold;
  await start();await identityCases('officially assessed/adopted closed store cold copy');await stop();await assertColdCopySourceUnchanged(custody);
  await fs.writeFile(out+'/qualification.json',JSON.stringify({passed:true,records:records.length,native:config.native,oldReader:config.oldNative,scope:'Source-authored SDK strict ABI and ordinary-native verified identity, legacy numeric accessor, imported same-transaction effective identity, verifier and route grant refusals, actual SIGKILL/restart and closed-store cold restore',sourceCompilerWorkflowQualified:false,realIssuer:false,Hub:false,AuthAccountsChanged:false,mainAccountUntouched:true},null,2)+'\n');
  console.log('Principal SDK PASS '+records.length+' records '+out);
}finally{await stop();await fs.writeFile(out+'/server.log',Buffer.concat(logs));}
