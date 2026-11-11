// Isolated protocol regression with a fixture issuer. Real customer/Hub
// acceptance must use the actual issuer separately.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {generateKeyPairSync,randomBytes,sign} from 'node:crypto';
import {mkdtemp,mkdir,readFile,writeFile,cp,rename} from 'node:fs/promises';
import {request as httpsRequest} from 'node:https';
import {request as httpRequest} from 'node:http';
import {createServer} from 'node:net';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const root=await mkdtemp(join(tmpdir(),'bicdb-native-oidc-'));
const binary=resolve(process.env.BICDB_BINARY??'target/release/bicdb');
const compiler=process.env.CARRIER_BINARY??'carrier';
const tls=!process.argv.includes('--loopback-http');
const environment={...process.env,CARRIER_BICDB_SIGNING_KEY:randomBytes(32).toString('hex'),CARRIER_BICDB_SIGNING_KEY_ID:'native-oidc-qualification'};
const results=[];
async function command(executable,args,name,cwd=root) {
  const chunks=[];
  const child=spawn(executable,args,{cwd,env:environment,stdio:['ignore','pipe','pipe']});
  child.stdout.on('data',chunk=>chunks.push(chunk));child.stderr.on('data',chunk=>chunks.push(chunk));
  const status=await new Promise((yes,no)=>{child.on('exit',yes);child.on('error',no);});
  const output=Buffer.concat(chunks).toString();await writeFile(join(root,`${name}.log`),output);
  assert.equal(status,0,output);return output;
}
await mkdir(join(root,'project/src'),{recursive:true});
await writeFile(join(root,'project/carrier.toml'),'[package]\nname = "qualification/native-oidc"\nversion = "1.0.0"\n\n[module]\nnamespace = "qualification.native_oidc"\nversion = "1.0.0"\n');
await writeFile(join(root,'project/src/main.carrier'),`
service NativeOidcQualification {}
auth jwt ProbeAuth { issuer: "https://issuer.example.test" audience: "native-probe" algorithm: "RS256" roles_claim: "roles" }
role integration { title: "Protocol fixture" }
model ProbeRow { id: UUID @primary tenant_id: UUID @index name: String version: Int @version = 1 }
policy ProbeRow { tenant_field: tenant_id read: roles [integration] write: roles [integration] }
crud ProbeRow at "/records" {
  create: protect ProbeAuth roles [integration] all_scopes ["erp:automation"]
  list: protect ProbeAuth roles [integration] all_scopes ["erp:automation"]
  get: protect ProbeAuth roles [integration] all_scopes ["erp:automation"]
  update: protect ProbeAuth roles [integration] all_scopes ["erp:automation"]
}
route GET "/scope-probe" protect ProbeAuth roles [integration] all_scopes ["erp:automation"] -> Json {
  handler { audit.record("oidc.probe", "ProbeRow", uuid.new(), {}) return { accepted: true } }
}
`);
await command(compiler,['build','--target','bicdb'],'build',join(root,'project'));
const generated=join(root,'project/.carrier/generated-bicdb');
const base=['app','--trusted-key',`native-oidc-qualification=${generated}/signing-key.pub`,join(root,'database')];
await command(binary,[...base,'install',join(generated,'application.bicdb.json')],'install');
const {privateKey,publicKey}=generateKeyPairSync('rsa',{modulusLength:2048});
await writeFile(join(root,'jwks.json'),JSON.stringify({keys:[{...publicKey.export({format:'jwk'}),kid:'fixture',alg:'RS256',use:'sig'}]}));
await writeFile(join(root,'auth.json'),JSON.stringify({schemes:[{kind:'oidc_rs256',issuer:'https://issuer.example.test',audience:'native-probe',jwks_file:'jwks.json'}]}));
if(tls) {
  await command('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',join(root,'tls.key'),'-out',join(root,'tls.crt'),'-days','1','-subj','/CN=localhost','-addext','subjectAltName=IP:127.0.0.1,DNS:localhost'],'certificate');
}
async function port() {
  const server=createServer();await new Promise(yes=>server.listen(0,'127.0.0.1',yes));
  const number=server.address().port;await new Promise(yes=>server.close(yes));return number;
}
const httpPort=await port(),pgPort=await port();
const args=[...base,'serve','--http-host',tls?'0.0.0.0':'127.0.0.1','--http-port',String(httpPort),'--pg-host','127.0.0.1','--pg-port',String(pgPort),'--pg-require-auth','--auth-config',join(root,'auth.json')];
if(tls)args.push('--http-tls-cert',join(root,'tls.crt'),'--http-tls-key',join(root,'tls.key'));
const output=[];
const start=()=>{
  const child=spawn(binary,args,{env:environment,stdio:['ignore','pipe','pipe']});
  child.stdout.on('data',chunk=>output.push(chunk));child.stderr.on('data',chunk=>output.push(chunk));
  return child;
};
let server=start();
const ca=tls?await readFile(join(root,'tls.crt')):undefined;
const encode=value=>Buffer.from(JSON.stringify(value)).toString('base64url');
const bearer=change=>{
  const now=Math.floor(Date.now()/1000);
  const claims={iss:'https://issuer.example.test',aud:'native-probe',sub:'42',roles:['integration'],tenant_id:'11111111-1111-4111-8111-111111111111',scope:'erp:automation',sid:'session-standard',org_id:'11111111-1111-4111-8111-111111111111',iat:now,exp:now+300,token_type:'access',...change};
  for(const key of Object.keys(claims))if(claims[key]===undefined)delete claims[key];
  const message=encode({alg:'RS256',kid:'fixture',typ:'JWT'})+'.'+encode(claims);
  return message+'.'+sign('RSA-SHA256',Buffer.from(message),privateKey).toString('base64url');
};
const call=(path,token,method='GET',body)=>new Promise((yes,no)=>{
  const request=(tls?httpsRequest:httpRequest)({hostname:'127.0.0.1',port:httpPort,path,method,ca,headers:{...(token?{Authorization:`Bearer ${token}`} :{}),'Content-Type':'application/json'}},response=>{
    const chunks=[];response.on('data',chunk=>chunks.push(chunk));response.on('end',()=>yes({status:response.statusCode,body:JSON.parse(Buffer.concat(chunks).toString())}));
  });
  request.on('error',no);request.setTimeout(10000,()=>request.destroy(Error('HTTP timeout')));
  request.end(body?JSON.stringify(body):undefined);
});
async function ready() {
  let ready=false;
  for(let i=0;i<100;i++) {
    if(server.exitCode!==null)throw Error(Buffer.concat(output).toString());
    try {await call('/scope-probe');ready=true;break;}catch {await new Promise(yes=>setTimeout(yes,100));}
  }
  assert.ok(ready,'native listener did not become ready');
}
async function stop(signal='SIGTERM') {
  if(server.exitCode!==null||server.signalCode!==null)return;
  const exited=new Promise(yes=>server.once('exit',yes));server.kill(signal);await exited;
}
try {
  await ready();
  async function check(name,token,status,path='/scope-probe',method='GET',body) {
    const result=await call(path,token,method,body);results.push({name,...result});
    await writeFile(join(root,'results.json'),JSON.stringify({tls,results},null,2)+'\n');
    if(status==='denied')assert.ok(result.status>=400&&result.status<500,JSON.stringify(result));
    else assert.equal(result.status,status,JSON.stringify(result));
    console.log(`${name}: ${result.status}`);return result;
  }
  await check('standard-oauth-scope',bearer({}),200);
  await check('legacy-scope-array',bearer({scope:undefined,scopes:['erp:automation'],sid:undefined,session_id:'session-legacy',org_id:undefined,organization_id:'11111111-1111-4111-8111-111111111111'}),200);
  for(const [name,change] of [['wrong-scope',{scope:'erp:other'}],['wrong-role',{roles:['viewer']}],['wrong-audience',{aud:'other-api'}],['wrong-issuer',{iss:'https://other.example.test'}],['expired',{exp:Math.floor(Date.now()/1000)-120}],['conflicting-scope-claims',{scopes:['erp:other']}],['invalid-scope-token',{scope:'erp:automation\tadmin'}]])await check(name,bearer(change),'denied');
  const created=await check('tenant-stamped-create',bearer({}),201,'/records','POST',{name:'Tenant A'});
  await check('forged-tenant-body',bearer({}),'denied','/records','POST',{name:'Forged',tenant_id:'22222222-2222-4222-8222-222222222222'});
  const other=await check('other-tenant-isolation',bearer({tenant_id:'22222222-2222-4222-8222-222222222222',org_id:'22222222-2222-4222-8222-222222222222'}),200,'/records');
  assert.equal(other.body.items.length,0);
  const path=`/records/${created.body.id}`;
  const races=await Promise.all(Array.from({length:8},(_,index)=>call(path,bearer({}),'PATCH',{name:`Writer ${index}`,version:created.body.version})));
  assert.equal(races.filter(result=>result.status===200).length,1,JSON.stringify(races));
  assert.equal(races.filter(result=>result.status===409).length,7,JSON.stringify(races));
  results.push({name:'eight-concurrent-editors',statuses:races.map(result=>result.status)});
  const saved=await check('read-concurrent-winner',bearer({}),200,path);
  assert.equal(saved.body.version,2);
  await stop('SIGKILL');server=start();await ready();
  const restarted=await check('acknowledged-write-survives-crash',bearer({}),200,path);
  assert.equal(restarted.body.name,saved.body.name);assert.equal(restarted.body.version,2);
  await stop();
  await cp(join(root,'database'),join(root,'restored-database'),{recursive:true});
  await rename(join(root,'database'),join(root,'original-offline'));
  args[args.indexOf(join(root,'database'))]=join(root,'restored-database');
  server=start();await ready();
  const restored=await check('cold-backup-restores-data-and-signed-catalog',bearer({}),200,path);
  assert.equal(restored.body.name,saved.body.name);assert.equal(restored.body.version,2);
} finally {
  await stop();
  await writeFile(join(root,'server.log'),Buffer.concat(output));
  console.log(`Evidence: ${root}`);
}
