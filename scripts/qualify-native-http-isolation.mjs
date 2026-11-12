// Isolated protocol regression with a fixture issuer. Real customer/Hub
// acceptance must use the actual issuer separately.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {generateKeyPairSync,randomBytes,randomUUID,sign} from 'node:crypto';
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
await mkdir(join(root,'provider/src'),{recursive:true});
await writeFile(join(root,'provider/carrier.toml'),'[package]\nname="qualification/provider"\nversion="1.0.0"\n[module]\nnamespace="qualification.provider"\nversion="1.0.0"\n');
await writeFile(join(root,'provider/src/main.carrier'),'service IsolationProvider {}\naction stronger() -> Json { transaction(isolation:serializable) { return {accepted:true} } }\n');

async function command(executable,args,name,cwd=root,expected=0) {
  const chunks=[];
  const child=spawn(executable,args,{cwd,env:environment,stdio:['ignore','pipe','pipe']});
  child.stdout.on('data',chunk=>chunks.push(chunk));child.stderr.on('data',chunk=>chunks.push(chunk));
  const status=await new Promise((yes,no)=>{child.on('exit',yes);child.on('error',no);});
  const output=Buffer.concat(chunks).toString();await writeFile(join(root,`${name}.log`),output);
  assert.equal(status,expected,output);return output;
}
await command(compiler,['build','--target','bicdb'],'provider-build',join(root,'provider'));
await mkdir(join(root,'project/src'),{recursive:true});
await cp(join(root,'provider/.carrier/generated-bicdb/federation-manifest.json'),join(root,'project/src/provider.json'));

await writeFile(join(root,'project/carrier.toml'),'[package]\nname = "qualification/native-oidc"\nversion = "1.0.0"\n\n[module]\nnamespace = "qualification.native_oidc"\nversion = "1.0.0"\n');
await writeFile(join(root,'project/src/main.carrier'),"\nservice NativeIdempotentIsolationQualification {}\nfederated service IsolationProvider { url: \"bicdb://qualification.provider\" trust: mtls(cert:\"bicdb-cell-host\",key:\"bicdb-cell-host\") import_manifest:\"./provider.json\" expose_remote action qualification.provider.stronger as remote_stronger }\nauth jwt ProbeAuth { issuer:\"https://issuer.example.test\" audience:\"native-probe\" algorithm:\"RS256\" roles_claim:\"roles\" }\nrole integration {title:\"Transaction fixture\"}\nmodel ProbeRow { id: UUID @primary tenant_id: UUID @index name: String }\npolicy ProbeRow { tenant_field: tenant_id read: roles [integration] write: roles [integration] }\ncrud ProbeRow at \"/records\" { list: protect ProbeAuth roles [integration] get: protect ProbeAuth roles [integration] }\ntype ProbeWrite { id: UUID name: String }\naction write_rc(input: ProbeWrite) -> Json { transaction { let rows = ProbeRow.bulk_insert([{ id: input.id name: input.name }]) return {id: input.id name: input.name} } }\naction write_rr(input: ProbeWrite) -> Json { transaction(isolation:repeatable_read) { let rows = ProbeRow.bulk_insert([{ id: input.id name: input.name }]) return {id: input.id name: input.name} } }\naction write_serializable(input: ProbeWrite) -> Json { transaction(isolation:serializable) { let rows = ProbeRow.bulk_insert([{ id: input.id name: input.name }]) return {id: input.id name: input.name} } }\naction wrapped_serializable(input: ProbeWrite) -> Json { if input.name == \"wrapped\" { return write_serializable(input) } else { return write_rr(input) } }\naction invalid_nested(input: ProbeWrite) -> Json { transaction { let rows = ProbeRow.bulk_insert([{id:uuid.new() name:\"must roll back\"}]) return remote_stronger() } }\nroute GET \"/scope-probe\" protect ProbeAuth roles [integration] all_scopes [\"erp:automation\"] -> Json {handler {return {accepted:true}}}\nroute POST \"/idempotent/rc\" protect ProbeAuth roles [integration] all_scopes [\"erp:automation\"] idempotent -> Json { input: ProbeWrite handler {return write_rc(input)} }\nroute POST \"/idempotent/rr\" protect ProbeAuth roles [integration] all_scopes [\"erp:automation\"] idempotent -> Json { input: ProbeWrite handler {return write_rr(input)} }\nroute POST \"/idempotent/serializable\" protect ProbeAuth roles [integration] all_scopes [\"erp:automation\"] idempotent -> Json { input: ProbeWrite handler {return write_serializable(input)} }\nroute POST \"/idempotent/wrapped\" protect ProbeAuth roles [integration] all_scopes [\"erp:automation\"] idempotent -> Json { input: ProbeWrite handler {return wrapped_serializable(input)} }\nroute POST \"/idempotent/invalid\" protect ProbeAuth roles [integration] all_scopes [\"erp:automation\"] idempotent -> Json { input: ProbeWrite handler {return invalid_nested(input)} }\n");
await command(compiler,['build','--target','bicdb'],'build',join(root,'project'));
const generated=join(root,'project/.carrier/generated-bicdb');
const pkg=JSON.parse(await readFile(join(generated,'application.bicdb.json')));
assert.ok(pkg.manifest.application.resources.length>=1);
const oldCompiler=process.env.EXPECT_TRANSACTION_CAPABILITY_REJECTION==='1';
const base=['app','--trusted-key',`native-oidc-qualification=${generated}/signing-key.pub`,join(root,'database')];
await command(binary,[...base,'install',join(root,'provider/.carrier/generated-bicdb/application.bicdb.json')],'provider-install');
const installed=await command(binary,[...base,'install',join(generated,'application.bicdb.json')],'install',root,oldCompiler?1:0);
if(oldCompiler){assert.match(installed,/transactions require the signed transactions capability/);console.log('Old transaction-only package rejection reproduced: '+root);process.exit(0);}
assert.ok(pkg.manifest.capabilities.includes('transactions'));assert.ok(pkg.manifest.capabilities.includes('database'));
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
const call=(path,token,method='GET',body,idem)=>new Promise((yes,no)=>{
  const request=(tls?httpsRequest:httpRequest)({hostname:'127.0.0.1',port:httpPort,path,method,ca,headers:{...(token?{Authorization:`Bearer ${token}`} :{}),'Content-Type':'application/json',...(idem?{'Idempotency-Key':idem}:{})}},response=>{
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
 const check=async(name,path,method='GET',body,key,expected=200,claims={})=>{const r=await call(path,bearer(claims),method,body,key);results.push({name,...r});await writeFile(join(root,'results.json'),JSON.stringify({synthetic_issuer:true,tls,known_broken:process.env.EXPECT_HTTP_ISOLATION_REJECTION==='1',results},null,2)+'\n');assert.equal(r.status,expected,JSON.stringify(r));console.log(name+': '+r.status);return r;};
 const broken=process.env.EXPECT_HTTP_ISOLATION_REJECTION==='1';
 const accepted=[];let lastAccepted;
 for(const level of ['rc','rr','serializable','wrapped']) {
   const body={id:randomUUID(),name:level==='wrapped'?'wrapped':level},key=randomUUID(),status=broken&&level!=='rc'?500:200;
   const first=await check('idempotent-'+level,'/idempotent/'+level,'POST',body,key,status);
   if(status===500){assert.match(first.body.message,/cannot strengthen/);continue;}
   accepted.push(body.id);lastAccepted={path:'/idempotent/'+level,body,key,first};const replay=await check('replay-'+level,'/idempotent/'+level,'POST',body,key);assert.deepEqual(replay,first);
   await check('changed-request-'+level,'/idempotent/'+level,'POST',{...body,name:'changed'},key,409);
 }
 const invalid=await check('invalid-nested-still-rejected','/idempotent/invalid','POST',{id:randomUUID(),name:'invalid'},randomUUID(),500);assert.match(invalid.body.message,/cannot strengthen/);
 await check('idempotency-key-still-required','/idempotent/serializable','POST',{id:randomUUID(),name:'missing-key'},undefined,400);
 await check('strong-isolation-does-not-grant-role','/idempotent/serializable','POST',{id:randomUUID(),name:'denied'},randomUUID(),403,{roles:['viewer']});
 const snapshot=await check('no-partial-writes','/records');assert.equal(snapshot.body.items.length,accepted.length);assert.deepEqual(snapshot.body.items.map(x=>x.id).sort(),accepted.sort());
 await stop('SIGKILL');server=start();await ready();assert.deepEqual((await check('crash-retains-exact-records','/records')).body,snapshot.body);assert.deepEqual(await check('crash-retains-idempotent-receipt',lastAccepted.path,'POST',lastAccepted.body,lastAccepted.key),lastAccepted.first);
 await stop();await cp(join(root,'database'),join(root,'restored-database'),{recursive:true});await rename(join(root,'database'),join(root,'original-offline'));args[args.indexOf(join(root,'database'))]=join(root,'restored-database');server=start();await ready();assert.deepEqual((await check('coldrestore-retains-exact-records','/records')).body,snapshot.body);assert.deepEqual(await check('coldrestore-retains-idempotent-receipt',lastAccepted.path,'POST',lastAccepted.body,lastAccepted.key),lastAccepted.first);
 console.log(broken?'Old HTTP wrapper isolation failure reproduced':'Native HTTP idempotency isolation, replay, nested refusal and recovery passed');
} finally {
  await stop();
  await writeFile(join(root,'server.log'),Buffer.concat(output));
  console.log(`Evidence: ${root}`);
}
