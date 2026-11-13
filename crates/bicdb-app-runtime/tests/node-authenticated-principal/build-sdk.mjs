// Public ABI-v2 SDK source, independent of Carrier-generated artifacts.
// The official native CLI normalizes and signs the valid fixture. Negative
// vectors are separately SDK-authored, cryptographically signed variants.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash,createPrivateKey,createPublicKey,sign} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {signatureVectors} from './vectors.mjs';
import {transportModule} from './transport.mjs';
const binary=process.argv[2],out=process.argv[3];assert(binary&&out,'Native binary and isolated output directory required');
await fs.mkdir(out,{recursive:true,mode:0o700});
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const seed=Buffer.alloc(32,0x29),key=createPrivateKey({format:'der',type:'pkcs8',key:Buffer.concat([Buffer.from('302e020100300506032b657004220420','hex'),seed])});
await fs.writeFile(path.join(out,'sdk-seed.hex'),seed.toString('hex'),{mode:0o600});await fs.writeFile(path.join(out,'sdk-key.pub'),createPublicKey(key).export({format:'der',type:'spki'}).subarray(-32).toString('hex'));
const call=target=>({op:'call',kind:'builtin',target,method:null,result_type:'other',argument_types:[],argument_item_types:[],result_projection:null,arguments:[]});
const callable=target=>({parameters:[],body:[{op:'return',value:call(target)}]});
const principalCall=callable('auth.current_principal'),legacyCall=callable('auth.current_user');
const route=(name,template,exportName,{publicRoute=false,dual=false}={})=>({name,method:'GET',template,export:exportName,resource:null,operation:null,public:publicRoute,...(publicRoute?{}:{auth_scheme:'ProbeAuth'}),roles:dual?['custodian','issuer_operator']:[],scopes:publicRoute?[]:['probe:principal'],roles_any:false,scopes_any:false,max_request_bytes:16384,max_response_bytes:1048576,streaming_request:false,streaming_response:false,sse:false,websocket:false});
function unsigned(name,legacy=false){
  const dependency_lock=Buffer.from('{"format":1,"packages":[]}'),sbom=Buffer.from('{"bomFormat":"CycloneDX","specVersion":"1.5"}'),provenance=Buffer.from(JSON.stringify({builder:'source-authored-authenticated-principal-sdk-v1',source:'build-sdk.mjs',generatedProductInput:false}));
  const application={abi_version:2,application_profile:'bicdb-application-v2',package:{application:name,version:'999.1.0',package_sha256:'0'.repeat(64),dependency_lock_sha256:hash(dependency_lock),sbom_sha256:hash(sbom),provenance_sha256:hash(provenance),signature_key_id:'qa-principal',signature_algorithm:'ed25519',signature:'unsigned-placeholder'},relation_permissions:[],raw_sql:[],service_imports:[],service_exports:[],secrets:[],egress:[],blobs:[],routes:legacy?[route('legacy','/sdk-legacy/current','legacy')]:[route('who','/sdk-principal/current','who'),route('dual','/sdk-principal/dual','who',{dual:true}),route('public','/sdk-principal/public','who',{publicRoute:true}),route('legacy','/sdk-principal/legacy','legacy')],auth_schemes:{ProbeAuth:{kind:'oidc_rs256',issuer:'https://issuer.invalid/principal',audience:'principal-native-probe'}},resources:[],invariants:[],migrations:[],workers:[],schedules:[],application_program:{version:1,max_steps:10000,max_call_depth:16,callables:legacy?{legacy:legacyCall}:{legacy:legacyCall,who:principalCall},service_bindings:{},client_bindings:{},flags:{},job_bindings:{},secret_bindings:{},event_bindings:{},mutation_bindings:[],workflow_bindings:{}},required_features:[...(legacy?[]:['authenticated_principal']),'trusted_connection_identity','exact_column_authority','oidc_rs256','http'],max_call_depth:16};
  const manifest={identity:{name,version:'999.1.0',abi_version:2,description:'Source-authored public ABI authenticated principal probe'},dependencies:[],capabilities:['http_routes'],permissions:{read_relations:[],write_relations:[],publish_queues:[],consume_queues:[],network_hosts:[]},limits:{memory_bytes:67108864,fuel:10000000,timeout_ms:10000,max_input_bytes:1048576,max_output_bytes:4194304,max_concurrency:8},functions:[],indexes:[],storage:[],routes:[],subscriptions:[],observability:[],application};
  return {manifest,modules:{[name]:transportModule(manifest)},dependency_lock:[...dependency_lock],sbom:[...sbom],provenance:[...provenance]};
}
function cli(args){const result=spawnSync(binary,args,{encoding:'utf8',maxBuffer:8*1024*1024});assert.equal(result.status,0,result.stdout+result.stderr);return result.stdout;}
async function official(name,document){const input=path.join(out,name+'.unsigned.json'),output=path.join(out,name+'.bicdb.json');await fs.writeFile(input,JSON.stringify(document));const log=cli(['app-package','sign',input,'--key-id','qa-principal','--signing-key',path.join(out,'sdk-seed.hex'),'--out',output]);await fs.writeFile(path.join(out,name+'.sign.log'),log);return JSON.parse(await fs.readFile(output));}
const positive=await official('principal',unsigned('qualification.principal_sdk'));
await official('legacy',unsigned('qualification.principal_legacy',true));
function resign(document){const metadata=document.manifest.application.package;metadata.package_sha256='';metadata.signature='';const payload={format:'bicdb-application-package-signature-v1',manifest:document.manifest,module_sha256:Object.fromEntries(Object.entries(document.modules).map(([name,bytes])=>[name,hash(Buffer.from(bytes))])),...(Object.keys(document.frontend_assets??{}).length?{frontend_assets:document.frontend_assets}:{}),...(document.components?.length?{components:document.components}:{}),...(Object.keys(document.scripts??{}).length?{script_sha256:Object.fromEntries(Object.entries(document.scripts).map(([name,text])=>[name,hash(text)]))}:{}),dependency_lock_sha256:hash(Buffer.from(document.dependency_lock)),sbom_sha256:hash(Buffer.from(document.sbom)),provenance_sha256:hash(Buffer.from(document.provenance))};const digest=hash(JSON.stringify(payload));metadata.package_sha256=digest;metadata.signature=sign(null,Buffer.from(digest),key).toString('base64');return digest;}
const roundTrip=structuredClone(positive);assert.equal(resign(roundTrip),positive.manifest.application.package.package_sha256,'Independent SDK signing payload must equal official native canonical payload');assert.equal(roundTrip.manifest.application.package.signature,positive.manifest.application.package.signature);
const vectors=[];
for(const vector of [{name:'missing_feature'},...signatureVectors]){
  const document=structuredClone(positive),application=document.manifest.application;
  document.manifest.identity.name='qualification.principal_'+vector.name;application.package.application=document.manifest.identity.name;
  if(vector.name==='missing_feature')application.required_features=application.required_features.filter(name=>name!=='authenticated_principal');
  else Object.assign(application.application_program.callables.who.body[0].value,vector.patch);
  document.modules={[document.manifest.identity.name]:transportModule(document.manifest)};resign(document);const file=path.join(out,vector.name+'.bicdb.json');await fs.writeFile(file,JSON.stringify(document));vectors.push({name:vector.name,path:file,sha256:hash(await fs.readFile(file)),expected:/missing_feature/.test(vector.name)?'requires signed authenticated_principal':'requires exactly zero arguments'});
}
// Read-only service crossing uses the same verified actor and one propagated
// native transaction. It creates no schema or finance data.
const ownerName='qualification.principal_owner',callerName='qualification.principal_caller';
const response=[['subject','string',false],['roles','json',false],['scopes','json',false],['tenant_id','string',true],['workspace_id','string',true]].map(([name,field_type,nullable])=>({name,field_type,nullable}));
const methods=[{name:'who',request:[],response,errors:[],allows_undeclared_errors:false,retryable_errors:[]}];
const serviceHash=hash(JSON.stringify(methods));
const owner=unsigned(ownerName),oa=owner.manifest.application;
owner.manifest.capabilities=['http_routes','plugin_services','transactions'];
oa.routes=[];oa.required_features.push('services','read_committed');
oa.service_exports=[{service:'principal_identity',version:'1.0.0',contract_sha256:serviceHash,export:'who',methods,allow_reentrant:false,delegated_authority:false}];
owner.modules={[ownerName]:transportModule(owner.manifest)};
const ownerSigned=await official('owner',owner);
const caller=unsigned(callerName),ca=caller.manifest.application;
caller.manifest.capabilities=['http_routes','plugin_services','transactions'];
caller.manifest.dependencies=[{name:ownerName,version:'=999.1.0',abi_version:2,optional:false,capabilities:[],module_sha256:null}];
const locked=Buffer.from(JSON.stringify({format:1,packages:[{name:ownerName,version:'999.1.0',package_sha256:ownerSigned.manifest.application.package.package_sha256}]}));
caller.dependency_lock=[...locked];ca.package.dependency_lock_sha256=hash(locked);
ca.required_features.push('services','read_committed','savepoints');
ca.service_imports=[{name:ownerName,service:'principal_identity',version:'1.0.0',contract_sha256:serviceHash,optional:false,propagate_transaction:true,allow_reentrant:false,delegated_authority:false}];
ca.routes=[route('cross_owner','/sdk-principal/cross-owner','cross_owner')];
ca.application_program.service_bindings.remote={dependency:ownerName,service:'principal_identity',method:'who',parameters:[],propagate_transaction:true};
ca.application_program.callables.cross_owner={parameters:[],body:[{op:'transaction',isolation:'read_committed',body:[{op:'let',name:'caller',value:call('auth.current_principal')},{op:'let',name:'owner',value:{...call('remote'),kind:'action'}},{op:'return',value:{op:'object',fields:{caller:{op:'variable',name:'caller'},owner:{op:'variable',name:'owner'}}}}]}]};
caller.modules={[callerName]:transportModule(caller.manifest)};await official('caller',caller);
await fs.writeFile(path.join(out,'vectors.json'),JSON.stringify({sourceAuthoredSDK:true,generatedCarrierOutputReadOrPatched:false,canonicalSigningRoundTrip:true,positive:path.join(out,'principal.bicdb.json'),legacy:path.join(out,'legacy.bicdb.json'),owner:path.join(out,'owner.bicdb.json'),caller:path.join(out,'caller.bicdb.json'),publicKey:path.join(out,'sdk-key.pub'),negative:vectors},null,2)+'\n');
console.log('SDK principal/legacy fixtures signed; '+vectors.length+' independently signed strict-ABI negative vectors prepared.');
