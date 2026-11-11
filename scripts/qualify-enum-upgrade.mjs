// Node drives signed native packages; no Rust test compilation.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdtemp,mkdir,readFile,writeFile} from 'node:fs/promises';
import {randomBytes,createHash} from 'node:crypto';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
const binary=resolve(process.env.BICDB_BINARY??'target/release/bicdb');
const compiler=process.env.CARRIER_BINARY??'carrier';
const root=await mkdtemp(join(tmpdir(),'bicdb-enum-upgrade-'));
const environment={...process.env,CARRIER_BICDB_SIGNING_KEY:randomBytes(32).toString('hex'),CARRIER_BICDB_SIGNING_KEY_ID:'enum-upgrade'};
const results=[];
async function run(exe,args,label,cwd=root,expected=0){const chunks=[];const p=spawn(exe,args,{cwd,env:environment});p.stdout.on('data',x=>chunks.push(x));p.stderr.on('data',x=>chunks.push(x));const code=await new Promise((r,j)=>{p.on('exit',r);p.on('error',j);});const text=Buffer.concat(chunks).toString();await writeFile(join(root,label+'.log'),text);results.push({label,code,expected});await writeFile(join(root,'results.json'),JSON.stringify({binary,results},null,2));assert.equal(code,expected,text);console.log(label+': '+code);return text;}
const packages={};
for(const [name,version,values,scalar] of [['v1','1.0.0','alpha'],['v2','1.0.1','alpha beta'],['remove','1.0.2','alpha'],['substitute','1.0.2','alpha beta',true]]){
 const dir=join(root,name);await mkdir(join(dir,'src'),{recursive:true});
 await writeFile(join(dir,'carrier.toml'),`[package]\nname="qualification/enum-upgrade"\nversion="${version}"\n[module]\nnamespace="qualification.enum-upgrade"\nversion="${version}"\n[runtime]\nsource="git"\ngit="https://github.com/nikoma/carrier.git"\nrev="c396d6626a897ace687946c644351ee3ad0e2550"\n`);
 const type=scalar?'String':'Purpose';
 const source=`service EnumUpgradeQualification {}
auth jwt FixtureAuth { issuer: "https://auth.parau.org" audience: "walknorth-ehr-api" algorithm: "RS256" roles_claim: "roles" }
role editor { title: "Editor" }
enum Purpose { ${values} }
model Setting { id: UUID @primary tenant_id: UUID @index purpose: ${type} optional_purpose: ${type}? }
policy Setting { tenant_field: tenant_id read: roles [editor] write: roles [editor] }
crud Setting at "/settings" { create: protect FixtureAuth roles [editor] get: protect FixtureAuth roles [editor] list: protect FixtureAuth roles [editor] }
test "seed old values" {
 auth { id: 9611 email: "enum@example.com" name: "Editor" roles: ["editor"] tenant_id: "96119611-9611-4611-8611-961196119611" }
 given {} when { let a = http.request(method: "POST", path: "/settings", body: { purpose: "alpha" optional_purpose: "alpha" }) let b = http.request(method: "POST", path: "/settings", body: { purpose: "alpha" }) }
 then a.status == 201 && b.status == 201
}
test "old values remain readable" {
 auth { id: 9611 email: "enum@example.com" name: "Editor" roles: ["editor"] tenant_id: "96119611-9611-4611-8611-961196119611" }
 given {} when { let rows = Setting.Where(s => s.purpose == ${scalar?'"alpha"':'alpha'}).ToList() }
 then array.length(rows) == 2
}
test "new values and invalid values are governed" {
 auth { id: 9611 email: "enum@example.com" name: "Editor" roles: ["editor"] tenant_id: "96119611-9611-4611-8611-961196119611" }
 given {} when { let good = http.request(method: "POST", path: "/settings", body: { purpose: "beta" optional_purpose: "beta" }) let bad = http.request(method: "POST", path: "/settings", body: { purpose: "gamma" optional_purpose: "beta" }) }
 then good.status == 201 && bad.status == 400
}
`;
 await writeFile(join(dir,'src/main.carrier'),source);await run(compiler,['build','--target','bicdb'],'build-'+name,dir);packages[name]=join(dir,'.carrier/generated-bicdb/application.bicdb.json');
}
const key=join(root,'v1/.carrier/generated-bicdb/signing-key.pub');
const app=(args,label,expected=0)=>run(binary,['app','--trusted-key',`enum-upgrade=${key}`,join(root,'database'),...args],label,root,expected);
const scenario=(name,label)=>app(['test','qualification.enum-upgrade',name,'--case-index','0'],label);
await app(['install',packages.v1],'install-v1');await scenario('test_0_seed_old_values','seed-v1');
const failure=process.env.EXPECT_ADDITIVE_ENUM_REJECTION==='1';
const upgraded=await app(['upgrade',packages.v2],'expand-enums',failure?1:0);
if(failure){assert.match(upgraded,/incompatible field/);console.log('Old-host additive enum rejection reproduced: '+root);process.exit(0);}
await scenario('test_1_old_values_remain_readable','existing-values-after-upgrade');
await app(['rollback','qualification.enum-upgrade'],'pre-traffic-rollback');await scenario('test_1_old_values_remain_readable','existing-values-after-rollback');
await app(['upgrade',packages.v2],'upgrade-after-rollback');await scenario('test_2_new_values_and_invalid_values_are_governed','new-values-and-validation');
for(const name of ['remove','substitute']){const refused=await app(['upgrade',packages[name]],'refuse-'+name,1);assert.match(refused,/incompatible field/);await scenario('test_1_old_values_remain_readable',name+'-preserves-active');}
await writeFile(join(root,'qualification.json'),JSON.stringify({passed:true,binary_sha256:createHash('sha256').update(await readFile(binary)).digest('hex'),results},null,2)+'\n');console.log('Evidence: '+root);
