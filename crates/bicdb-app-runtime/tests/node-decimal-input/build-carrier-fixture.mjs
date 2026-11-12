// Rebuild the retained fixture with the exact official compiler. Generated
// package and public-key bytes are copied verbatim, never postprocessed.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {spawn} from 'node:child_process';
const root=path.dirname(import.meta.filename),binary=process.argv[2],evidence=process.argv[3];
assert.ok(binary&&evidence,'compiler artifact and build evidence directory required');
const previous=JSON.parse(await fs.readFile(root+'/fixture.json')),sha=b=>createHash('sha256').update(b).digest('hex');
assert.equal(sha(await fs.readFile(binary)),previous.official_compiler.sha256);
await fs.mkdir(evidence,{recursive:true});
const env={...process.env,CARRIER_BICDB_SIGNING_KEY:'19'.repeat(32),CARRIER_BICDB_SIGNING_KEY_ID:'qa-decimal-input'},steps=[];
for(const [name,args]of [['fmt',['fmt']],['check',['check','--target','bicdb']],['build',['build','--target','bicdb']]]){
 const output=[],p=spawn(binary,args,{cwd:root+'/source',env});p.stdout.on('data',b=>output.push(b));p.stderr.on('data',b=>output.push(b));
 const code=await new Promise((r,j)=>{p.once('exit',r);p.once('error',j)}),bytes=Buffer.concat(output);
 await fs.writeFile(evidence+'/'+name+'.log',bytes);steps.push({name,code,log_sha256:sha(bytes)});
 assert.equal(code,0,bytes.toString().slice(-3000));
}
const bytes=await fs.readFile(root+'/source/.carrier/generated-bicdb/application.bicdb.json'),digest=sha(bytes),packageName='fixtures/'+digest+'.bicdb.json';
await fs.copyFile(root+'/source/.carrier/generated-bicdb/application.bicdb.json',root+'/'+packageName);
const key=await fs.readFile(root+'/source/.carrier/generated-bicdb/signing-key.pub');assert.equal(sha(key),previous.public_key_sha256,'Public test key changed');
const files=[];for(const p of ['source/carrier.toml','source/src/main.carrier'])files.push({path:p,sha256:sha(await fs.readFile(root+'/'+p))});
const next={...previous,package:packageName,package_sha256:digest,source_files:files};
await fs.writeFile(root+'/fixture.json',JSON.stringify(next,null,2)+'\n');
const result={passed:true,official_compiler:previous.official_compiler,source_files:files,package_sha256:digest,steps,generated_output_edited:false};
await fs.writeFile(evidence+'/qualification.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
