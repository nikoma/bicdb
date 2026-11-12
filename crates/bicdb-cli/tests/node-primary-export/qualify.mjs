// Actual CLI recovery export; no Rust test/helper compilation.
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {spawn} from 'node:child_process';
const config=JSON.parse(await fs.readFile(process.argv[2],'utf8'));
assert.equal(typeof config.expectExport,'boolean');
assert.match(config.outputRoot,/\/private\//);
const hash=b=>createHash('sha256').update(b).digest('hex');
assert.equal(hash(await fs.readFile(config.binary)),config.binarySha256);
assert.equal(hash(await fs.readFile(config.oldReader)),config.oldReaderSha256);
const run=config.outputRoot+'/primary-export-'+Date.now();await fs.mkdir(run+'/evidence',{recursive:true});
await fs.copyFile(import.meta.filename,run+'/evidence/driver.mjs');
await fs.copyFile(process.argv[2],run+'/evidence/config.json');
const records=[];
async function command(binary,args,label){
 const out=[],err=[],p=spawn(binary,args,{stdio:['ignore','pipe','pipe']});
 p.stdout.on('data',b=>out.push(b));p.stderr.on('data',b=>err.push(b));
 const code=await new Promise((r,j)=>{p.once('exit',r);p.once('error',j)});
 const value={binary,args,code,stdout:Buffer.concat(out).toString(),stderr:Buffer.concat(err).toString()};
 await fs.writeFile(run+'/evidence/'+label+'.json',JSON.stringify(value,null,2)+'\n');
 records.push({label,code});return value;
}
const rows=text=>text.trim().split('\n').filter(Boolean).map(JSON.parse).sort((a,b)=>a.id.localeCompare(b.id));
async function tree(dir){const files=[];async function visit(at,rel=''){for(const e of(await fs.readdir(at,{withFileTypes:true})).sort((a,b)=>a.name.localeCompare(b.name))){const p=rel+e.name;if(e.isDirectory())await visit(at+'/'+e.name,p+'/');else{assert.equal(e.isFile(),true);files.push({path:p,sha256:hash(await fs.readFile(at+'/'+e.name))});}}}await visit(dir);return files;}
const fixtures=['decimal-predicate-legacy-main482','decimal-predicate-legacy-main482-embedded','decimal-predicate-legacy-main482-multiple','decimal-predicate-legacy-main482-duplicate'];
const cases=[];
for(const name of fixtures){
 const original=config.fixtureRoot+'/'+name,before=await tree(original),database=run+'/'+name;
 await fs.cp(original,database,{recursive:true,errorOnExist:true,force:false});
 const old=await command(config.oldReader,['export',database,'--collection','precision_probe'],name+'-acknowledged-old-reader-rows');assert.equal(old.code,0,old.stderr);
 const acknowledged=rows(old.stdout);assert.ok(acknowledged.length>0);
 const indexes=Buffer.from('not valid json'),sync=Buffer.from('not a valid sync log');
 await fs.writeFile(database+'/indexes.json',indexes);await fs.writeFile(database+'/sync.log',sync);
 const normal=await command(config.binary,['verify',database,'--json'],name+'-ordinary-open-refuses-corrupt-catalog');assert.notEqual(normal.code,0);assert.match(normal.stderr,/json|index|catalog|format/i);
 const exported=await command(config.binary,['export',database,'--collection','precision_probe','--batch-rows','1'],name+'-bounded-primary-export');
 if(config.expectExport){assert.equal(exported.code,0,exported.stderr);assert.deepEqual(rows(exported.stdout),acknowledged);}else assert.notEqual(exported.code,0,'Negative control unexpectedly exports');
 assert.deepEqual(await fs.readFile(database+'/indexes.json'),indexes);assert.deepEqual(await fs.readFile(database+'/sync.log'),sync);
 const cold=database+'-cold';await fs.cp(database,cold,{recursive:true,errorOnExist:true,force:false});
 const reopened=await command(config.binary,['export',cold,'--collection','precision_probe','--batch-rows','1'],name+'-cold-primary-export');
 if(config.expectExport){assert.equal(reopened.code,0,reopened.stderr);assert.deepEqual(rows(reopened.stdout),acknowledged);}else assert.notEqual(reopened.code,0);
 assert.deepEqual(await fs.readFile(cold+'/indexes.json'),indexes);assert.deepEqual(await fs.readFile(cold+'/sync.log'),sync);
 assert.deepEqual(await tree(original),before,'Original closed fixture changed');
 cases.push({name,acknowledged_rows:acknowledged.length,export_succeeded:exported.code===0,ordinary_corrupt_catalog_refused:true,index_and_sync_bytes_unchanged:true,cold_copy:true,original_fixture_unchanged:true});
}
const result={passed:true,expectExport:config.expectExport,actualRecoveryExport:cases.every(x=>x.export_succeeded),source:config.source,binary:config.binary,binary_sha256:config.binarySha256,node_only:true,rust_tests:false,local_operator_CLI:true,http_or_Hub:false,scope:'Bounded primary record recovery despite unrelated corrupt secondary/CDC metadata; ordinary indexed admission still refuses corruption',cases,records};
await fs.writeFile(run+'/evidence/qualification.json',JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({run,...result}));
