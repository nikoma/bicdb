import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const bindings=fs.readFileSync(new URL('../../crates/bicdb-script/src/bindings.js',import.meta.url),'utf8');
function execute(expression,errors={}){
 const calls=[];
 const context={__event:'{}',__host(method,args){
  calls.push({method,args:JSON.parse(args)});
  return JSON.stringify(errors[method]?{error:errors[method]}:{value:null});
 }};
 try{return{value:vm.runInNewContext(bindings+'\n'+expression,context),calls};}
 catch(error){return{error,calls};}
}
test('actual binding preserves callback failure when terminal cleanup also fails',()=>{
 const result=execute('db.transaction(()=>{throw new Error("original late failure");})',{'db.rollback':'expired transaction'});
 assert.equal(result.error.message,'original late failure');
 assert.deepEqual(result.calls.map(x=>x.method),['db.begin','db.rollback']);
});
test('actual binding preserves failed native commit instead of expired cleanup',()=>{
 const result=execute('db.transaction(()=>42)',{'db.commit':'constraint: WithholdingPolicyActualScope','db.rollback':'expired transaction'});
 assert.equal(result.error.message,'constraint: WithholdingPolicyActualScope');
 assert.deepEqual(result.calls.map(x=>x.method),['db.begin','db.commit','db.rollback']);
});
test('successful transaction commits once without terminal cleanup',()=>{
 const result=execute('db.transaction(()=>42)');
 assert.equal(result.error,undefined);assert.equal(result.value,42);
 assert.deepEqual(result.calls.map(x=>x.method),['db.begin','db.commit']);
});
test('thenable callback remains refused and rolled back',()=>{
 const result=execute('db.transaction(()=>({then(){}}))');
 assert.equal(result.error.message,'Transaction callbacks must be synchronous');
 assert.deepEqual(result.calls.map(x=>x.method),['db.begin','db.rollback']);
});
