import assert from'node:assert/strict';import{randomUUID}from'node:crypto';
export async function qualify({call,record,tenant,other}){
 const failures=[],denied=['commit','rollback','commit_chain','rollback_chain','dynamic_commit','dynamic_rollback','dynamic_begin','rethrow','notice'],unsupported=['prepare','commit_prepared','rollback_prepared'],retained=[];
 const expect=(condition,label,detail)=>{if(!condition)failures.push({label,detail});};
 const observe=async(mode,id)=>{const r=await call('/child-guards/'+mode+'/observe',{prefix:id});await record('actual retained state '+mode+' '+id,r);assert.equal(r.status,200);return r.body.rows;};
 for(const mode of ['off','on']){
  for(const statement of denied){const id=mode+'-'+statement+'-'+randomUUID(),r=await call('/child-guards/'+mode+'/run',{id,statement,catch_error:false,late_fail:false});await record('genuine host-owned denial '+mode+' '+statement,r);expect(r.status===403,mode+' '+statement+' typed403',r);expect(/host-owned application transaction/.test(JSON.stringify(r.body)),mode+' '+statement+' origin',r);expect((await observe(mode,id)).length===0,mode+' '+statement+' atomic rollback',r);}
  for(const statement of unsupported){const id=mode+'-'+statement+'-'+randomUUID(),r=await call('/child-guards/'+mode+'/run',{id,statement,catch_error:false,late_fail:false});await record('prepared transaction target restriction '+mode+' '+statement,r);expect(r.status!==200,mode+' '+statement+' refused',r);expect(/2D000|42601|0A000|PREPARED|PREPARE TRANSACTION|host-owned|parse|syntax/i.test(JSON.stringify(r.body)),mode+' '+statement+' relevant refusal',r);expect((await observe(mode,id)).length===0,mode+' '+statement+' no side effects',r);}
  for(const statement of ['commit','dynamic_commit']){const id=mode+'-guestcaught-'+statement+'-'+randomUUID(),r=await call('/child-guards/'+mode+'/run',{id,statement,catch_error:true,late_fail:true});await record('guest catches denial then late-fails '+mode+' '+statement,r);expect(r.status!==200,mode+' guest late error',r);expect((await observe(mode,id)).length===0,mode+' guest caught denial cannot early-commit',r);}
  for(const statement of ['caught_commit','caught_dynamic','savepoint'])for(const late_fail of [true,false]){
   const id=mode+'-'+statement+'-'+late_fail+'-'+randomUUID(),r=await call('/child-guards/'+mode+'/run',{id,statement,catch_error:false,late_fail});await record('caller retains final decision '+mode+' '+statement+' late='+late_fail,r);
   const rows=await observe(mode,id);if(late_fail){expect(r.status!==200,mode+' '+statement+' late failure',r);expect(rows.length===0,mode+' '+statement+' late rollback',rows);}else{expect(r.status===200,mode+' '+statement+' legitimate caller commit',r);expect(JSON.stringify(rows)===JSON.stringify([{id:id+'-after',phase:'after'},{id:id+'-before',phase:'before'}]),mode+' '+statement+' exact retained rows',rows);if(rows.length===2)retained.push({mode,id,rows});}
  }
  const spoofId=mode+'-spoof-'+randomUUID(),spoof=await call('/child-guards/'+mode+'/run',{id:spoofId,statement:'spoof',catch_error:false,late_fail:false});await record('raw RAISE2D000 keeps non-guard origin '+mode,spoof);expect(spoof.status!==200&&spoof.status!==403,mode+' spoof not typed guard403',spoof);expect(/SQLSTATE 2D000/.test(JSON.stringify(spoof.body)),mode+' ordinary SQL error prefix retained',spoof);expect((await observe(mode,spoofId)).length===0,mode+' spoof rolls back',spoof);
 }
 const wrong=await call('/child-guards/on/run',{id:'weak-'+randomUUID(),statement:'savepoint',catch_error:false,late_fail:false},tenant,[]);await record('unprivileged role cannot run control',wrong);expect(wrong.status===403,'role guard',wrong);

 for(const mode of ['off','on'])for(const endpoint of ['origin','async_origin'])for(const variant of ['direct','mutate','getter','prototype','constructor','local','replace','copy','cause','string','forge','return']){
  const id='origin-'+mode+'-'+endpoint+'-'+variant+'-'+randomUUID(),r=await call('/child-guards/'+mode+'/'+endpoint,{id,mode:variant});
  await record('actual exception identity '+mode+' '+endpoint+' '+variant,r);
  const authoritative=['direct','mutate','getter','prototype','constructor'].includes(variant);
  expect(r.status===(authoritative?403:variant==='return'?200:500),'exception identity '+mode+' '+endpoint+' '+variant,r);
  if(authoritative)expect(/host-owned application transaction/.test(JSON.stringify(r.body)),'native authoritative diagnostic survives guest mutation',r);
  const rows=await observe(mode,id);
  if(variant==='return'){
   expect(JSON.stringify(rows)===JSON.stringify([{id:id+'-after',phase:'after'},{id:id+'-before',phase:'before'}]),'caught native denial followed by normal caller commit',rows);
   if(rows.length===2)retained.push({mode,id,rows});
  }else expect(rows.length===0,'unhandled native/guest exception rolls back actual owning TX',rows);
 }
 const recovery=async label=>{for(const x of retained)assert.deepEqual(await observe(x.mode,x.id),x.rows,label+' legitimate caller commits remain exact');};
 return {failures,recovery,retained,source_authority:'Generic test-owned rows only; not ERP/GL qualification'};
}
