import test from 'node:test';
import assert from 'node:assert/strict';
import {BASE,LIMIT,limbOracle,limbVectors} from './limb-access-vectors.mjs';
test('six exact bounded limbs reconstruct canonical unsigned units including254-bit boundary',()=>{
  const cases=limbVectors(); assert(cases.length>1000);
  for(let i=0;i<cases.length;i+=6){let value=0n;for(let p=5;p>=0;p--){const c=cases[i+p];assert(c.expected>=0 && BigInt(c.expected)<BASE);value=value*BASE+BigInt(c.expected);}assert.equal(value.toString(),cases[i].units);}
});
test('invalid canonical units and positions are rejected before decomposition',()=>{
  for(const units of ['', '00','01','-0','-1','+1',' 1','1 ','1.0','1e2','１２','1'.repeat(78),LIMIT.toString(),null,1,true,[],{}])assert.throws(()=>limbOracle(units,0));
  for(const p of [-1,6,1.5,'1',null,true,[],{}])assert.throws(()=>limbOracle('0',p));
});
