import test from 'node:test';
import assert from 'node:assert/strict';
import {MAX96, UNITS_LIMIT, LIST_CAP, UnitsAccumulator, sumUnits, prorateUnits, displayRatio,
  nativeDecimal, toUnits, outcome} from './exact-units-oracle.mjs';
import {buildCases, DISPLAY, rawNativeCases} from './exact-units-vectors.mjs';

test('exact unit sum retains signed cancellation and genuinely wide final units', () => {
  assert.equal(sumUnits([[MAX96.toString(),'0.01','-'+MAX96],2]),'1');
  assert.equal(sumUnits([[MAX96.toString(),'-0.01'],2]),(MAX96*100n-1n).toString());
  assert.equal(sumUnits([['-2.50','1.25'],2]),'-125');
  assert.equal(sumUnits([['-0.0000','0e1000000'],28]),'0');
  assert.equal(sumUnits([[],28]),'0');
});

test('each subunit is refused even when the final mathematical total is integral', () => {
  assert.throws(()=>sumUnits([['0.001','-0.001'],2]),{category:'subunit'});
  assert.throws(()=>sumUnits([[null,'1'],2]),{category:'decimal'});
  const sum=new UnitsAccumulator(2);sum.add('1.25');
  assert.throws(()=>sum.add('0.001'),{category:'subunit'});
  assert.equal(sum.finish(),'125','failed add did not partially change the state');
});

test('shared exact scalar admission retains scientific values without rounding', () => {
  assert.equal(sumUnits([['1e2','1.00000000000000000000000000000'],0]),'101');
  assert.equal(sumUnits([['1e-28'],28]),'1');
  assert.equal(sumUnits([[(MAX96*10n).toString()+'e-1'],0]),MAX96.toString());
  assert.throws(()=>sumUnits([[(MAX96+1n).toString()],0]),{category:'decimal'});
  assert.throws(()=>sumUnits([['1e-29'],28]),{category:'decimal'});
  assert.equal(sumUnits([[1.25,'2.75'],2]),'400','internal shared decoder compatibility, not a Carrier wire type assertion');
});

test('memory list cap stays unchanged while the streaming contract has no list materialization cap', () => {
  assert.equal(sumUnits([Array(LIST_CAP).fill('0'),28]),'0');
  assert.throws(()=>sumUnits([Array(LIST_CAP+1).fill('0'),28]),{category:'list_cap'});
  const stream=new UnitsAccumulator(28);for(let i=0;i<=LIST_CAP;i++)stream.add('0');
  assert.equal(stream.finish(),'0');
  // This is a Node mathematical contract test, not native SQL capacity evidence.
});

test('remaining-state rounding preserves both original financial sequences', () => {
  for(const[C,Q,operations,expected]of [['1','4',['1','1','1','1'],['0','0','0','0.01']],
    ['5','6',['2','1','3'],['0.02','0.01','0.02']]]) {
    let c=BigInt(C),q=BigInt(Q);const actual=[];
    for(const op of operations){const value=prorateUnits([c.toString(),q.toString(),op,0,2]);actual.push(value);c-=toUnits(value,2);q-=BigInt(op);}
    assert.deepEqual(actual,expected);assert.equal(c,0n);assert.equal(q,0n);
  }
});

test('wide remainders around a midpoint stay on the correct side', () => {
  const base=1n<<200n,Q=(2n*base).toString();
  assert.equal(prorateUnits([(base-1n).toString(),Q,'1',0,2]),'0');
  assert.equal(prorateUnits([base.toString(),Q,'1',0,2]),'0');
  assert.equal(prorateUnits([(base+1n).toString(),Q,'1',0,2]),'0.01');
  assert.equal(prorateUnits([(3n*base-1n).toString(),Q,'1',0,2]),'0.01');
  assert.equal(prorateUnits([(3n*base).toString(),Q,'1',0,2]),'0.02');
});

test('zero total never bypasses quantity, precision or capacity validation', () => {
  for(const args of [['0','0','1',0,2],['0','1','0',0,2],['0','1','-1',0,2],
    ['0','1','2',0,2],['0','1','0.001',2,2],['0','1','1',13,2],['0','1','1',0,9]])
    assert(outcome('exact_prorate_units_value',args).error,JSON.stringify(args));
});

test('proration normalizes only exact zeros and refuses an unrepresentable final result', () => {
  assert.equal(prorateUnits([(MAX96*10n**8n).toString(),'1','1',0,8]),MAX96.toString());
  assert.throws(()=>prorateUnits([(MAX96*100n-1n).toString(),'1','1',0,2]),{category:'result_unrepresentable'});
  assert.equal(prorateUnits([(UNITS_LIMIT-1n).toString(),(UNITS_LIMIT-1n).toString(),'1',12,8]),'10000');
  const numerator=(UNITS_LIMIT-1n)*MAX96*10n**12n;
  assert.equal(numerator.toString(2).length,390);
  assert.throws(()=>prorateUnits([(UNITS_LIMIT-1n).toString(),(UNITS_LIMIT-1n).toString(),MAX96.toString(),12,8]),{category:'result_unrepresentable'});
});

test('wide MAX+.01 display is NEW DOMAIN semantics, never ordinary Decimal equivalence or money admission', () => {
  const C=(MAX96*100n+1n).toString();
  assert.equal(displayRatio([C,'1',0,2]),MAX96.toString());
  assert.throws(()=>prorateUnits([C,'1','1',0,2]),{category:'result_unrepresentable'});
  assert.throws(()=>nativeDecimal(MAX96+'.01'),{category:'decimal'});
  assert.throws(()=>displayRatio([(MAX96*100n+50n).toString(),'1',0,2]),{category:'rate_unrepresentable'});
});

test('derived display rates do not supply financial carrying values', () => {
  assert.equal(displayRatio(['1',MAX96.toString(),0,2]),'0');
  assert.equal(prorateUnits(['1',MAX96.toString(),MAX96.toString(),0,2]),'0.01');
  assert.equal(displayRatio(['1','3',0,0]),'0.3333333333333333333333333333');
  assert.equal(displayRatio([MAX96.toString(),'2',0,0]),'39614081257132168796771975168');
});

test('all ordinary native division captures match only after exact ratio re-encoding', async () => {
  const cases=(await buildCases()).filter(c=>c.domain==='ordinary_native_value_compatibility');
  assert.equal(cases.length,169);let successes=0,failures=0;
  for(const c of cases){if(c.captured.status===200){assert.equal(c.expected.error,undefined,c.id);const a=nativeDecimal(c.expected.value),b=nativeDecimal(c.captured.value);assert.deepEqual(a,b,c.id);successes++;}
    else {assert(c.expected.error,c.id);failures++;}}
  assert.equal(successes,149);assert.equal(failures,20);
});

test('portable corpus is deterministic, strict and includes raw-native-only type cases', async () => {
  const a=await buildCases(),b=await buildCases();assert.deepEqual(a,b);
  assert.equal(new Set(a.map(c=>c.id)).size,a.length);
  assert(a.some(c=>c.domain==='wide_new_semantics'&&c.ordinary_equivalence===false));
  assert(a.some(c=>c.id.endsWith('/ subunit refuses cancellation')&&c.expected.error==='subunit'));
  assert(a.filter(c=>c.operation===DISPLAY).length>=169);
  assert.equal(rawNativeCases.length,3);
  for(const c of rawNativeCases)assert.equal(c.expected_error,true);
});
