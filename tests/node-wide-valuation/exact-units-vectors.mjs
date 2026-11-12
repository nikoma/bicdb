import fs from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {MAX96, UNITS_LIMIT, LIST_CAP, nativeDecimal, outcome, toUnits} from './exact-units-oracle.mjs';

export const SUM = 'exact_decimal_sum_units_value';
export const PRORATE = 'exact_prorate_units_value';
export const DISPLAY = 'nearest_native_display_ratio_value';
const permutations = [[0,1,2],[0,2,1],[1,0,2],[1,2,0],[2,0,1],[2,1,0]];

// Exact re-encoding of an ordinary native Decimal ratio. This does not coerce
// an unrepresentable numerator to Decimal or claim wide ordinary equivalence.
export function encodeOrdinaryRatio(left, right) {
  const a = nativeDecimal(left), b = nativeDecimal(right);
  let C = a.coefficient, Q = b.coefficient, exponent = b.scale - a.scale;
  let p = 0, s = 0;
  if (exponent > 12) { C *= 10n ** BigInt(exponent - 12); p = 12; }
  else if (exponent < -8) { Q *= 10n ** BigInt(-8 - exponent); s = 8; }
  else if (exponent >= 0) p = exponent;
  else s = -exponent;
  return [C.toString(), Q.toString(), p, s];
}

export async function buildCases({includeLimits = false} = {}) {
  const cases = [];
  const add = (id, operation, args, metadata = {}) => cases.push({id:operation+' / '+id, operation, arguments: args,
    expected: outcome(operation, args), ...metadata});
  for (const terms of [[MAX96.toString(),'0.01','-'+MAX96], [MAX96.toString(),'-0.01','-'+MAX96]])
    for (const order of permutations) add('signed cancellation '+terms[1]+' '+order.join(''), SUM, [order.map(i=>terms[i]),2]);
  for (const [id, args] of [
    ['empty', [[],28]], ['wide before MAX-.01', [[MAX96.toString(),'-0.01'],2]],
    ['negative total', [['-2.50','1.25'],2]], ['negative zero canonical', [['-0.00','0'],28]],
    ['redundant zeros exact', [['1.00000000000000000000000000000'],0]],
    ['scientific exact', [['1e-28','-1e-28','1e2'],28]],
    ['shared decoder simple numeric value', [[1.25,'2.75'],2]],
    ['subunit refuses cancellation', [['0.001','-0.001'],2]], ['null is not skipped', [[null],2]],
    ['null values', [null,2]], ['not a list', ['["1"]',2]], ['nested value', [[['1']],2]],
    ['bool value', [[true],2]], ['object value', [[{value:'1'}],2]],
    ['unrepresentable input', [[(MAX96+1n).toString()],0]],
    ['overprecision input', [['0.00000000000000000000000000001'],28]],
    ['scale string', [['1'],'2']], ['scale fraction', [['1'],2.5]], ['scale high', [['1'],29]],
    ['scale negative', [['1'],-1]], ['arity zero', []], ['arity extra', [[],2,0]],
  ]) add(id, SUM, args);

  const parts = ['0.01',(MAX96-1n).toString(),'0.99'];
  for (const cost of ['0','1']) for (const order of permutations) {
    let C = cost==='0' ? 0n : MAX96*100n, Q = MAX96*10n**12n;
    for (const [index,k] of order.entries()) {
      const args = [C.toString(),Q.toString(),parts[k],12,2];
      add('actual MAX rate'+cost+' order'+order.join('')+' line'+index, PRORATE, args,
        {domain:'exact remaining state; no initial-prefix policy substitution'});
      const expected = cases.at(-1).expected;
      if (expected.error) throw Error('Actual MAX specification unexpectedly refused');
      C -= toUnits(expected.value,2); Q -= toUnits(parts[k],12);
    }
    if (C!==0n||Q!==0n) throw Error('Actual MAX conservation');
  }
  for (const [id,args] of [
    ['zero', ['0','1','1',0,2]], ['full exact MAX normalized', [(MAX96*10n**8n).toString(),'1','1',0,8]],
    ['half even down', ['1','2','1',0,2]], ['half odd up', ['3','2','1',0,2]],
    ['half upper even down', ['5','2','1',0,2]],
    ['near-half wide below', [((1n<<200n)-1n).toString(),(1n<<201n).toString(),'1',0,2]],
    ['near-half wide exact', [(1n<<200n).toString(),(1n<<201n).toString(),'1',0,2]],
    ['near-half wide above', [((1n<<200n)+1n).toString(),(1n<<201n).toString(),'1',0,2]],
    ['wide operands small exact result', [(UNITS_LIMIT-1n).toString(),(UNITS_LIMIT-1n).toString(),'1',12,8]],
    ['390-bit bounded numerator final failure', [(UNITS_LIMIT-1n).toString(),(UNITS_LIMIT-1n).toString(),MAX96.toString(),12,8]],
    ['full before amount unrepresentable', [(MAX96*100n-1n).toString(),'1','1',0,2]],
    ['zero C still rejects zero Q', ['0','0','1',0,2]], ['zero C still rejects capacity', ['0','1','2',0,2]],
    ['zero C still rejects subunit', ['0','1','0.001',2,2]],
    ['zero C still rejects precision', ['0','1','1',13,2]], ['zero C still rejects scale', ['0','1','1',0,9]],
    ['operated zero', ['1','1','0',0,2]], ['operated negative', ['1','1','-1',0,2]],
    ['operated null', ['1','1',null,0,2]], ['operated bool', ['1','1',true,0,2]],
    ['operated scientific exact', ['100','10','1e-1',1,2]],
    ['operated unrepresentable', ['1',(UNITS_LIMIT-1n).toString(),(MAX96+1n).toString(),0,2]],
    ['precision string', ['1','1','1','0',2]], ['arity too few', ['1','1','1',0]],
  ]) add(id, PRORATE, args);
  for (const [id,args] of [
    ['zero', ['0','1',12,8]], ['recurring third', ['1','3',0,0]],
    ['tiny positive rate displays zero', ['1',MAX96.toString(),0,2]],
    ['greatest coefficient scale', [MAX96.toString(),'2',0,0]],
    ['wide MAX plus cent display NEW DOMAIN', [(MAX96*100n+1n).toString(),'1',0,2]],
    ['wide MAX minus cent display NEW DOMAIN', [(MAX96*100n-1n).toString(),'1',0,2]],
    ['wide MAX plus half tie cannot fit', [(MAX96*100n+50n).toString(),'1',0,2]],
    ['wide common terms', [(UNITS_LIMIT-1n).toString(),(UNITS_LIMIT-1n).toString(),12,8]],
    ['display zero C still rejects zero Q', ['0','0',0,2]], ['ratio beyond native magnitude', [(UNITS_LIMIT-1n).toString(),'1',0,0]],
    ['precision fraction', ['1','1',0.5,2]], ['scale string', ['1','1',0,'2']], ['arity extra', ['1','1',0,2,0]],
  ]) add(id, DISPLAY, args, {domain:id.includes('NEW DOMAIN')?'wide_new_semantics':'bounded display',ordinary_equivalence:false});
  for (const bad of ['', '00', '01', '+1', '-0', '-1', '1.0', '1e2', ' 1', '1\n', '１', '١',
    '9'.repeat(78), UNITS_LIMIT.toString(), null, true, 1, {units:'1'}]) {
    add('C canonical refusal '+JSON.stringify(bad), PRORATE, [bad,'1','1',0,2]);
    add('Q canonical refusal '+JSON.stringify(bad), PRORATE, ['0',bad,'1',0,2]);
    add('display C canonical refusal '+JSON.stringify(bad), DISPLAY, [bad,'1',0,2]);
    add('display Q canonical refusal '+JSON.stringify(bad), DISPLAY, ['0',bad,0,2]);
  }

  const captured = JSON.parse(await fs.readFile(new URL('./ordinary-division-b998.json',import.meta.url)));
  for (const [index,c] of captured.cases.entries()) add('captured ordinary native division '+index, DISPLAY,
    encodeOrdinaryRatio(c.numerator,c.denominator), {domain:'ordinary_native_value_compatibility',captured:c});

  if (includeLimits) {
    add('existing list element cap accepted by leaf wrapper', SUM, [Array(LIST_CAP).fill('0'),28],
      {scope:'Core wrapper only; outer structured/wire admission may refuse earlier'});
    add('existing list element cap exceeded', SUM, [Array(LIST_CAP+1).fill('0'),28],
      {scope:'Core wrapper only; not a limit increase or ERP capacity claim'});
  }
  return cases;
}

// JSON parsers in JS cannot retain the distinction between literal2 and2.0.
// These raw representations are explicit native/ABI-only admission vectors.
export const rawNativeCases = [
  {operation:SUM,raw_arguments_json:'[["1"],2.0]',expected_error:true},
  {operation:PRORATE,raw_arguments_json:'["1","1","1",0.0,2]',expected_error:true},
  {operation:DISPLAY,raw_arguments_json:'["1","1",0,2e0]',expected_error:true},
];

if (process.argv[1] && fileURLToPath(import.meta.url)===process.argv[1]) {
  const cases = await buildCases({includeLimits:process.argv.includes('--include-limits')});
  const output = process.argv.find((a,i)=>i>1&&!a.startsWith('--'));
  const value = {schema:'node-wide-valuation-v1',scope:'Independent Node oracle/vector specification only; Core build/runtime qualification is separate.',cases,raw_native_only:rawNativeCases};
  if (output) await fs.writeFile(output,JSON.stringify(value,null,2)+'\n');
  console.log(JSON.stringify({cases:cases.length,raw_native_only:rawNativeCases.length,output:output??null}));
}
