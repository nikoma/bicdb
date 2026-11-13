import {MAX96,UNITS_LIMIT,toUnits}from'./exact-units-oracle.mjs';
import{defaultOutcome,splitLimbs}from'./default-units-oracle.mjs';
export function defaultVectors(){
 const rows=[],add=(operation,args,label)=>rows.push({id:rows.length+1,operation,args,label,...defaultOutcome(operation,args)}),P='exact_prorate_native_units_value',D='nearest_native_full_display_ratio_value',L='exact_units_from_limbs_value',M=MAX96.toString(),max=UNITS_LIMIT-1n;
 for(const C of['0','1','3','5','7','10',M])for(const[Q,q,p,s]of[['3','1',0,0],['4','1',0,2],['10','0.05',2,28],[(MAX96*10n**28n).toString(),M,28,0]])add(P,[C,Q,q,p,s],'default ordinary/tie/zero/full28');
 for(const args of[
  [(MAX96*10n**28n).toString(),(MAX96*10n**28n).toString(),M,28,28],
  [(MAX96*100n+1n).toString(),'1','1',0,2],
  [(MAX96*10n**28n-1n).toString(),(MAX96*10n**28n+1n).toString(),M,28,28],
  ['1','3','1',0,0],['3','10','0.05',2,28],['5','10','0.05',2,28],['7','10','0.05',2,28],
  ['1',(max).toString(),'0.0000000000000000000000000001',28,28],
  [max.toString(),max.toString(),M,28,28],
  [max.toString(),'1','1',0,0],['0','0','1',0,0],['0','1','2',0,0],['0','1','0',0,0],['0','1','0.01',0,0],['1','1','-1',0,0],['01','1','1',0,0],['1','1','1',29,0],['1','1','1',0,29],['1','1',true,0,0],['1','1','1','0',0],[(UNITS_LIMIT).toString(),'1','1',0,0],['1','1','1',0],['1','1','1',0,0,0]
 ])add(P,args,'identity/conservation/domain/strict metadata');
 for(const C of['0','1','3',M,(MAX96*10n**28n+1n).toString(),max.toString()])for(const[Q,p,s]of[['3',0,0],['1',28,28],[(MAX96*10n**28n).toString(),28,28]])add(D,[C,Q,p,s],'full native display separate from money authority');
 for(const args of[['0','0',0,0],['1','1',29,0],['1','1',0,29],['1','1',0],['1','1',0,0,0],['-1','1',0,0],['1','01',0,0],['1','1','0',0],['1','1',true,0],['1','1',0.5,0]])add(D,args,'display strict domains');
 for(const n of[0n,1n,10n**14n-1n,10n**14n,10n**28n-1n,10n**28n,10n**56n,10n**70n,max,UNITS_LIMIT,10n**84n-1n])add(L,splitLimbs(n),'six-limb normalization/bounds');
 for(const args of[[],[0,0,0,0,0],[0,0,0,0,0,0,0],[0,0,0,0,0,-1],[0,0,0,0,0,100_000_000_000_000],[0,0,0,0,0,1.5],[0,0,0,0,0,'1'],[0,0,0,0,0,true],[0,0,0,0,0,null],null])add(L,args,'arity/actual-kind/limb poison');
 // Ordinary cases are linked separately to actual capture; these cases exercise
 // a genuinely wide basis that native Decimal operands cannot encode.
 for(const q of['0.01','0.99',(MAX96-1n).toString()])add(P,[(MAX96*10n**28n).toString(),(MAX96*10n**28n).toString(),q,28,28],'MAX transfer domain full-scale units');
 return rows;
}
export const rawDefaultIntKindVectors=[
 {operation:'exact_units_from_limbs_value',raw:'[0,0,0,0,0,2.0]',error:'strict_Int64_kind'},
 {operation:'exact_units_from_limbs_value',raw:'[0,0,0,0,0,2e0]',error:'strict_Int64_kind'},
 {operation:'exact_prorate_native_units_value',raw:'["1","3","1",0.0,0]',error:'strict_Int64_kind'},
 {operation:'nearest_native_full_display_ratio_value',raw:'["1","3",0,0e0]',error:'strict_Int64_kind'}
];
