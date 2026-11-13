// Independent specification, not Core/native execution. It rounds the integer
// quotient and remainder instead of repeating Core's midpoint inequality.
import{MAX96,UNITS_LIMIT}from'./exact-units-oracle.mjs';
const fail=category=>{const e=new Error(category);e.category=category;throw e;};
function unsigned(x){if(typeof x!=='string'||x.length>77||!/^(0|[1-9][0-9]*)$/.test(x))fail('units_grammar');const n=BigInt(x);if(n>=UNITS_LIMIT)fail('units_bound');return n;}
function scale(x){if(typeof x!=='number'||!Number.isInteger(x)||x<0||x>28)fail('scale');return x;}
export function ratioRepresentable(args){
 if(!Array.isArray(args)||args.length!==4)fail('arity');
 const C=unsigned(args[0]),Q=unsigned(args[1]),p=scale(args[2]),s=scale(args[3]);if(Q===0n)fail('quantity_domain');
 const e=p-s,N=e>=0?C*10n**BigInt(e):C,D=e>=0?Q:Q*10n**BigInt(-e),whole=N/D,remainder=N%D;
 const rounded=whole+(remainder>D-remainder||remainder===D-remainder&&whole%2n!==0n?1n:0n);
 return rounded<=MAX96;
}
export function ratioOutcome(args){try{return{value:ratioRepresentable(args)};}catch(e){return{error:e.category??e.message};}}
