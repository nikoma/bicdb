// Independent default-domain specification only. No native/Rust execution.
import {MAX96, UNITS_LIMIT, nativeDecimal, toUnits, fromUnits} from './exact-units-oracle.mjs';
const fail=category=>{const error=new Error(category);error.category=category;throw error;};
const BASE=100_000_000_000_000;
function units(x){if(typeof x!=='string'||x.length>77||!/^(0|[1-9][0-9]*)$/.test(x))fail('units_grammar');const n=BigInt(x);if(n>=UNITS_LIMIT)fail('units_bound');return n;}
function scale(x){if(typeof x!=='number'||!Number.isInteger(x)||x<0||x>28)fail('scale');return x;}
function nearestEven(N,D){const q=N/D,r=N%D;return q+(r>D-r||r===D-r&&q%2n!==0n?1n:0n);}
function nearestNative(N,D){
 // Factor powers onto either side; compare each candidate to the original
 // rational rather than repeating Core's multiplication expression.
 for(let p=28;p>=0;p--){const c=nearestEven(N*10n**BigInt(p),D);if(c<=MAX96)return fromUnits(c,p);}
 fail('result_unrepresentable');
}
export function prorateNativeUnits(args){
 if(!Array.isArray(args)||args.length!==5)fail('arity');const C=units(args[0]),Q=units(args[1]),d=nativeDecimal(args[2]),p=scale(args[3]),s=scale(args[4]);
 if(Q===0n||d.coefficient<=0n)fail('quantity_domain');const q=toUnits(args[2],p);if(q>Q)fail('quantity_capacity');
 if(q===Q)return fromUnits(C,s); // Exact full depletion; never nearest-round C.
 const out=nearestNative(C*q,Q*10n**BigInt(s)),v=nativeDecimal(out);
 // Common-power rational comparison also admits a finer result than s.
 if(v.coefficient*10n**BigInt(s)>C*10n**BigInt(v.scale))fail('rounded_exceeds_basis');return out;
}
export function fullDisplayRatio(args){
 if(!Array.isArray(args)||args.length!==4)fail('arity');const C=units(args[0]),Q=units(args[1]),p=scale(args[2]),s=scale(args[3]);if(Q===0n)fail('quantity_domain');
 const exponent=p-s;return nearestNative(exponent>=0?C*10n**BigInt(exponent):C,exponent>=0?Q:Q*10n**BigInt(-exponent));
}
export function unitsFromLimbs(args){
 if(!Array.isArray(args)||args.length!==6)fail('arity');
 // Join fixed-width digits rather than reproducing Core's multiply-add fold.
 for(const n of args)if(typeof n!=='number'||!Number.isSafeInteger(n)||n<0||n>=BASE)fail('limb');
 const text=args.map(n=>String(n).padStart(14,'0')).join('').replace(/^0+/,'')||'0';return units(text).toString();
}
export function splitLimbs(n){const text=n.toString().padStart(84,'0');if(text.length>84)fail('units_bound');return Array.from({length:6},(_,i)=>Number(text.slice(i*14,i*14+14)));}
export const DEFAULT_OPERATIONS={exact_prorate_native_units_value:prorateNativeUnits,nearest_native_full_display_ratio_value:fullDisplayRatio,exact_units_from_limbs_value:unitsFromLimbs};
export function defaultOutcome(operation,args){try{return{value:DEFAULT_OPERATIONS[operation](args)};}catch(error){return{error:error.category??error.message};}}
