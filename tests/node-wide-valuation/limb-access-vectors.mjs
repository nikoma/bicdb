import assert from 'node:assert/strict';
export const BASE = 100000000000000n;
export const LIMIT = 1n << 254n;
export function limbOracle(units, position) {
  assert.equal(typeof units, 'string');
  assert.match(units, /^(0|[1-9][0-9]{0,76})$/);
  assert(BigInt(units) < LIMIT);
  assert(Number.isSafeInteger(position) && position >= 0 && position <= 5);
  return Number(BigInt(units) / (BASE ** BigInt(position)) % BASE);
}
export function limbVectors() {
  const values = [0n, 1n, BASE-1n, BASE, BASE+1n, LIMIT-1n];
  for (let n=1n; n<=5n; n++) values.push(BASE**n-1n, BASE**n, BASE**n+1n);
  let state = 72810941n;
  for (let n=0; n<160; n++) { state=(state*6364136223846793005n+1442695040888963407n)%LIMIT; values.push(state); }
  return [...new Set(values.map(String))].flatMap(units=>Array.from({length:6},(_,position)=>({units,position,expected:limbOracle(units,position)})));
}
