import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import app from './app.mjs';

test('room authorization filters notes and encrypts continuations for that capability', async () => {
  const token = 'a'.repeat(43);
  const room = createHash('sha256').update(token).digest('hex');
  const next = { range_id: 1, after: 'another-room.private-record' };
  const note = { id: '10000000-0000-4000-8000-000000000001', text: 'Private note', author: 'Pat', created: 42 };
  const seen = [];
  const env = { EVENTS: {
    async list(options) { seen.push(options.cursor); return { events: [
      { key: 'own', value: { ...note, room } },
      { key: 'other', value: { ...note, room: 'another-room', text: 'Hidden content' } },
    ], next }; },
  } };
  const request = (capability, cursor = null) => app.fetch(new Request('http://local/api/page', {
    method: 'POST', headers: { authorization: `Bearer ${capability}`, 'content-type': 'application/json' },
    body: JSON.stringify({ cursor }),
  }), env);
  assert.equal((await request('missing')).status, 401);
  const first = await (await request(token)).json();
  assert.deepEqual(first.notes, [note]);
  assert.equal(typeof first.next, 'string');
  assert(!first.next.includes('private-record'));
  assert.equal((await request('b'.repeat(43), first.next)).status, 400);
  assert.equal(seen.length, 1, 'a different capability must not reach the data binding');
  assert.equal((await request(token, first.next)).status, 200);
  assert.deepEqual(seen[1], next);
});
