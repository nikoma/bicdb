import test from 'node:test';
import assert from 'node:assert/strict';
import { Notebook } from './client.mjs';

test('a lost response and a browser restart retain the same durable event ID', async () => {
  const records = new Map(), pending = new Map(), server = new Map();
  const store = {
    async enqueue(note) { records.set(note.id, note); pending.set(note.id, note); },
    async pending() { return [...pending.values()]; },
    async acknowledge(id) { pending.delete(id); },
    async merge(notes) { for (const note of notes) records.set(note.id, note); },
  };
  let responseLost = true;
  const transport = {
    async append(note) { server.set(note.id, note); if (responseLost) throw Error('connection lost after commit'); },
    async page() { return { notes: [...server.values()], next: null }; },
  };
  const note = { id: 'stable-id', text: 'saved offline', created: 42 };
  await new Notebook(store, transport).add(note);
  await assert.rejects(new Notebook(store, transport).flush());
  assert.equal(pending.size, 1);
  responseLost = false;
  const reopened = new Notebook(store, transport);
  await reopened.flush();
  await reopened.page(null); await reopened.page(null);
  assert.equal(pending.size, 0); assert.equal(server.size, 1); assert.equal(records.size, 1);
  assert.deepEqual(server.get(note.id), note);
});
