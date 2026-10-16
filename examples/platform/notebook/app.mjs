import { readFile } from 'node:fs/promises';
import { createHash, createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';

const assets = new Map([
  ['/', ['index.html', 'text/html; charset=utf-8']],
  ['/index.html', ['index.html', 'text/html; charset=utf-8']],
  ['/app.css', ['app.css', 'text/css; charset=utf-8']],
  ['/browser.mjs', ['browser.mjs', 'text/javascript; charset=utf-8']],
  ['/client.mjs', ['client.mjs', 'text/javascript; charset=utf-8']],
  ['/sw.js', ['sw.js', 'text/javascript; charset=utf-8']],
]);
const headers = {
  'cache-control': 'no-store',
  'referrer-policy': 'no-referrer',
  'x-content-type-options': 'nosniff',
  'content-security-policy': "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; worker-src 'self'; base-uri 'none'; frame-ancestors 'none'",
};
const reply = (value, status = 200) => Response.json(value, { status, headers });

// Browser cursors contain no other room's record IDs. Deriving this key from
// the high-entropy room capability also binds continuations to that room and
// lets a supervisor restart without invalidating them.
function encodeCursor(cursor, key) {
  if (!cursor) return null;
  const nonce = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', key, nonce);
  cipher.setAAD(Buffer.from('bic-notebook-cursor-v1'));
  const data = Buffer.concat([cipher.update(JSON.stringify(cursor)), cipher.final()]);
  return Buffer.concat([nonce, cipher.getAuthTag(), data]).toString('base64url');
}
function decodeCursor(cursor, key) {
  if (cursor === null) return null;
  if (typeof cursor !== 'string' || cursor.length > 2048 || !/^[A-Za-z0-9_-]+$/.test(cursor)) throw Error('Invalid cursor');
  const data = Buffer.from(cursor, 'base64url');
  if (data.length < 29) throw Error('Invalid cursor');
  const decipher = createDecipheriv('aes-256-gcm', key, data.subarray(0, 12));
  decipher.setAAD(Buffer.from('bic-notebook-cursor-v1'));
  decipher.setAuthTag(data.subarray(12, 28));
  return JSON.parse(Buffer.concat([decipher.update(data.subarray(28)), decipher.final()]).toString('utf8'));
}

export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (request.method === 'GET' && assets.has(path)) {
      const [file, type] = assets.get(path);
      return new Response(await readFile(new URL(file, import.meta.url)), {
        headers: { ...headers, 'content-type': type },
      });
    }
    if (!path.startsWith('/api/')) return reply({ error: 'Not found' }, 404);
    // The fragment of the shared link is a 256-bit room capability. It is
    // deliberately absent from URLs sent to servers and access logs.
    const token = request.headers.get('authorization')?.match(/^Bearer ([A-Za-z0-9_-]{43})$/)?.[1];
    if (!token || Buffer.from(token, 'base64url').length !== 32) return reply({ error: 'Room capability required' }, 401);
    const room = createHash('sha256').update(token).digest('hex');
    const cursorKey = createHash('sha256').update('bic-notebook-cursor-v1').update(token).digest();
    try {
      if (path === '/api/notes' && request.method === 'POST') {
        const note = await request.json();
        if (!/^[a-f0-9-]{36}$/.test(note.id) || typeof note.text !== 'string' || !note.text.trim()
            || note.text.length > 4000 || typeof note.author !== 'string' || note.author.length > 80
            || !Number.isSafeInteger(note.created) || note.created < 0) return reply({ error: 'Invalid note' }, 400);
        // Keep the stored payload stable across retries. Unknown input fields
        // never become part of the event or change another room's namespace.
        const value = { room, text: note.text, author: note.author, created: note.created, id: note.id };
        const result = await env.EVENTS.append(`${room}.${note.id}`, value);
        return reply({ id: note.id, ...result });
      }
      if (path === '/api/page' && request.method === 'POST') {
        const { cursor = null } = await request.json();
        let continuation;
        try { continuation = decodeCursor(cursor, cursorKey); }
        catch { return reply({ error: 'Invalid notebook continuation' }, 400); }
        const page = await env.EVENTS.list({ cursor: continuation, limit: 128 });
        return reply({ notes: page.events.filter(event => event.value.room === room)
          .map(event => ({ id: event.value.id, text: event.value.text, author: event.value.author, created: event.value.created })),
          next: encodeCursor(page.next, cursorKey) });
      }
      return reply({ error: 'Not found' }, 404);
    } catch (error) {
      if (String(error.message).includes('different value')) return reply({ error: 'This note ID already has different content. Keep the local copy for review.' }, 409);
      return reply({ error: 'State is temporarily unavailable; queued notes remain on this device.' }, 503);
    }
  },
};
