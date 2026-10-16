import { Notebook } from './client.mjs';

const $ = id => document.getElementById(id);
if (!location.pathname.endsWith('/')) location.replace(location.pathname + '/' + location.search + location.hash);
if (!/^[A-Za-z0-9_-]{43}$/.test(location.hash.slice(1))) {
  const token = btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(32))))
    .replaceAll('+', '-').replaceAll('/', '_').replaceAll('=', '');
  history.replaceState(null, '', '#' + token);
}
const capability = location.hash.slice(1);
const room = Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(capability))))
  .map(byte => byte.toString(16).padStart(2, '0')).join('');
const open = indexedDB.open('bic-notebook', 1);
open.onupgradeneeded = () => {
  open.result.createObjectStore('notes', { keyPath: 'key' });
  open.result.createObjectStore('pending', { keyPath: 'key' });
};
const db = await new Promise((resolve, reject) => { open.onsuccess = () => resolve(open.result); open.onerror = () => reject(open.error); });
const key = id => `${room}/${id}`;
const request = action => new Promise((resolve, reject) => { action.onsuccess = () => resolve(action.result); action.onerror = () => reject(action.error); });
const transaction = (stores, action) => new Promise((resolve, reject) => {
  const tx = db.transaction(stores, 'readwrite');
  tx.oncomplete = resolve; tx.onerror = () => reject(tx.error); tx.onabort = () => reject(tx.error);
  action(tx);
});
const all = async store => (await request(db.transaction(store).objectStore(store).getAll(IDBKeyRange.bound(room + '/', room + '/\uffff')))).map(row => row.note);
const store = {
  enqueue: note => transaction(['notes', 'pending'], tx => {
    for (const name of ['notes', 'pending']) tx.objectStore(name).put({ key: key(note.id), note });
  }),
  pending: () => all('pending'),
  acknowledge: id => transaction(['pending'], tx => tx.objectStore('pending').delete(key(id))),
  merge: notes => transaction(['notes'], tx => {
    for (const note of notes) tx.objectStore('notes').put({ key: key(note.id), note });
  }),
};
async function api(path, value) {
  const response = await fetch('./api/' + path, { method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${capability}` },
    body: JSON.stringify(value), signal: AbortSignal.timeout(25000) });
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || 'Sync failed');
  return result;
}
const notebook = new Notebook(store, { append: note => api('notes', note), page: cursor => api('page', { cursor }) });
async function render() {
  const pending = new Set((await store.pending()).map(note => note.id));
  const notes = (await all('notes')).sort((a, b) => b.created - a.created || a.id.localeCompare(b.id));
  $('notes').replaceChildren(...notes.map(note => {
    const item = document.createElement('article');
    const meta = document.createElement('small');
    meta.textContent = `${note.author || 'Anonymous'} · ${new Date(note.created).toLocaleString()} · ${pending.has(note.id) ? 'Saved on device; awaiting sync' : 'Synced'}`;
    const text = document.createElement('p'); text.textContent = note.text;
    item.append(meta, text); return item;
  }));
  $('count').textContent = `${notes.length} notes · ${pending.size} awaiting sync`;
}
let cursor = null;
let syncing = false;
async function sync() {
  if (syncing) return;
  syncing = true;
  try {
    if (!navigator.onLine) throw new Error('Offline. Your notes are saved on this device.');
    await notebook.flush();
    // Bound work per poll; a complete range walk repeats to discover notes
    // inserted before an earlier page. Event IDs remove duplicate deliveries.
    for (let i = 0; i < 4; i++) { cursor = await notebook.page(cursor); if (!cursor) break; }
    $('status').textContent = cursor ? 'Syncing more pages…' : 'Connected · live updates every 2 seconds';
  } catch (error) { $('status').textContent = error.message; }
  finally { syncing = false; await render(); }
}
$('compose').addEventListener('submit', async event => {
  event.preventDefault();
  const note = { id: crypto.randomUUID(), text: $('text').value.trim(), author: $('author').value.trim(), created: Date.now() };
  if (!note.text) return;
  try {
    await notebook.add(note);
    $('text').value = ''; await render(); void sync();
  } catch { $('status').textContent = 'Could not save on this device. Your text has been kept in the editor.'; }
});
$('share').addEventListener('click', async () => {
  try { await navigator.clipboard.writeText(location.href); $('status').textContent = 'Link copied. Anyone with this link can read and add notes.'; }
  catch { $('status').textContent = 'Copy the address bar to share this notebook.'; }
});
addEventListener('online', sync); addEventListener('offline', sync);
await render(); void sync(); setInterval(sync, 2000);
if ('serviceWorker' in navigator) navigator.serviceWorker.register('./sw.js').catch(() => { $('status').textContent = 'Offline shell unavailable; keep this tab open while offline.'; });
