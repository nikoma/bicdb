import http from 'node:http';
import { pathToFileURL } from 'node:url';

const application = (await import(pathToFileURL(process.env.BICDB_ENTRYPOINT).href)).default;
if (!application || typeof application.fetch !== 'function') throw new Error('application must export a default fetch handler');
const endpoint = process.env.BICDB_DATABASE_URL;
const token = process.env.BICDB_DATABASE_TOKEN;
async function call(path, body) {
  const response = await fetch(endpoint + path, {
    method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
    body: JSON.stringify(body), signal: AbortSignal.timeout(30000),
  });
  const value = await response.json();
  if (!response.ok) throw new Error(value.error || 'database request failed');
  return value;
}
function rows(result) {
  return result.rows.map(row => Object.fromEntries(result.columns.map((column, i) => [column, row[i]])));
}
const env = Object.freeze({
  DB: Object.freeze({
    async conflicts(table) { return call('/conflicts', { table }); },
    async query(sql, params = []) { return rows((await call('/query', { statements: [{ sql, params }] }))[0]); },
    async transaction(statements, options = {}) {
      return (await call('/query', { statements, idempotency_key: options.idempotencyKey ?? null, jobs: options.jobs ?? [] })).map(rows);
    },
  }),
  JOBS: Object.freeze({
    async send(payload, options = {}) { return call('/jobs', { payload, idempotency_key: options.idempotencyKey ?? null }); },
  }),
});
const maximum = 8 * 1024 * 1024;
const server = http.createServer(async (incoming, outgoing) => {
  try {
    const url = new URL(incoming.url, 'http://application.local');
    if (url.pathname.startsWith('/__bic/')) {
      if (incoming.headers['x-bic-internal-token'] !== token) { outgoing.writeHead(404).end(); return; }
      if (url.pathname === '/__bic/ready') { outgoing.writeHead(204).end(); return; }
      if (url.pathname !== '/__bic/job' || incoming.method !== 'POST') { outgoing.writeHead(404).end(); return; }
      if (typeof application.queue !== 'function') { outgoing.writeHead(422).end(); return; }
      const chunks = []; let size = 0;
      for await (const chunk of incoming) { size += chunk.length; if (size > maximum) throw new Error('request too large'); chunks.push(chunk); }
      await application.queue(JSON.parse(Buffer.concat(chunks).toString('utf8')), env);
      outgoing.writeHead(204).end(); return;
    }
    const chunks = []; let size = 0;
    for await (const chunk of incoming) { size += chunk.length; if (size > maximum) { outgoing.writeHead(413).end(); return; } chunks.push(chunk); }
    const method = incoming.method || 'GET';
    const headers = new Headers();
    for (let i = 0; i < incoming.rawHeaders.length; i += 2) {
      if (!incoming.rawHeaders[i].toLowerCase().startsWith('x-bic-')) headers.append(incoming.rawHeaders[i], incoming.rawHeaders[i + 1]);
    }
    const request = new Request(url, { method, headers, ...(['GET', 'HEAD'].includes(method) ? {} : { body: Buffer.concat(chunks) }) });
    const response = await application.fetch(request, env);
    if (!(response instanceof Response)) throw new Error('fetch handler must return a Response');
    outgoing.statusCode = response.status;
    for (const [key, value] of response.headers) if (key !== 'set-cookie') outgoing.setHeader(key, value);
    const cookies = response.headers.getSetCookie();
    if (cookies.length) outgoing.setHeader('set-cookie', cookies);
    if (response.body && method !== 'HEAD') {
      for await (const chunk of response.body) {
        if (!outgoing.write(chunk)) await new Promise(resolve => outgoing.once('drain', resolve));
      }
    }
    outgoing.end();
  } catch {
    if (!outgoing.headersSent) outgoing.writeHead(500, { 'content-type': 'application/json' });
    outgoing.end('{"error":"application handler failed"}');
  }
});
server.requestTimeout = 30000;
server.headersTimeout = 10000;
server.listen(Number(process.env.PORT), '127.0.0.1');
process.on('SIGTERM', () => { server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 5000).unref(); });
