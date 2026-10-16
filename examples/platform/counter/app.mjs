export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (request.method === 'POST' && path === '/increment') {
      const key = request.headers.get('idempotency-key');
      if (!key) return Response.json({ error: 'idempotency-key header required' }, { status: 400 });
      const result = await env.DB.transaction([
        { sql: 'UPDATE counters SET value = value + 1 WHERE id = $1 RETURNING value', params: ['requests'] },
      ], { idempotencyKey: key });
      return Response.json(result[0][0]);
    }
    if (request.method === 'POST' && path === '/jobs') {
      const key = request.headers.get('idempotency-key');
      if (!key) return Response.json({ error: 'idempotency-key header required' }, { status: 400 });
      return Response.json(await env.JOBS.send({ action: 'increment' }, { idempotencyKey: key }), { status: 202 });
    }
    if (request.method !== 'GET' || path !== '/') return new Response('Not found', { status: 404 });
    return Response.json(await env.DB.query('SELECT id, value FROM counters ORDER BY id'));
  },
  async queue(message, env) {
    // Broker delivery can repeat after a crash. The receipt and increment
    // commit together, so a repeated delivery has one database effect.
    await env.DB.transaction([
      { sql: 'UPDATE counters SET value = value + 1 WHERE id = $1', params: ['jobs'] },
    ], { idempotencyKey: `job:${message.id}` });
  },
};
