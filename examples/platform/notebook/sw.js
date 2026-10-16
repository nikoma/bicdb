const cacheName = 'bic-notebook-shell-v1';
self.addEventListener('install', event => event.waitUntil(caches.open(cacheName)
  .then(cache => cache.addAll(['./', './index.html', './app.css', './browser.mjs', './client.mjs']))));
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
self.addEventListener('fetch', event => {
  if (event.request.method !== 'GET') return;
  const url = new URL(event.request.url);
  const base = new URL('./', self.location.href);
  if (url.origin !== base.origin || !url.pathname.startsWith(base.pathname)) return;
  const asset = url.pathname.slice(base.pathname.length);
  if (!['', 'index.html', 'app.css', 'browser.mjs', 'client.mjs'].includes(asset)) return;
  event.respondWith(fetch(event.request).catch(() => caches.open(cacheName).then(cache => cache.match(event.request))));
});
