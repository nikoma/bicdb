import { readFileSync } from 'node:fs';
import { spawn, spawnSync } from 'node:child_process';
const configuration = JSON.parse(readFileSync('/app/launch.json', 'utf8'));
const entrypoint = `/app/release/${configuration.entrypoint}`;
const arguments_ = [`--max-old-space-size=${configuration.heap_mb}`,
  configuration.runtime === 'node' ? '/app/adapter.mjs' : entrypoint];
const child = spawn('/usr/bin/node', arguments_, {
  cwd: '/app/release', stdio: 'inherit', uid: 1000, gid: 1000,
  env: {
    PATH: '/usr/bin:/bin', NODE_ENV: 'production', HOST: '0.0.0.0', PORT: '8080',
    BICDB_ENTRYPOINT: entrypoint,
    BICDB_DATABASE_URL: configuration.database_url,
    BICDB_DATABASE_TOKEN: configuration.database_token,
  },
});
child.on('error', () => spawnSync('/bin/poweroff', ['-f']));
child.on('exit', () => spawnSync('/bin/poweroff', ['-f']));
