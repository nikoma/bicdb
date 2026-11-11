import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import tomllib
import shutil
import resource
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

work = Path('/tmp/bicdb-local-namespace-public')
out = Path('/tmp/bicdb-public-namespace-validation-v2')
out.mkdir(parents=True, exist_ok=True)
# Same frozen source/worktree as v1; reuse completed Core/rename/snapshot checks.
patch = subprocess.check_output(['git', 'diff', '--binary'], cwd=work)
(out / 'candidate.patch').write_bytes(patch)
identity = {
    'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=work, text=True).strip(),
    'uncommitted_patch_sha256': hashlib.sha256(patch).hexdigest(),
    'source_version': tomllib.loads((work / 'crates/bicdb-core/Cargo.toml').read_text())['package']['version'],
    'scope': 'Public opt-in local physical namespace identity only on pending source475/SSI474 parent; no native image reader capability or RF3 activation. Focused default-release checks, pinned463 old-reader negative, and broad Core CI-backstop-enabled unit scope; no physical/production qualification',
    'physical_qualification': False,
}
environment = os.environ.copy()
environment.update(CARGO_TARGET_DIR='/tmp/bicdb-azure-artifact-target', CARGO_BUILD_JOBS='1',
                   CARGO_INCREMENTAL='0', RUST_TEST_THREADS='2')
prefix = ['cargo', 'test', '--release', '--locked', '-p', 'bicdb-core', '--features', 'tls-replication',
          '--config', 'profile.release.package.bicdb-core.opt-level=0',
          '--config', 'profile.release.package.bicdb-core.debug=0']
build = ['cargo', 'build', '--release', '--locked', '-p', 'bicdb-core', '--features', 'tls-replication',
         '--config', 'profile.release.package.bicdb-core.opt-level=0',
         '--config', 'profile.release.package.bicdb-core.debug=0', '--example', 'ha_state_child']
jobs = [('build-ha-crash-child', build),
        ('integration-ha_state_crash', prefix + ['--test', 'ha_state_crash', '--', '--nocapture']),
        ('integration-log_archive_pitr', prefix + ['--test', 'log_archive_pitr', '--', '--nocapture'])]
results = []
for name, argv in jobs:
    environment.pop('BICDB_PAGED_UNIQUE_APPLY_PROBE', None)
    environment.pop('BICDB_OLD_READER_BINARY', None)
    override = {}
    if name == 'namespace-old-reader-fence':
        override['BICDB_OLD_READER_BINARY'] = '/tmp/bicdb-azure-artifact-463-evidence/bicdb-1.0.463-beta-d49a391b'
        environment.update(override)
    if name == 'all-core-units':
        override['BICDB_PAGED_UNIQUE_APPLY_PROBE'] = '1'
        environment.update(override)
    started = time.monotonic()
    paused = False
    interrupted = None
    samples = []
    with (out / (name + '.stdout')).open('w') as stdout, (out / (name + '.stderr')).open('w') as stderr:
        process = subprocess.Popen(argv, cwd=work, env=environment, stdout=stdout, stderr=stderr,
                                   start_new_session=True)
        while process.poll() is None:
            fields = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
            available = int(fields['MemAvailable'].split()[0]) * 1024
            samples.append({'elapsed_seconds': time.monotonic() - started, 'available_bytes': available,
                            'paused': paused, 'swap_total_bytes': int(fields['SwapTotal'].split()[0]) * 1024, 'swap_free_bytes': int(fields['SwapFree'].split()[0]) * 1024, 'memory_pressure': Path('/proc/pressure/memory').read_text(), 'disk_free_bytes': shutil.disk_usage('/tmp').free})
            if available < 8 * 1024 ** 3 or shutil.disk_usage('/tmp').free < 8 * 1024 ** 3 or time.monotonic() - started > 1200:
                interrupted = 'severe_memory_headroom' if available < 8 * 1024 ** 3 else 'validation_time_bound'
                if paused:
                    os.killpg(process.pid, signal.SIGCONT)
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                break
            if available < 12 * 1024 ** 3 and not paused:
                os.killpg(process.pid, signal.SIGSTOP)
                paused = True
            elif available >= 14 * 1024 ** 3 and paused:
                os.killpg(process.pid, signal.SIGCONT)
                paused = False
            time.sleep(1)
        code = process.wait()
    result = dict(identity, environment_override=override, name=name, command=argv, exit_code=code, interrupted_reason=interrupted,
                  elapsed_seconds=time.monotonic() - started,
                  minimum_available_bytes=min(x['available_bytes'] for x in samples) if samples else None)
    (out / (name + '-result.json')).write_text(json.dumps(result, indent=2) + '\n')
    (out / (name + '-resources.json')).write_text(json.dumps(samples, indent=2) + '\n')
    results.append(result)
    print(json.dumps(result), flush=True)
    if code or interrupted:
        break
    if not name.startswith('build-') and not __import__('re').search(r'test result: ok\. [1-9][0-9]* passed;', (out / (name + '.stdout')).read_text()):
        raise RuntimeError('zero tests or missing success summary: ' + name)
(out / 'candidate-validation.json').write_text(json.dumps(results, indent=2) + '\n')
