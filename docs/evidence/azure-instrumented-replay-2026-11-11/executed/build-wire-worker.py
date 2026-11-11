#!/usr/bin/env python3
"""Link the test-only wire bridge against an existing frozen optimized core cache."""
import argparse, hashlib, json, os, pathlib, signal, subprocess, time, tomllib

def sha(path):
    h = hashlib.sha256()
    with pathlib.Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--frozen-source', type=pathlib.Path, required=True)
    p.add_argument('--artifact-target', type=pathlib.Path, required=True)
    p.add_argument('--expected-source-commit',required=True)
    p.add_argument('--core-rlib',type=pathlib.Path,required=True)
    p.add_argument('--expected-core-sha256',required=True)
    p.add_argument('--output', type=pathlib.Path, required=True)
    p.add_argument('--minimum-available-bytes', type=int, default=8 * 1024 ** 3)
    p.add_argument('--timeout', type=int, default=300)
    a = p.parse_args()
    source_commit = subprocess.check_output(['git', '-C', str(a.frozen_source), 'rev-parse', 'HEAD'], text=True).strip()
    if source_commit != a.expected_source_commit:p.error('linked core source does not match explicit pinned commit')
    if subprocess.check_output(['git','-C',str(a.frozen_source),'status','--porcelain','--untracked-files=no'],text=True).strip():p.error('linked core source checkout has tracked changes')
    core_version=tomllib.loads((a.frozen_source/'crates/bicdb-core/Cargo.toml').read_text())['package']['version']
    if not a.core_rlib.is_file() or sha(a.core_rlib)!=a.expected_core_sha256:p.error('explicit core library hash mismatch')
    a.output.mkdir(mode=448, parents=True, exist_ok=False)
    source = pathlib.Path(__file__).with_name('wire-worker.rs')
    frozen = a.output / 'wire-worker.executed.rs'
    frozen.write_bytes(source.read_bytes())
    frozen.chmod(256)
    deps = a.artifact_target / 'release/deps'
    names = {'serde_json': '6161d00796a1441f', 'sha2': 'e98894dbca273d11'}
    libraries = {name: deps / f'lib{name}-{suffix}.rlib' for name, suffix in names.items()}
    if a.core_rlib.resolve().parent!=deps.resolve():p.error('core library must belong to explicit artifact dependency directory')
    libraries['bicdb_core']=a.core_rlib
    if not all((v.is_file() for v in libraries.values())):
        p.error('exact frozen release library cache unavailable; do not silently rebuild or substitute')
    command = ['rustc', '--crate-name', 'bicdb_trace_wire_worker', '--edition=2021', str(frozen), '-o', str(a.output / 'wire-worker'), '-C', 'opt-level=1', '-C', 'debuginfo=0', '-C', 'lto=off', '-C', 'codegen-units=1', '-L', 'dependency=' + str(deps)]
    for name, path in libraries.items():
        command += ['--extern', name + '=' + str(path)]
    for directory in sorted({v.parent for v in (a.artifact_target / 'release/build').rglob('*.a')}):
        command += ['-L', 'native=' + str(directory)]
    report = {'format': 1, 'source_commit': source_commit, 'linked_core_version':core_version,'policy_source':'b05385d4b033edf0db03f3eccf4bdd86f7c8eddb','policy_files_sha256':{name:sha(a.frozen_source/'crates/bicdb-core/src'/name) for name in ['distribution_events.rs','distribution_transport.rs','distribution.rs','replication_transport.rs']}, 'source_sha256': sha(frozen), 'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(), 'argv': command, 'libraries': {k: {'path': str(v), 'sha256': sha(v)} for k, v in libraries.items()}, 'started_unix_ns': time.time_ns(), 'minimum_available_bytes': a.minimum_available_bytes, 'build_jobs': 1, 'engine_recompiled': False, 'memory_samples': []}
    (a.output / 'build.json').write_text(json.dumps(report, indent=2) + '\n')
    with (a.output / 'stdout').open('w') as out, (a.output / 'stderr').open('w') as err:
        build_env={**os.environ,'BICDB_TRACE_CORE_SOURCE':source_commit,'BICDB_TRACE_CORE_VERSION':core_version}
        process = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True,env=build_env)
        deadline = time.monotonic() + a.timeout
        while process.poll() is None:
            memory = {line.split(':')[0]: int(line.split()[1]) * 1024 for line in pathlib.Path('/proc/meminfo').read_text().splitlines()}
            available = memory['MemAvailable']
            report['memory_samples'].append({'unix_ns': time.time_ns(), 'available_bytes': available})
            if available < a.minimum_available_bytes or time.monotonic() > deadline:
                report['interrupted_reason'] = 'memory_headroom' if available < a.minimum_available_bytes else 'bounded_link_timeout'
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                break
            time.sleep(0.5)
    report.update(exit_code=process.wait(), completed_unix_ns=time.time_ns())
    if report['exit_code'] == 0:
        report['binary_sha256'] = sha(a.output / 'wire-worker')
        report['binary_bytes'] = (a.output / 'wire-worker').stat().st_size
    (a.output / 'build.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k in ('exit_code', 'interrupted_reason', 'binary_sha256', 'binary_bytes', 'source_sha256')}))
    raise SystemExit(report['exit_code'])
if __name__ == '__main__':
    main()
