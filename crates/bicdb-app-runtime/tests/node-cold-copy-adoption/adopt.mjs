// Qualification harness only. The first-party CLI owns all restore authority.
import fs from 'node:fs/promises';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {spawn} from 'node:child_process';
import {createReadStream} from 'node:fs';

const ROOT = 'native-origin-root-v1.json';
const FLAG = 'native_origin_root_v1';
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
async function fileDigest(file) {
  const hash = createHash('sha256');
  for await (const bytes of createReadStream(file)) hash.update(bytes);
  return hash.digest('hex');
}
async function durable(file, value) {
  const handle = await fs.open(file, 'w', 0o600);
  try {await handle.writeFile(JSON.stringify(value, null, 2) + '\n'); await handle.sync();}
  finally {await handle.close();}
}
export async function snapshotTree(directory) {
  const files = [];
  async function walk(relative = '') {
    for (const name of (await fs.readdir(path.join(directory, relative))).sort()) {
      const file = path.join(relative, name), absolute = path.join(directory, file), stat = await fs.lstat(absolute);
      assert(!stat.isSymbolicLink(), 'Qualification stores must not contain symlinks: ' + file);
      if (stat.isDirectory()) await walk(file);
      else {
        assert(stat.isFile(), 'Unexpected special store file: ' + file);
        files.push({file, bytes:stat.size, mode:stat.mode & 0o777, sha256:await fileDigest(absolute)});
      }
    }
  }
  await walk();
  return files;
}
async function origin(directory) {
  let format;
  try {format = JSON.parse(await fs.readFile(path.join(directory, 'format.json'), 'utf8'));}
  catch (error) {if (error.code !== 'ENOENT') throw error; format = {feature_flags:[]};}
  assert(Array.isArray(format.feature_flags), 'Native format flags must be explicit or legacy-absent');
  const active = format.feature_flags.includes(FLAG);
  let record = null, bytes = null;
  try {bytes = await fs.readFile(path.join(directory, ROOT)); record = JSON.parse(bytes);}
  catch (error) {if (error.code !== 'ENOENT') throw error;}
  if (active) assert(record, 'An active origin requires its genuine record');
  // Read for evidence only; never recompute/write a checksum or authorization.
  const generation = bytes?.toString().match(/"custody_generation"\s*:\s*(\d+)/)?.[1] ?? '0';
  return {active, flags:format.feature_flags, record, generation, recordFileSHA256:bytes ? digest(bytes) : null};
}

function assertStopped(closedSource) {
  assert(Number.isInteger(closedSource?.pid) && closedSource.pid > 0, 'The caller must retain its stopped source PID');
  assert(Number.isInteger(closedSource.exitCode) || (typeof closedSource.signalCode === 'string' && closedSource.signalCode.length > 0), 'The source process must actually exit before copying');
  try {process.kill(closedSource.pid, 0); assert.fail('The source PID is still alive');}
  catch (error) {assert.equal(error.code, 'ESRCH', 'The stopped source PID must no longer exist');}
}

export async function qualifyColdCopy({native, source, target, evidence, openArgs, closedSource}) {
  assert(native?.path && /^[a-f0-9]{64}$/.test(native.sha256));
  assert.equal(await fileDigest(native.path), native.sha256, 'Exact selected native pin');
  assert.notEqual(path.resolve(source), path.resolve(target));
  assert(!path.resolve(target).startsWith(path.resolve(source) + path.sep), 'Do not copy a source into itself');
  assertStopped(closedSource);
  await fs.mkdir(evidence, {recursive:false, mode:0o700});
  let number = 0;
  async function command(name, args) {
    const id = ++number;
    await durable(path.join(evidence, id + '-before.json'), {name, binary:native.path, sha256:native.sha256, args});
    const stdout = [], stderr = [];
    const child = spawn(native.path, args);
    let timedOut = false;
    const deadline = setTimeout(() => {timedOut = true; child.kill('SIGTERM');},30000);
    child.stdout.on('data', b => stdout.push(b)); child.stderr.on('data', b => stderr.push(b));
    const result = await new Promise(resolve => {
      let transportError;
      child.once('error', error => {transportError={name:error.name,message:error.message};});
      child.once('close', (code, signal) => resolve({code,signal,...(transportError ? {transportError} : {})}));
    });
    clearTimeout(deadline);
    Object.assign(result, {name, args, timedOut, stdout:Buffer.concat(stdout).toString(), stderr:Buffer.concat(stderr).toString()});
    await durable(path.join(evidence, id + '-response.json'), result);
    assert.equal(timedOut,false,'New custody CLI operation exceeded its30s qualification bound');
    return result;
  }
  const sourceBefore = await snapshotTree(source), observed = await origin(source);
  await durable(path.join(evidence, 'source-before.json'), {source, files:sourceBefore, origin:observed, closedSource, sourcePIDAbsent:true});
  await fs.cp(source, target, {recursive:true,errorOnExist:true,force:false});
  assert.deepEqual(await snapshotTree(source), sourceBefore);
  assert.deepEqual(await snapshotTree(target), sourceBefore, 'Exact closed copy before any open');
  const targetBefore = await snapshotTree(target);
  const targetOriginBefore = await origin(target);
  assert.equal(targetOriginBefore.active, observed.active);
  const negative = [];
  async function refusal(name, args, pattern) {
    const result = await command(name,args);
    assert.equal(result.transportError, undefined);
    assert.equal(typeof result.code,'number','A signal/transport loss is not a refusal');
    assert.notEqual(result.code,0,name+' must refuse');
    assert.match(result.stdout + result.stderr,pattern,name+' must fail at the real custody boundary');
    negative.push({name,code:result.code});
  }
  if (observed.active) {
    await refusal('active plain copy cannot open', openArgs(target), /Native origin root: copied or replaced physical directory/);
    assert.deepEqual(await snapshotTree(source),sourceBefore);
    assert.deepEqual(await snapshotTree(target),targetBefore,'Refusal must not repair/rebind the copy');
    const unauthorized = target + '-unauthorized-source', unauthorizedTarget = target + '-unauthorized-target';
    await fs.cp(source,unauthorized,{recursive:true,errorOnExist:true,force:false});
    await fs.cp(source,unauthorizedTarget,{recursive:true,errorOnExist:true,force:false});
    await refusal('unauthorized source copy cannot confer custody',
      ['backup','adopt-copy',unauthorized,unauthorizedTarget], /restore source is itself an unauthorized copy/);
    assert.deepEqual(await snapshotTree(unauthorized),sourceBefore);
    assert.deepEqual(await snapshotTree(unauthorizedTarget),sourceBefore);
    assert.deepEqual(await snapshotTree(source),sourceBefore);
  }
  const adopted = await command('official offline adopt-copy', ['backup','adopt-copy',source,target]);
  assert.equal(adopted.transportError,undefined); assert.equal(adopted.code,0,adopted.stdout+adopted.stderr);
  const after = await origin(target), sourceAfter = await snapshotTree(source), targetAfter = await snapshotTree(target);
  assert.deepEqual(sourceAfter,sourceBefore,'Official API must never alter the source');
  if (observed.active) {
    assert.match(adopted.stdout,/Adopted copy .*custody generation/);
    assert.equal(after.active,true); assert.equal(after.record.root_id,observed.record.root_id);
    assert.equal(BigInt(after.generation),BigInt(observed.generation)+1n);
    assert.equal(after.record.predecessor_sha256,observed.record.checksum_sha256);
    assert.notEqual(after.recordFileSHA256,observed.recordFileSHA256);
    assert.deepEqual(targetAfter.filter(x=>x.file!==ROOT),targetBefore.filter(x=>x.file!==ROOT),'Only the copied root record may change');
  } else {
    assert.equal(after.active,false);
    assert.match(adopted.stdout,/No native origin root .*copy opens as it is/);
    assert.deepEqual(targetAfter,targetBefore,'Ordinary/legacy inactive NotRequired must write no store/root data');
  }
  const again = await command('official adopt-copy idempotent retry', ['backup','adopt-copy',source,target]);
  assert.equal(again.code,0,again.stdout+again.stderr);
  assert.match(again.stdout,observed.active?/was already adopted/:/No native origin root .*copy opens as it is/);
  assert.deepEqual(await snapshotTree(source),sourceBefore);
  assert.deepEqual(await snapshotTree(target),targetAfter,'Retry must not increment/rewrite custody');
  if (observed.active) {
    const copiedAdoption=target+'-copied-adoption';
    await fs.cp(target,copiedAdoption,{recursive:true,errorOnExist:true,force:false});
    await refusal('plain copy of adopted store cannot open',openArgs(copiedAdoption),/Native origin root: copied or replaced physical directory/);
    assert.deepEqual(await snapshotTree(copiedAdoption),targetAfter);
    assert.deepEqual(await snapshotTree(source),sourceBefore);
  }
  const report={passed:true,source,target,native,observedOriginActive:observed.active,
    outcome:observed.active?'Rebound then AlreadyRebound':'NotRequired with both flags inactive',
    activeOriginRefusalQualified:observed.active,activeOriginRefusals:negative,
    noHAConfigurationChange:true,sourceBefore,sourceAfter,targetBefore,targetAfter,
    closedSource,sourceNeverAppOpenedByHelper:true,firstOpenOfAuthorizedTargetIsCallersNextStep:true};
  await durable(path.join(evidence,'qualification.json'),report);
  return {source,evidence,sourceBefore,closedSource,observedOriginActive:observed.active};
}

export async function assertColdCopySourceUnchanged(report) {
  assertStopped(report.closedSource);
  assert.deepEqual(await snapshotTree(report.source),report.sourceBefore,'Cold recovery must keep original closed source exact');
  await durable(path.join(report.evidence,'source-after-cold-runtime.json'),
    {passed:true,source:report.source,files:report.sourceBefore,closedOriginalPreserved:true});
}
