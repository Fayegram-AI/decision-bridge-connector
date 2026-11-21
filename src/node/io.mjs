import { createReadStream } from 'node:fs';
import { lstat, mkdir, open, link, rename, unlink } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { Transform } from 'node:stream';
import { ConnectorError, assert, isRecord, publicError, snapshot, validateSignal } from '../util.mjs';
import { stagingPath } from './file-staging.mjs';

const MAX_BYTES = 64 * 1024 * 1024;
const cancelled = signal => {
  if (signal?.aborted) throw new ConnectorError('ABORTED', 'I/O cancelled.');
};
const byteLimit = value => assert(Number.isSafeInteger(value) && value > 0 && value <= MAX_BYTES,
  'Byte limits must be positive integers through 64 MiB.');

/** Bound waiting even when a custom stream/iterator ignores destruction. */
function withAbort(promise, signal) {
  if (!signal) return Promise.resolve(promise);
  if (signal.aborted) {
    Promise.resolve(promise).catch(() => {});
    return Promise.reject(new ConnectorError('ABORTED', 'I/O cancelled.'));
  }
  return new Promise((resolvePromise, reject) => {
    const cleanup = () => signal.removeEventListener('abort', abort);
    const abort = () => { cleanup(); reject(new ConnectorError('ABORTED', 'I/O cancelled.')); };
    signal.addEventListener('abort', abort, { once: true });
    Promise.resolve(promise).then(value => { cleanup(); resolvePromise(value); }, error => { cleanup(); reject(error); });
  });
}

function releaseIterator(iterator) {
  // An outstanding read can keep return() pending forever. Request cleanup without
  // extending caller-visible cancellation; both paths have rejection handlers.
  void Promise.resolve().then(() => iterator.return?.()).catch(() => {});
}

export async function readJson(path, { stdin = process.stdin, maxBytes = 16 * 1024 * 1024, signal } = {}) {
  validateSignal(signal); byteLimit(maxBytes); cancelled(signal);
  const input = path === '-' ? stdin : createReadStream(path);
  const iterator = input[Symbol.asyncIterator]();
  const chunks = []; let bytes = 0; let ended = false;
  const cancel = () => input.destroy?.();
  signal?.addEventListener('abort', cancel, { once: true });
  try {
    while (true) {
      cancelled(signal);
      const { done, value } = await withAbort(Promise.resolve().then(() => iterator.next()), signal);
      if (done) { ended = true; break; }
      const data = Buffer.from(value); bytes += data.length;
      assert(bytes <= maxBytes, 'JSON input exceeds its byte limit.', 'INPUT_TOO_LARGE'); chunks.push(data);
    }
    cancelled(signal);
    try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks))); }
    catch { throw new ConnectorError('INVALID_JSON', 'Input must be valid UTF-8 JSON.'); }
  } catch (error) { if (signal?.aborted) cancelled(signal); throw error; }
  finally {
    signal?.removeEventListener('abort', cancel);
    if (path !== '-' || signal?.aborted) input.destroy?.();
    if (!ended) releaseIterator(iterator);
  }
}

export async function* jsonLines(input, { maxBytes = 16 * 1024 * 1024, signal, recoverRecords = false } = {}) {
  validateSignal(signal); byteLimit(maxBytes); cancelled(signal);
  const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });
  const bom = Buffer.from([0xef, 0xbb, 0xbf]);
  let record = Buffer.alloc(0); let used = 0; let oversized = false; let line = 0; let ended = false;
  const iterator = input[Symbol.asyncIterator]();
  const cancel = () => input.destroy?.();
  signal?.addEventListener('abort', cancel, { once: true });
  const tooLarge = () => new ConnectorError('INPUT_TOO_LARGE', 'JSONL record exceeds its byte limit.');
  function append(part) {
    if (oversized) return;
    // Allow a split CRLF and a stream-initial UTF-8 BOM before measuring the record.
    const capacity = maxBytes + (line === 0 ? 4 : 1);
    const size = used + part.length;
    if (size > capacity) {
      if (!recoverRecords) throw tooLarge();
      oversized = true;
      used = 0;
      return;
    }
    if (size > record.length) {
      const expanded = Buffer.allocUnsafe(Math.min(capacity, Math.max(1024, size, record.length * 2)));
      record.copy(expanded, 0, 0, used);
      record = expanded;
    }
    part.copy(record, used);
    used = size;
  }
  function finish(terminated) {
    let bytes = record.subarray(0, used);
    if (line === 0 && bytes.subarray(0, bom.length).equals(bom)) bytes = bytes.subarray(bom.length);
    if (terminated && bytes.at(-1) === 13) bytes = bytes.subarray(0, -1);
    const exceeded = oversized || bytes.length > maxBytes;
    used = 0;
    oversized = false;
    if (!terminated && !exceeded && bytes.length === 0) return null;
    line++;
    const failure = error => {
      if (!recoverRecords) throw error;
      return { line, error: publicError(error, 'ingress') };
    };
    if (exceeded) return failure(tooLarge());
    let text;
    try { text = decoder.decode(bytes); }
    catch { return failure(new ConnectorError('INVALID_UTF8', 'JSONL record is not valid UTF-8.')); }
    try { return { line, value: JSON.parse(text) }; }
    catch { return { line, error: { code: 'INVALID_JSON', message: 'Line must contain valid JSON.', stage: 'ingress' } }; }
  };
  try {
    while (true) {
      cancelled(signal);
      const item = await withAbort(Promise.resolve().then(() => iterator.next()), signal);
      if (item.done) { ended = true; break; }
      const chunk = Buffer.isBuffer(item.value) ? item.value : Buffer.from(item.value);
      let start = 0;
      while (start < chunk.length) {
        cancelled(signal);
        const newline = chunk.indexOf(10, start);
        append(chunk.subarray(start, newline === -1 ? chunk.length : newline));
        if (newline === -1) break;
        yield finish(true);
        start = newline + 1;
      }
    }
    cancelled(signal);
    const last = finish(false);
    if (last) yield last;
  } catch (error) { if (signal?.aborted) cancelled(signal); throw error; }
  finally {
    signal?.removeEventListener('abort', cancel);
    if (!ended || signal?.aborted) input.destroy?.();
    if (!ended) releaseIterator(iterator);
  }
}

/** Ordered results remain observable while the input producer is idle. */
export async function* orderedMap(source, transform, concurrency = 4, { signal } = {}) {
  assert(Number.isSafeInteger(concurrency) && concurrency > 0 && concurrency <= 64, 'Invalid batch concurrency.');
  assert(typeof transform === 'function', 'A batch transform is required.');
  validateSignal(signal); cancelled(signal);
  const iterator = source[Symbol.asyncIterator]?.() ?? source[Symbol.iterator]?.();
  assert(iterator && typeof iterator.next === 'function', 'A batch source must be iterable.');
  const pending = [];
  let reading;
  let ended = false;
  const failure = error => ({ kind: 'error', error });
  try {
    while (!ended || pending.length) {
      cancelled(signal);
      if (!ended && !reading && pending.length < concurrency) {
        reading = Promise.resolve().then(() => { cancelled(signal); return iterator.next(); })
          .then(item => ({ kind: 'input', item }), failure);
      }
      const next = await withAbort(Promise.race([
        ...(reading ? [reading] : []), ...(pending.length ? [pending[0]] : []),
      ]), signal);
      if (next.kind === 'error') throw next.error;
      if (next.kind === 'input') {
        reading = undefined;
        ended = next.item.done;
        if (!ended) pending.push(Promise.resolve().then(() => {
          cancelled(signal); return transform(next.item.value);
        }).then(value => ({ kind: 'output', value }), failure));
      } else {
        pending.shift();
        yield next.value;
      }
    }
  } finally {
    // All transform/read promises are observed. A caller stopping consumption owns
    // cancellation of its in-flight tasks; we do not await uncooperative callbacks.
    if (!ended) releaseIterator(iterator);
  }
}

export function writeStream(stream, text, signal, { destroyOnAbort = true, end = false } = {}) {
  return new Promise((resolvePromise, reject) => {
    let settled = false;
    let pending = false;
    const outputError = () => new ConnectorError('OUTPUT_ERROR', 'The output stream did not accept a complete record.', { ambiguous: true });
    const cleanup = () => {
      signal?.removeEventListener('abort', abort);
      stream.removeListener?.('error', error);
      stream.removeListener?.('close', close);
    };
    const finish = failure => {
      if (settled) return;
      settled = true;
      signal?.removeEventListener('abort', abort);
      if (failure) reject(failure); else resolvePromise();
    };
    const error = () => { finish(outputError()); cleanup(); };
    const close = () => { if (pending) finish(outputError()); cleanup(); };
    const abort = () => {
      finish(new ConnectorError('ABORTED', 'Output cancelled; a prefix may already have been written.', { ambiguous: pending }));
      // Keep error/close listeners until a pending write settles, including a late
      // EPIPE after cancellation. Destroying the pipe stops blocked OS writes.
      if (pending && destroyOnAbort) stream.destroy?.(); else if (!pending) cleanup();
    };
    stream.on?.('error', error);
    stream.once?.('close', close);
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) { abort(); return; }
    try {
      pending = true;
      stream[end ? 'end' : 'write'](text, failure => {
        pending = false;
        finish(failure ? outputError() : undefined);
        // Node emits a write error after calling the write callback.
        if (failure) setImmediate(cleanup); else cleanup();
      });
    } catch {
      pending = false; finish(outputError()); setImmediate(cleanup);
    }
  });
}

export async function createOutput(path, { force = false, stdout = process.stdout, signal } = {}) {
  validateSignal(signal); cancelled(signal);
  assert(typeof force === 'boolean', 'force must be boolean.');
  if (!path || path === '-') return {
    async write(value) {
      cancelled(signal);
      await writeStream(stdout, `${JSON.stringify(snapshot(value))}\n`, signal);
    }, async finish() { cancelled(signal); }, async abort() {},
  };
  const target = resolve(path); await mkdir(dirname(target), { recursive: true, mode: 0o700 }); cancelled(signal);
  if (!force) {
    try { await lstat(target); throw new ConnectorError('OUTPUT_ERROR', 'Cannot publish output; existing files require --force.'); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  cancelled(signal);
  const temporary = stagingPath(target); const handle = await open(temporary, 'wx', 0o600); let closed = false;
  async function close() { if (!closed) { closed = true; await handle.close(); } }
  if (signal?.aborted) { await close(); await unlink(temporary).catch(() => {}); cancelled(signal); }
  return {
    async write(value) { cancelled(signal); await handle.writeFile(`${JSON.stringify(snapshot(value))}\n`); cancelled(signal); },
    async finish() {
      let committing = false;
      try {
        cancelled(signal); await handle.sync(); cancelled(signal); await close(); cancelled(signal);
        // Once submitted, the filesystem commit cannot be withdrawn by cancellation.
        committing = true;
        if (force) await rename(temporary, target); else { await link(temporary, target); await unlink(temporary); }
      } catch (error) {
        if (error instanceof ConnectorError) throw error;
        throw new ConnectorError('OUTPUT_ERROR', 'Cannot publish output; existing files require --force.',
          { ambiguous: committing && error.code !== 'EEXIST' });
      }
    },
    async abort() { await close().catch(() => {}); await unlink(temporary).catch(() => {}); },
  };
}

export function processSource({ command, args = [], cwd, env = {}, timeoutMs = 60000,
  maxErrorBytes = 65536, maxOutputBytes = 64 * 1024 * 1024 }, signal) {
  validateSignal(signal); cancelled(signal);
  assert(typeof command === 'string' && command.trim().length > 0 && !command.includes('\0') &&
    Array.isArray(args) && args.every(x => typeof x === 'string' && !x.includes('\0')), 'Invalid incoming process configuration.');
  assert(cwd === undefined || typeof cwd === 'string' && cwd.length > 0, 'Invalid incoming process working directory.');
  assert(isRecord(env) && Object.entries(env).every(([key, value]) => key.length > 0 && !/[=\0]/.test(key) &&
    typeof value === 'string' && !value.includes('\0')), 'Invalid incoming process environment.');
  assert(Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 2147483647, 'Invalid process deadline.');
  byteLimit(maxErrorBytes); byteLimit(maxOutputBytes);
  let child;
  try { child = spawn(command, [...args], { cwd, env: { ...env }, shell: false, detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'] }); }
  catch { throw new ConnectorError('PROCESS_ERROR', 'Unable to start incoming process.'); }
  let failure; let errorBytes = 0; let outputBytes = 0;
  let settled = false; let stopping = false; let timer; let killTimer;
  let resolveDone; let rejectDone;
  const done = new Promise((resolvePromise, reject) => { resolveDone = resolvePromise; rejectDone = reject; });
  done.catch(() => {});
  const stream = new Transform({ transform(chunk, _encoding, callback) {
    outputBytes += chunk.length;
    if (outputBytes > maxOutputBytes) callback(new ConnectorError('OUTPUT_LIMIT', 'Incoming process output exceeded its byte limit.'));
    else callback(null, chunk);
  } });
  const finish = () => {
    if (settled) return;
    settled = true; clearTimeout(timer); clearTimeout(killTimer); signal?.removeEventListener('abort', cancel);
    if (failure) rejectDone(failure); else resolveDone();
  };
  const kill = () => {
    try { if (child.pid && process.platform !== 'win32') process.kill(-child.pid, 'SIGKILL'); else child.kill('SIGKILL'); }
    catch { /* Exit may race termination. */ }
  };
  const stop = cause => {
    failure ??= cause;
    if (settled || stopping) return;
    stopping = true; kill(); stream.destroy(failure);
    killTimer = setTimeout(() => { child.stdout.destroy(); child.stderr.destroy(); kill(); finish(); }, 250);
  };
  const cancel = () => stop(new ConnectorError('ABORTED', 'Incoming process cancelled.'));
  // The error listener also protects callers that await done without consuming the stream.
  stream.on('error', error => stop(error instanceof ConnectorError ? error : new ConnectorError('PROCESS_ERROR', 'Incoming process stream failed.')));
  child.stdout.on('error', () => stop(new ConnectorError('PROCESS_ERROR', 'Incoming process stdout failed.')));
  child.stdout.pipe(stream);
  child.stderr.on('data', chunk => {
    errorBytes += chunk.length;
    if (errorBytes > maxErrorBytes) stop(new ConnectorError('OUTPUT_LIMIT', 'Incoming process stderr exceeded its byte limit.'));
  });
  child.once('error', () => stop(new ConnectorError('PROCESS_ERROR', 'Unable to start incoming process.')));
  child.once('close', code => {
    if (code !== 0) failure ??= new ConnectorError('PROCESS_ERROR', 'Incoming process exited unsuccessfully.');
    if (failure) stream.destroy(failure);
    finish();
  });
  timer = setTimeout(() => stop(new ConnectorError('TIMEOUT', 'Incoming process deadline exceeded.')), timeoutMs);
  signal?.addEventListener('abort', cancel, { once: true });
  if (signal?.aborted) cancel();
  return { stream, done, close: cancel };
}

export const safeLineFailure = error => ({ ok: false, error: publicError(error, 'ingress') });
