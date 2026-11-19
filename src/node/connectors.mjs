import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { link, lstat, mkdir, open, rename, unlink } from 'node:fs/promises';
import { basename, resolve } from 'node:path';
import { ConnectorError, assert, bounded, captureObject, deliveryPreflight, identifier, isRecord, keysOnly, snapshot, validateSignal } from '../util.mjs';
import { stagingPath } from './file-staging.mjs';
import { incompleteReceipt, validateReceipt } from '../engine/receipts.mjs';

const MAX_BYTES = 64 * 1024 * 1024;
const caps = (acknowledgement, idempotent, local = false) => Object.freeze({
  direction: 'outgoing', runtime: 'node', acknowledgement, idempotent, local,
});
const bytesOption = (value, label) => {
  assert(Number.isSafeInteger(value) && value > 0 && value <= MAX_BYTES, `${label} must be between 1 and 64 MiB.`);
  return value;
};
const timeoutOption = value => {
  assert(Number.isSafeInteger(value) && value > 0 && value <= 2147483647, 'timeoutMs must be a positive 32-bit integer.');
  return value;
};
const encoded = (payload, maxBytes, pretty = false, encoding = 'json', envelope = false) => {
  let value;
  try { value = envelope ? captureObject(payload, ['payload'], maxBytes) : snapshot(payload, { maxBytes }); }
  catch (error) {
    if (error.code === 'RESOURCE_LIMIT') throw new ConnectorError('PAYLOAD_TOO_LARGE', 'Outgoing payload exceeds its resource limit.');
    throw error;
  }
  assert(encoding !== 'text' || typeof value === 'string', 'Text payloads must be strings.');
  const data = Buffer.from(encoding === 'text' ? value : `${JSON.stringify(value, null, pretty ? 2 : undefined)}\n`);
  assert(data.length <= maxBytes, 'Outgoing payload exceeds its byte limit.', 'PAYLOAD_TOO_LARGE');
  return data;
};
const cancelled = signal => {
  validateSignal(signal);
  if (signal?.aborted) throw new ConnectorError('ABORTED', 'Delivery cancelled before dispatch.', { ambiguous: false });
};

/** Atomic one-file-per-delivery output, with identical-content duplicate recognition. */
export function createFileConnector(options = {}) {
  keysOnly(options, ['name', 'revision', 'directory', 'overwrite', 'pretty', 'maxPayloadBytes'], 'File connector options');
  const { name = 'file', revision = '1', directory, overwrite = false, pretty = false,
    maxPayloadBytes = 8 * 1024 * 1024 } = options;
  assert(typeof directory === 'string' && directory.length > 0, 'A file destination directory is required.');
  assert(typeof overwrite === 'boolean' && typeof pretty === 'boolean', 'File flags must be boolean.');
  bytesOption(maxPayloadBytes, 'maxPayloadBytes');
  identifier(name, 'Connector name'); identifier(revision, 'Connector revision');
  const dir = resolve(directory);
  return Object.freeze({
    // Shared overwrite targets cannot suppress an uncertain earlier write after
    // another delivery changes that target. Only immutable output permits replay.
    name, revision, capabilities: caps('completed', !overwrite, true),
    async send(payload, context = {}) {
      const { target, data, digest } = deliveryPreflight(() => {
        cancelled(context.signal);
        if (context.destination !== undefined) keysOnly(context.destination, ['filename'], 'File destination');
        assert(typeof context.deliveryId === 'string' && context.deliveryId.length > 0, 'deliveryId is required.');
        const defaultName = /^[A-Za-z0-9_-]{1,160}$/.test(context.deliveryId) ? context.deliveryId
          : createHash('sha256').update(context.deliveryId).digest('hex');
        const filename = context.destination?.filename ?? `${defaultName}.json`;
        assert(typeof filename === 'string' && filename.length > 0 && filename.length <= 240 &&
          basename(filename) === filename && !/[\\/:*?"<>|\x00-\x1f]/.test(filename) && !['.', '..'].includes(filename),
        'File destination filename must be a plain filename.');
        const data = encoded(payload, maxPayloadBytes, pretty, context.encoding);
        return { target: resolve(dir, filename), data, digest: createHash('sha256').update(data).digest('hex') };
      });
      const receipt = duplicate => ({ status: 'acknowledged', receipt: { path: target, bytes: data.length,
        sha256: digest, duplicate }, completion: { status: 'completed', scope: 'file' } });
      const existing = async () => {
        let handle;
        try {
          if (!(await lstat(target)).isFile()) return false;
          handle = await open(target, 'r');
          const info = await handle.stat();
          if (!info.isFile() || info.size !== data.length) return false;
          const buffer = Buffer.alloc(data.length + 1);
          let size = 0;
          while (size < buffer.length) {
            const { bytesRead } = await handle.read(buffer, size, buffer.length - size, size);
            if (!bytesRead) break;
            size += bytesRead;
          }
          return size === data.length && buffer.subarray(0, size).equals(data);
        } catch (error) { if (error.code === 'ENOENT') return false; throw error; }
        finally { await handle?.close(); }
      };
      const temporary = stagingPath(target);
      let handle;
      let committed = false;
      let commitAttempted = false;
      try {
        await mkdir(dir, { recursive: true, mode: 0o700 });
        cancelled(context.signal);
        if (await existing()) return receipt(true);
        cancelled(context.signal);
        handle = await open(temporary, 'wx', 0o600);
        await handle.writeFile(data);
        await handle.sync();
        await handle.close();
        handle = undefined;
        cancelled(context.signal);
        try {
          commitAttempted = true;
          if (overwrite) await rename(temporary, target);
          else await link(temporary, target);
          committed = true;
        } catch (error) {
          if (error.code === 'EEXIST') {
            if (await existing()) return receipt(true);
            throw new ConnectorError('DELIVERY_CONFLICT', 'The destination file already contains different data.',
              { retryable: false, ambiguous: false });
          }
          throw error;
        }
        return receipt(false);
      } catch (error) {
        if (error instanceof ConnectorError) throw error;
        throw new ConnectorError('FILE_DELIVERY_ERROR', 'Cannot commit the delivery output file.',
          { retryable: true, ambiguous: committed || commitAttempted });
      } finally {
        await handle?.close().catch(() => {});
        await unlink(temporary).catch(() => {});
      }
    },
  });
}

function endpoint(value) {
  let url;
  try { url = new URL(value); } catch { throw new ConnectorError('CONFIGURATION_ERROR', 'A valid HTTP destination URL is required.'); }
  assert(!url.username && !url.password && !url.hash, 'HTTP URLs cannot contain user information or fragments.');
  const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname);
  assert(url.protocol === 'https:' || (url.protocol === 'http:' && loopback), 'HTTP destinations require HTTPS, except loopback.');
  return url;
}

function retryAfter(value) {
  if (!value) return undefined;
  const now = Date.now();
  // Persisted retry timestamps use RFC 3339's four-digit year range. An
  // unrepresentable delay is ignored, never shortened into an early retry.
  const latest = 253402300799999;
  if (/^\d+$/.test(value)) {
    const ms = Number(value) * 1000;
    return Number.isSafeInteger(ms) && ms <= latest - now ? ms : undefined;
  }
  const time = Date.parse(value);
  return Number.isFinite(time) && time <= latest ? Math.max(0, time - now) : undefined;
}

async function responseBody(response, maxBytes, signal) {
  if (!response.body) return null;
  const reader = response.body.getReader();
  const chunks = [];
  let size = 0;
  const abort = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener('abort', abort, { once: true });
  try {
    while (true) {
      if (signal.aborted) throw new ConnectorError('ABORTED', 'Response reading cancelled.');
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maxBytes) throw new ConnectorError('RESPONSE_TOO_LARGE', 'Delivery response exceeds its byte limit.');
      chunks.push(Buffer.from(value));
    }
    const text = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks));
    if (!text.trim()) return null;
    let value;
    try { value = JSON.parse(text); } catch { return text; }
    return snapshot(value);
  } finally {
    signal.removeEventListener('abort', abort);
    void reader.cancel().catch(() => {});
  }
}

/** One HTTP attempt. The runner, never this connector, owns retry scheduling. */
export function createHttpConnector(options = {}) {
  keysOnly(options, ['name', 'revision', 'url', 'headers', 'fetchImpl', 'timeoutMs', 'idempotent',
    'idempotencyHeader', 'maxPayloadBytes', 'maxResponseBytes', 'captureResponse', 'method'], 'HTTP connector options');
  const { name = 'http', revision = '1', url, headers = {}, fetchImpl = globalThis.fetch,
    timeoutMs = 30000, idempotent = false, idempotencyHeader = 'idempotency-key',
    maxPayloadBytes = 8 * 1024 * 1024, maxResponseBytes = 64 * 1024, captureResponse = false, method = 'POST' } = options;
  assert(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE'].includes(method), 'Invalid HTTP method.');
  const configured = endpoint(url);
  const baseHeaders = new Headers(headers);
  identifier(name, 'Connector name'); identifier(revision, 'Connector revision');
  assert(typeof fetchImpl === 'function', 'fetchImpl must be a function.');
  assert(typeof idempotent === 'boolean' && typeof captureResponse === 'boolean', 'HTTP flags must be boolean.');
  assert(typeof idempotencyHeader === 'string' && /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(idempotencyHeader),
    'idempotencyHeader must be a valid HTTP header name.');
  timeoutOption(timeoutMs);
  bytesOption(maxPayloadBytes, 'maxPayloadBytes');
  bytesOption(maxResponseBytes, 'maxResponseBytes');
  return Object.freeze({
    name, revision, capabilities: caps('accepted', idempotent),
    async send(payload, context = {}) {
      const { target, body, outgoing } = deliveryPreflight(() => {
        cancelled(context.signal);
        if (context.destination !== undefined) keysOnly(context.destination, ['url'], 'HTTP destination');
        const target = context.destination?.url === undefined ? configured : endpoint(context.destination.url);
        assert(target.origin === configured.origin, 'A connection cannot redirect configured HTTP credentials to another origin.');
        assert(!['GET', 'HEAD'].includes(method) || payload === null, 'GET and HEAD require a null payload.');
        const body = ['GET', 'HEAD'].includes(method) ? undefined : encoded(payload, maxPayloadBytes, false, context.encoding);
        const outgoing = new Headers(baseHeaders);
        if (context.headers !== undefined) for (const [key, value] of new Headers(context.headers)) outgoing.set(key, value);
        outgoing.set('content-type', context.encoding === 'text' ? 'text/plain; charset=utf-8' : context.encoding === 'jsonl' ? 'application/x-ndjson' : 'application/json');
        outgoing.set('accept', 'application/json');
        if (context.idempotencyKey !== undefined) outgoing.set(idempotencyHeader, String(context.idempotencyKey));
        if (context.deliveryId !== undefined) outgoing.set('x-decision-delivery-id', String(context.deliveryId));
        if (context.taskId !== undefined) outgoing.set('x-decision-task-id', String(context.taskId));
        return { target, body, outgoing };
      });
      let dispatched = false;
      let acknowledgement;
      try {
        return await bounded(async signal => {
          dispatched = true;
          const response = await fetchImpl(target.href, { method, headers: outgoing, body, signal, redirect: 'error' });
          assert(response instanceof Response, 'HTTP connector did not receive a Response.', 'INVALID_RESPONSE');
          if (!response.ok) {
            void response.body?.cancel().catch(() => {});
            const canRetry = [408, 425, 429, 500, 502, 503, 504, 529].includes(response.status);
            const retryAfterMs = retryAfter(response.headers.get('retry-after'));
            throw new ConnectorError('HTTP_DELIVERY_ERROR', `Destination returned HTTP ${response.status}.`, {
              status: response.status, retryable: canRetry, ambiguous: response.status === 408 || response.status >= 500,
              ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
            });
          }
          const requestId = response.headers.get('x-request-id');
          acknowledgement = { status: 'acknowledged', receipt: { httpStatus: response.status,
            ...(requestId ? { requestId: requestId.replace(/[\x00-\x1f\x7f]/g, '').slice(0, 200) } : {}) } };
          try { await context.onAcceptance?.(acknowledgement); }
          catch (error) { void response.body?.cancel().catch(() => {}); throw error; }
          if (captureResponse) {
            try { acknowledgement.receipt.response = await responseBody(response, maxResponseBytes, signal); }
            catch { acknowledgement.receipt.responseUnavailable = true; }
          } else void response.body?.cancel().catch(() => {});
          return acknowledgement;
        }, { signal: context.signal, timeoutMs, stage: 'delivery' });
      } catch (error) {
        // A received 2xx is an acceptance receipt even if reading its optional body fails.
        if (acknowledgement) return { ...acknowledgement, receipt: { ...acknowledgement.receipt, responseUnavailable: true } };
        if (error instanceof ConnectorError && error.ambiguous !== undefined) throw error;
        throw new ConnectorError(error instanceof ConnectorError ? error.code : 'NETWORK_ERROR',
          error instanceof ConnectorError ? error.message : 'HTTP delivery ended without a confirmed response.',
          { stage: 'delivery', retryable: true, ambiguous: dispatched });
      }
    },
  });
}

/** Decode the process protocol before treating a child exit as acceptance. */
function processReply(output) {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(output));
    const decoded = JSON.parse(text);
    if (isRecord(decoded) && ['acknowledged', 'submitted', 'unknown'].includes(decoded.status)) {
        const receipt = validateReceipt(decoded, MAX_BYTES);
        keysOnly(receipt, ['status', 'receipt', 'completion'], 'Process delivery receipt');
        return receipt;
    }
    const value = snapshot(decoded);
    return { status: 'acknowledged', receipt: { exitCode: 0, output: value, response: value },
        completion: { status: 'completed', scope: 'process' } };
}

function processAcceptance(receipt) {
    const completion = receipt.completion;
    const scope = completion?.scope;
    return { status: 'acknowledged', ...(completion && {
        completion: { status: completion.status, ...(typeof scope === 'string' && scope.length <= 256 ? { scope } : {}) },
    }) };
}

/** A configured executable, without a shell; stdout is one JSON reply and stderr is bounded diagnostics. */
export function createProcessConnector(options = {}) {
  keysOnly(options, ['name', 'revision', 'command', 'args', 'cwd', 'env', 'timeoutMs', 'killGraceMs',
    'maxInputBytes', 'maxOutputBytes', 'maxErrorBytes', 'idempotent', 'inputMode'], 'Process connector options');
  const { name = 'process', revision = '1', command, args = [], cwd, env,
    timeoutMs = 30000, killGraceMs = 250, maxInputBytes = 8 * 1024 * 1024,
    maxOutputBytes = 1024 * 1024, maxErrorBytes = 64 * 1024, idempotent = false, inputMode = 'envelope' } = options;
  assert(typeof command === 'string' && command.trim().length > 0 && !command.includes('\0'), 'A process executable is required.');
  assert(Array.isArray(args) && args.every(arg => typeof arg === 'string' && !arg.includes('\0')), 'Process args must be an array of strings.');
  assert(cwd === undefined || (typeof cwd === 'string' && cwd.length > 0), 'Process cwd must be a directory path.');
  assert(env === undefined || (isRecord(env) && Object.values(env).every(value => typeof value === 'string')),
    'Process env must map names to string values.');
  assert(inputMode === 'envelope' || inputMode === 'payload', 'Process inputMode must be envelope or payload.');
  assert(typeof idempotent === 'boolean', 'idempotent must be boolean.');
  identifier(name, 'Connector name'); identifier(revision, 'Connector revision');
  timeoutOption(timeoutMs);
  assert(Number.isSafeInteger(killGraceMs) && killGraceMs >= 0 && killGraceMs <= 60000, 'killGraceMs must be 0–60000.');
  for (const [key, value] of Object.entries({ maxInputBytes, maxOutputBytes, maxErrorBytes })) bytesOption(value, key);
  const argv = [...args];
  const environment = env === undefined ? undefined : { ...env };
  const active = new Set();
  let closed = false;
  return Object.freeze({
    name, revision, capabilities: caps('completed', idempotent, true),
    async send(payload, context = {}) {
      const input = deliveryPreflight(() => {
        assert(!closed, 'The process connector is closed.', 'CONNECTOR_CLOSED');
        cancelled(context.signal);
        assert(context.destination === undefined, 'Process destinations are configured through command/args, not per-delivery overrides.');
        const metadata = Object.fromEntries(['deliveryId', 'attemptId', 'taskId', 'eventId', 'idempotencyKey']
          .filter(key => context[key] !== undefined).map(key => [key, context[key]]));
        return encoded(inputMode === 'payload' ? payload : { schemaVersion: 3, payload, ...metadata }, maxInputBytes,
          false, inputMode === 'payload' ? context.encoding : 'json', inputMode === 'envelope');
      });
      let stop;
      const promise = new Promise((resolvePromise, reject) => {
        let child;
        let spawned = false;
        let settled = false;
        let terminal;
        let acceptance;
        let killTimer;
        let timer;
        let size = 0;
        let errorSize = 0;
        const output = [];
        const finish = (error, result) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          clearTimeout(killTimer);
          context.signal?.removeEventListener('abort', abort);
          if (error) reject(error); else resolvePromise(result);
        };
        const kill = signal => {
          try {
            if (child?.pid && process.platform !== 'win32') process.kill(-child.pid, signal);
            else child?.kill(signal);
          } catch { /* Exit can race termination. */ }
        };
        stop = (code = 'ABORTED') => {
          if (settled || terminal) return;
          if (acceptance) { finish(null, incompleteReceipt(acceptance)); return; }
          terminal = new ConnectorError(code, code === 'TIMEOUT' ? 'Configured process exceeded its deadline.' :
            code === 'OUTPUT_TOO_LARGE' ? 'Configured process exceeded its output byte limit.' :
              'Configured process was cancelled.', { retryable: false, ambiguous: spawned });
          kill('SIGTERM');
          killTimer = setTimeout(() => {
            kill('SIGKILL');
            child?.stdin?.destroy();
            child?.stdout?.destroy();
            child?.stderr?.destroy();
            finish(terminal);
          }, killGraceMs);
        };
        const abort = () => stop();
        context.signal?.addEventListener('abort', abort, { once: true });
        timer = setTimeout(() => stop('TIMEOUT'), timeoutMs);
        try {
          child = spawn(command, argv, { cwd, env: environment, shell: false, windowsHide: true,
            detached: process.platform !== 'win32', stdio: ['pipe', 'pipe', 'pipe'] });
        } catch { finish(new ConnectorError('PROCESS_START_ERROR', 'Cannot start the configured executable.',
          { retryable: false, ambiguous: false })); return; }
        child.once('spawn', () => { spawned = true; if (terminal) { terminal.ambiguous = true; kill('SIGTERM'); } });
        child.once('error', () => finish(new ConnectorError('PROCESS_START_ERROR', 'Cannot run the configured executable.',
          { retryable: false, ambiguous: spawned })));
        child.stdout.on('data', chunk => {
          size += chunk.length;
          if (size > maxOutputBytes) stop('OUTPUT_TOO_LARGE');
          else if (!terminal) output.push(chunk);
        });
        child.stderr.on('data', chunk => { errorSize += chunk.length; if (errorSize > maxErrorBytes) stop('OUTPUT_TOO_LARGE'); });
        // A child may acknowledge and exit before consuming all input. Validate its
        // exit and complete reply on close; the deadline still bounds a stalled child.
        child.stdin.on('error', error => { if (!['EPIPE', 'EOF'].includes(error.code) && !settled && !terminal) stop('PROCESS_INPUT_ERROR'); });
        child.once('close', (code, exitSignal) => {
          if (terminal) { finish(terminal); return; }
          if (code !== 0 || exitSignal) {
            finish(new ConnectorError('PROCESS_EXIT_ERROR', `Configured process exited unsuccessfully (${code ?? exitSignal}).`,
              { retryable: false, ambiguous: spawned }));
            return;
          }
          let receipt;
          try { receipt = processReply(output); }
          catch {
            finish(new ConnectorError('INVALID_RESPONSE', 'Configured process did not return one valid JSON reply.',
              { retryable: false, ambiguous: spawned }));
            return;
          }
          if (receipt.status !== 'acknowledged' || !context.onAcceptance) { finish(null, receipt); return; }
          acceptance = processAcceptance(receipt);
          // The child has exited. A stalled checkpoint hook cannot erase its reply.
          void Promise.resolve().then(() => context.onAcceptance(acceptance)).then(
            () => finish(null, receipt), () => finish(null, incompleteReceipt(acceptance)));
        });
        if (context.signal?.aborted) stop();
        if (!terminal) child.stdin.end(input);
      });
      const pending = { promise, stop: code => stop(code) };
      active.add(pending);
      try { return await promise; } finally { active.delete(pending); }
    },
    async close() {
      closed = true;
      const pending = [...active];
      for (const entry of pending) entry.stop('CONNECTOR_CLOSED');
      await Promise.allSettled(pending.map(entry => entry.promise));
    },
  });
}
