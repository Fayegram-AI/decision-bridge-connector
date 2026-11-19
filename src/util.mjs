import { DecisionError, publicError as bridgePublicError } from '@fayegram-ai/decision-bridge';

export class ConnectorError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'ConnectorError';
    this.code = code;
    for (const key of ['stage', 'retryable', 'ambiguous', 'status', 'retryAfterMs', 'inputInvalid']) {
      if (details[key] !== undefined) this[key] = details[key];
    }
  }
}

export function assert(condition, message, code = 'CONFIGURATION_ERROR') {
  if (!condition) throw new ConnectorError(code, message);
}

export function isRecord(value) {
  return value !== null && typeof value === 'object' &&
    [Object.prototype, null].includes(Object.getPrototypeOf(value));
}

export function snapshot(value, { maxBytes = 64 * 1024 * 1024, rootValueKeys, valueDepthPath } = {}) {
  assert(Number.isSafeInteger(maxBytes) && maxBytes > 0 && maxBytes <= 64 * 1024 * 1024,
    'JSON byte limit must be a positive integer through 64 MiB.');
  const ancestors = new Set();
  const stringBytes = new Map();
  let nodes = 0; let bytes = 0;
  function charge(count) {
    bytes += count;
    assert(bytes <= maxBytes, 'JSON byte limit exceeded.', 'RESOURCE_LIMIT');
  }
  function quotedBytes(text) {
    if (stringBytes.has(text)) return stringBytes.get(text);
    assert(text.length + 2 <= maxBytes - bytes, 'JSON byte limit exceeded.', 'RESOURCE_LIMIT');
    let length = 2;
    for (let index = 0; index < text.length; index++) {
      const code = text.charCodeAt(index);
      if (code === 34 || code === 92) length += 2;
      else if (code < 32) length += [8, 9, 10, 12, 13].includes(code) ? 2 : 6;
      else if (code < 128) length++;
      else if (code < 2048) length += 2;
      else if (code >= 0xd800 && code <= 0xdbff && index + 1 < text.length &&
        text.charCodeAt(index + 1) >= 0xdc00 && text.charCodeAt(index + 1) <= 0xdfff) { length += 4; index++; }
      else if (code >= 0xd800 && code <= 0xdfff) length += 6;
      else length += 3;
      assert(length <= maxBytes - bytes, 'JSON byte limit exceeded.', 'RESOURCE_LIMIT');
    }
    stringBytes.set(text, length);
    return length;
  }
  function copy(item, depth, root = false, path = -1) {
    assert(++nodes <= 500000 && depth <= 100, 'JSON resource limit exceeded.', 'RESOURCE_LIMIT');
    if (item === null) { charge(4); return item; }
    if (typeof item === 'string') { charge(quotedBytes(item)); return item; }
    if (typeof item === 'boolean') { charge(item ? 4 : 5); return item; }
    if (typeof item === 'number') {
      assert(Number.isFinite(item), 'JSON numbers must be finite.', 'VALIDATION_ERROR');
      charge(String(item).length);
      return item;
    }
    assert(Array.isArray(item) || isRecord(item), 'Use plain JSON values.', 'VALIDATION_ERROR');
    assert(!ancestors.has(item), 'Cyclic JSON is not supported.', 'VALIDATION_ERROR');
    assert(Object.getOwnPropertySymbols(item).length === 0, 'JSON symbol keys are not supported.', 'VALIDATION_ERROR');
    const descriptors = Object.getOwnPropertyDescriptors(item);
    for (const [key, descriptor] of Object.entries(descriptors)) {
      if (Array.isArray(item) && key === 'length') continue;
      assert(descriptor.enumerable && 'value' in descriptor, 'JSON accessors are not supported.', 'VALIDATION_ERROR');
    }
    ancestors.add(item);
    let out;
    if (Array.isArray(item)) {
      const keys = Object.keys(item);
      assert(keys.length === item.length && keys.every((key, index) => key === String(index)),
        'JSON arrays must be dense and have no extra fields.', 'VALIDATION_ERROR');
      charge(2 + Math.max(0, item.length - 1));
      out = item.map(child => copy(child, depth + 1));
    } else {
      const entries = Object.entries(descriptors);
      charge(2 + Math.max(0, entries.length - 1));
      out = Object.fromEntries(entries.map(([key, descriptor]) => {
        charge(quotedBytes(key) + 1);
        const next = path >= 0 && path < valueDepthPath.length &&
          (valueDepthPath[path] === null || valueDepthPath[path] === key) ? path + 1 : -1;
        return [key, copy(descriptor.value, (root && rootValueKeys?.has(key)) ||
          next >= 0 && next === valueDepthPath.length ? 0 : depth + 1, false, next)];
      }));
    }
    ancestors.delete(item);
    return out;
  }
  return copy(value, 0, true, valueDepthPath ? 0 : -1);
}

export function freeze(value) {
  if ((Array.isArray(value) || isRecord(value)) && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}

/** Only compose JSON already captured by its application/DTO boundary. */
export function stringifyCaptured(value) {
  const parts = []; const sizes = new Map(); const encoder = new TextEncoder(); let current = ''; let bytes = 0;
  function write(token) {
    assert(typeof token === 'string', 'Captured JSON is invalid.', 'VALIDATION_ERROR');
    let size = sizes.get(token);
    if (size === undefined) {
      size = /^[\x00-\x7f]*$/.test(token) ? token.length : encoder.encode(token).length;
      // Unique scalar sequences must not retain a second unbounded token index.
      if (sizes.size < 1024 && token.length <= 256) sizes.set(token, size);
    }
    bytes += size;
    assert(bytes <= 67108864, 'JSON byte limit exceeded.', 'RESOURCE_LIMIT');
    current += token;
    if (current.length >= 65536) { parts.push(current); current = ''; }
  }
  function visit(item) {
    if (Array.isArray(item)) {
      write('['); item.forEach((child, index) => { if (index) write(','); visit(child); }); write(']');
    } else if (isRecord(item)) {
      // Match the original sorted-object serializer, including JavaScript's numeric-key order.
      const keys = Object.keys(Object.fromEntries(Object.keys(item).sort().map(key => [key, null])));
      write('{'); keys.forEach((key, index) => {
        if (index) write(','); write(JSON.stringify(key)); write(':'); visit(item[key]);
      }); write('}');
    } else write(JSON.stringify(item));
  }
  visit(value); if (current) parts.push(current);
  return parts.join('');
}

export function stableStringify(value) {
  // Individual values retain the native serializer; generated DTOs use the bounded writer above.
  const sorted = item => Array.isArray(item) ? item.map(sorted) : isRecord(item)
    ? Object.fromEntries(Object.keys(item).sort().map(key => [key, sorted(item[key])])) : item;
  return JSON.stringify(sorted(snapshot(value)));
}

/** Preserve canonical identities without imposing an application limit on generated envelopes. */
export async function fingerprintCaptured(value) {
  const data = new TextEncoder().encode(stringifyCaptured(value));
  const bytes = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', data));
  return [...bytes].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

export async function fingerprint(value) {
  const data = new TextEncoder().encode(stableStringify(value));
  const bytes = new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', data));
  return [...bytes].map(byte => byte.toString(16).padStart(2, '0')).join('');
}

export function id(prefix = 'id') { return `${prefix}_${globalThis.crypto.randomUUID()}`; }

export function identifier(value, label = 'Identifier') {
  assert(typeof value === 'string' && value.length > 0 && value.length <= 256 && !/[\x00-\x1f\x7f]/.test(value),
    `${label} must be a nonempty printable string of at most 256 characters.`);
  return value;
}

/** Worker placement is an exact set of labels, independent of caller-owned arrays. */
export function placementLabels(value = []) {
  assert(Array.isArray(value), 'Worker placements must be an array of labels.');
  const labels = snapshot(value);
  labels.forEach(label => identifier(label, 'Placement label'));
  assert(new Set(labels).size === labels.length, 'Worker placement labels must be unique.');
  return freeze(labels);
}

/** Validate authority-bearing runner settings before constructing owned resources. */
export function runnerIdentity({ namespace = 'default', workerId, placements } = {}) {
  return Object.freeze({ namespace: identifier(namespace, 'Namespace'), placements: placementLabels(placements),
    ...(workerId === undefined ? {} : { workerId: identifier(workerId, 'Worker ID') }) });
}

export function publicError(error, stage) {
  try {
    let out;
    if (error instanceof DecisionError) out = bridgePublicError(error);
    else if (error instanceof ConnectorError) {
      out = { code: error.code, message: error.message };
      for (const key of ['stage', 'retryable', 'ambiguous', 'status', 'retryAfterMs', 'inputInvalid']) {
        if (error[key] !== undefined) out[key] = error[key];
      }
    }
    else out = { code: 'UNEXPECTED_ERROR', message: 'Operation failed.' };
    assert(typeof out.code === 'string' && out.code.length > 0 && out.code.length <= 256 && typeof out.message === 'string', 'Invalid public error.');
    for (const key of ['retryable', 'ambiguous', 'inputInvalid']) assert(out[key] === undefined || typeof out[key] === 'boolean', 'Invalid error disposition.');
    assert(out.status === undefined || Number.isSafeInteger(out.status), 'Invalid error status.');
    assert(out.retryAfterMs === undefined || Number.isSafeInteger(out.retryAfterMs) && out.retryAfterMs >= 0, 'Invalid retry delay.');
    return snapshot({ ...out, ...(stage ? { stage } : {}) });
  } catch {
    return { code: 'UNEXPECTED_ERROR', message: 'Operation failed.', ...(typeof stage === 'string' ? { stage } : {}) };
  }
}

/** Only synchronous validation/encoding belongs here, before any recipient I/O. */
export function deliveryPreflight(operation) {
  try { return operation(); }
  catch (error) {
    const failure = publicError(error, 'delivery');
    throw new ConnectorError(failure.code, failure.message, { ...failure, ambiguous: false, retryable: false });
  }
}

/** Only known validation failures at the admission boundary classify poison input. */
export function invalidInput(error) {
  const failure = publicError(error);
  if (['VALIDATION_ERROR', 'CONFIGURATION_ERROR', 'RESOURCE_LIMIT', 'MAPPING_MISSING_PATH', 'UNSUPPORTED_CAPABILITY',
    'INVALID_RESULT', 'INVALID_SAVED_OUTPUT', 'INVALID_SAVED_RESULT', 'INVALID_RESPONSE'].includes(failure.code) &&
    failure.retryable !== true && failure.ambiguous !== true)
    return new ConnectorError(failure.code, failure.message, { ...failure, inputInvalid: true, retryable: false, ambiguous: false });
  return error;
}

export function getPath(value, path) {
  assert(typeof path === 'string', 'A projection path must be a string.');
  if (path === '' || path === '$') return value;
  const parts = path.startsWith('/') ? path.slice(1).split('/').map(x => x.replace(/~1/g, '/').replace(/~0/g, '~'))
    : path.replace(/^\$\./, '').split('.');
  let current = value;
  for (const part of parts) {
    assert(!['__proto__', 'prototype', 'constructor'].includes(part), 'Unsafe property path.');
    if (current === null || typeof current !== 'object' || !Object.hasOwn(current, part)) return undefined;
    const descriptor = Object.getOwnPropertyDescriptor(current, part);
    assert(descriptor && 'value' in descriptor, 'Property accessors are not supported.');
    current = descriptor.value;
  }
  return current;
}

export function validateSignal(signal) {
  if (signal === undefined) return;
  let valid = false;
  try {
    Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted').get.call(signal);
    valid = typeof signal.addEventListener === 'function' && typeof signal.removeEventListener === 'function';
  } catch { /* Reject lookalikes before admission or external effects. */ }
  assert(valid, 'signal must be an AbortSignal.');
}

export function abortError(stage) { return new ConnectorError('ABORTED', 'Operation cancelled.', { stage }); }

export async function bounded(fn, { signal, timeoutMs = 60000, stage } = {}) {
  validateSignal(signal);
  assert(Number.isSafeInteger(timeoutMs) && timeoutMs > 0 && timeoutMs <= 2147483647, 'timeoutMs must be a positive 32-bit integer.');
  if (signal?.aborted) throw abortError(stage);
  const controller = new AbortController();
  const started = performance.now();
  let rejectBound;
  const boundary = new Promise((_resolve, reject) => { rejectBound = reject; });
  const cancel = () => { controller.abort(); rejectBound(abortError(stage)); };
  signal?.addEventListener('abort', cancel, { once: true });
  const timer = setTimeout(() => {
    controller.abort();
    rejectBound(new ConnectorError('TIMEOUT', 'Operation deadline exceeded.', { stage }));
  }, timeoutMs);
  try {
    const result = await Promise.race([boundary, Promise.resolve().then(() => {
      if (controller.signal.aborted) throw abortError(stage);
      return fn(controller.signal);
    })]);
    if (signal?.aborted) throw abortError(stage);
    if (performance.now() - started >= timeoutMs) {
      controller.abort();
      throw new ConnectorError('TIMEOUT', 'Operation deadline exceeded.', { stage });
    }
    return result;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener('abort', cancel);
  }
}

export function delay(ms, signal) {
  validateSignal(signal);
  if (signal?.aborted) return Promise.reject(abortError());
  return new Promise((resolve, reject) => {
    const cancel = () => { clearTimeout(timer); signal?.removeEventListener('abort', cancel); reject(abortError()); };
    const timer = setTimeout(() => { signal?.removeEventListener('abort', cancel); resolve(); }, ms);
    signal?.addEventListener('abort', cancel, { once: true });
  });
}

export function keysOnly(value, keys, label = 'Configuration') {
  assert(isRecord(value), `${label} must be an object.`);
  assert(Object.keys(value).every(key => keys.includes(key)), `${label} contains an unknown field.`);
}

/** Capture DTO fields independently; envelope structure is not an application value. */
export function captureObject(value, largeKeys = [], maxBytes = 1048576) {
  assert(isRecord(value) && Object.getOwnPropertySymbols(value).length === 0, 'Use a plain JSON object.', 'VALIDATION_ERROR');
  const descriptors = Object.getOwnPropertyDescriptors(value);
  assert(Object.values(descriptors).every(item => item.enumerable && 'value' in item), 'JSON accessors are not supported.', 'VALIDATION_ERROR');
  return Object.fromEntries(Object.entries(descriptors).map(([name, descriptor]) =>
    [name, snapshot(descriptor.value, { maxBytes: largeKeys.includes(name) ? maxBytes : 65536 })]));
}

/** Bind independently captured fields without serializing their combined copies. */
export async function fingerprintFields(value, capturedKeys = []) {
  const parts = [];
  for (const name of Object.keys(value).sort()) parts.push([name,
    await (capturedKeys.includes(name) ? fingerprintCaptured : fingerprint)(value[name])]);
  return fingerprint(parts);
}
