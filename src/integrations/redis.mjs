import { ConnectorError, abortError, assert, bounded, delay, deliveryPreflight, fingerprint, freeze, identifier, isRecord, keysOnly,
  publicError, snapshot, validateSignal } from '../util.mjs';

const utf8 = new TextDecoder('utf-8', { fatal: true });
function text(value) {
  if (typeof value === 'string') return value;
  if (value instanceof Uint8Array) return utf8.decode(value);
  throw new ConnectorError('INVALID_RESPONSE', 'Redis returned an invalid string.');
}
function positive(value, label, maximum = 2147483647) {
  assert(Number.isSafeInteger(value) && value > 0 && value <= maximum, `Invalid ${label}.`);
}
function clientRequired(client) { assert(client && typeof client.sendCommand === 'function', 'A connected Redis client with sendCommand is required.'); }
function streamId(value) {
  const id = text(value);
  assert(/^\d+-\d+$/.test(id), 'Redis returned an invalid stream ID.', 'INVALID_RESPONSE');
  return id;
}
function knownRejection(error) {
  return ['SimpleError', 'BlobError', 'ErrorReply'].includes(error?.constructor?.name);
}
function redisFailure(error, dispatched) {
  if (!dispatched && error instanceof ConnectorError) return error;
  if (knownRejection(error)) return new ConnectorError('REDIS_REJECTED', 'Redis rejected the command.', {
    retryable: /^(LOADING|TRYAGAIN|CLUSTERDOWN|MASTERDOWN|BUSY)\b/.test(error.message ?? ''), ambiguous: false,
  });
  return new ConnectorError(error?.code === 'ABORTED' ? 'ABORTED' : error?.code === 'TIMEOUT' ? 'TIMEOUT' : 'REDIS_TRANSPORT_ERROR',
    'The Redis command outcome is unknown.', { retryable: true, ambiguous: dispatched });
}
async function command(client, args, { signal, timeoutMs = 5000 } = {}) {
  let dispatched = false;
  try {
    return await bounded(child => {
      if (child.aborted) throw abortError('redis');
      dispatched = true;
      // node-redis can remove queued commands on abort. Already sent commands remain ambiguous.
      return client.sendCommand(args, { abortSignal: child, timeout: timeoutMs });
    }, { signal, timeoutMs, stage: 'redis' });
  } catch (error) { throw redisFailure(error, dispatched); }
}
async function closeOwned(client, timeoutMs) {
  if (client.isOpen === false) return;
  if (typeof client.close === 'function') {
    try { await bounded(() => client.close(), { timeoutMs, stage: 'redis-close' }); return; }
    catch { if (typeof client.destroy !== 'function') throw new ConnectorError('REDIS_CLOSE_FAILED', 'Could not close the owned Redis connection.'); }
  }
  try {
    if (typeof client.destroy === 'function') { client.destroy(); return; }
    if (typeof client.quit === 'function') { await bounded(() => client.quit(), { timeoutMs, stage: 'redis-close' }); return; }
  } catch { throw new ConnectorError('REDIS_CLOSE_FAILED', 'Could not close the owned Redis connection.'); }
  throw new ConnectorError('CONFIGURATION_ERROR', 'An owned Redis client requires close, destroy, or quit.');
}

function validateOwnership(client, ownsClient) {
  assert(typeof ownsClient === 'boolean', 'ownsClient must be boolean.');
  assert(!ownsClient || ['close', 'destroy', 'quit'].some(method => typeof client[method] === 'function'),
    'An owned Redis client requires close, destroy, or quit.');
}

/** XADD acknowledgement means Redis accepted an entry, not that a consumer acted on it. */
export function createRedisStreamConnector({ client, stream, name = 'redis-stream', revision = '1', field = 'payload',
  timeoutMs = 5000, maxPayloadBytes = 8 * 1024 * 1024, ownsClient = false } = {}) {
  clientRequired(client); identifier(stream, 'Redis stream'); identifier(name, 'Connector name'); identifier(revision, 'Connector revision'); identifier(field, 'Payload field');
  assert(!['delivery_id', 'attempt_id', 'task_id', 'event_id', 'idempotency_key'].includes(field), 'The payload field conflicts with delivery metadata.');
  positive(timeoutMs, 'Redis timeout'); positive(maxPayloadBytes, 'Redis payload limit', 64 * 1024 * 1024);
  validateOwnership(client, ownsClient);
  let closed = false; let closing;
  return Object.freeze({ name, revision, capabilities: freeze({ direction: 'outgoing', runtime: 'node',
    acknowledgement: 'accepted', idempotent: false, ordered: true }),
  async send(payload, context = {}) {
    const { target, args } = deliveryPreflight(() => {
      assert(!closed, 'Redis connector is closed.', 'CONNECTION_CLOSED');
      validateSignal(context.signal);
      if (context.signal?.aborted) throw abortError('delivery');
      const destination = context.destination ?? {};
      keysOnly(destination, ['stream'], 'Redis destination');
      const target = destination.stream === undefined ? stream : identifier(destination.stream, 'Destination stream');
      const encoded = JSON.stringify(snapshot(payload, { maxBytes: maxPayloadBytes }));
      const args = ['XADD', target, '*', field, encoded];
      for (const [key, wire] of Object.entries({ deliveryId: 'delivery_id', attemptId: 'attempt_id', taskId: 'task_id', eventId: 'event_id', idempotencyKey: 'idempotency_key' })) {
        if (context[key] !== undefined) args.push(wire, identifier(context[key], key));
      }
      return { target, args };
    });
    const response = await command(client, args, { signal: context.signal, timeoutMs });
    let entryId;
    try { entryId = streamId(response); }
    catch { throw new ConnectorError('INVALID_RESPONSE', 'Redis did not return a valid append receipt.', { ambiguous: true, retryable: false }); }
    return freeze({ status: 'acknowledged', receipt: { stream: target, entryId, acknowledgement: 'accepted' } });
  },
  close() { if (closing) return closing; closed = true;
    closing = ownsClient ? closeOwned(client, timeoutMs) : Promise.resolve(); return closing; },
  });
}

export { createRedisSource } from './redis-source.mjs';
