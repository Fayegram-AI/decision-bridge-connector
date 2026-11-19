import { ConnectorError, abortError, assert, bounded, freeze, identifier, isRecord, publicError,
  snapshot, validateSignal } from './util.mjs';
import { captureEnvelope, captureReceiptBody, captureSubmission } from './engine/input-capture.mjs';
import { acknowledgementFailure, replyFailure } from './engine/receipts.mjs';
import { SOURCE_BINDING, validateSourceBinding } from './source-binding.mjs';

function optionsObject(value, keys, label) {
  assert(isRecord(value) && Object.getOwnPropertySymbols(value).length === 0, `${label} must be a plain object.`);
  const descriptors = Object.getOwnPropertyDescriptors(value);
  assert(Object.entries(descriptors).every(([key, descriptor]) => keys.includes(key) && descriptor.enumerable && 'value' in descriptor),
    `${label} contains an unknown field or accessor.`);
  return Object.fromEntries(Object.entries(descriptors).map(([key, descriptor]) => [key, descriptor.value]));
}

function duration(value, label) {
  assert(Number.isSafeInteger(value) && value > 0 && value <= 2147483647, `${label} must be a positive 32-bit integer.`);
  return value;
}

/** Wrap a trusted iterable in an explicitly named, single-consumer incoming connector. */
export function createIterableSource(iterable, options = {}) {
  const { name, revision = '1', runtime = 'portable' } = optionsObject(options, ['name', 'revision', 'runtime'], 'Iterable source options');
  identifier(name, 'Incoming source name'); identifier(revision, 'Incoming source revision');
  assert(['portable', 'node'].includes(runtime), 'Unknown incoming runtime.');
  assert(iterable && (typeof iterable[Symbol.asyncIterator] === 'function' || typeof iterable[Symbol.iterator] === 'function'),
    'An async iterable or iterable of incoming messages is required.');
  let iterator;
  let opened = false;
  let ended = false;
  let closed = false;
  let closing;

  function beginClose() {
    if (closing) return closing;
    closed = true;
    closing = Promise.resolve().then(() => {
      if (!ended && iterator && typeof iterator.return === 'function') return iterator.return();
    });
    return closing;
  }

  async function close(options = {}) {
    const { signal, timeoutMs = 5000 } = optionsObject(options, ['signal', 'timeoutMs'], 'Source close options');
    validateSignal(signal); duration(timeoutMs, 'Source close timeout');
    await bounded(() => beginClose(), { signal, timeoutMs, stage: 'source-close' });
  }

  return Object.freeze({ name, revision,
    capabilities: freeze({ direction: 'incoming', runtime, acknowledgement: 'per-message', backpressure: 'pull', replayable: false }),
    open(options = {}) {
      const { signal } = optionsObject(options, ['signal'], 'Source open options');
      validateSignal(signal);
      assert(!closed, 'Incoming source is closed.', 'SOURCE_CLOSED');
      assert(!opened, 'An iterable source supports one consumer.', 'SOURCE_BUSY');
      if (signal?.aborted) throw abortError('source');
      const method = iterable[Symbol.asyncIterator] ?? iterable[Symbol.iterator];
      iterator = method.call(iterable);
      assert(iterator && typeof iterator.next === 'function', 'Source did not create an iterator.', 'INVALID_SOURCE');
      opened = true;
      return {
        [Symbol.asyncIterator]() { return this; },
        async next() {
          if (signal?.aborted) throw abortError('source');
          if (closed || ended) return { done: true, value: undefined };
          const step = await iterator.next();
          if (step?.done === true) ended = true;
          return step;
        },
        async return() { await beginClose(); return { done: true, value: undefined }; },
      };
    },
    close,
  });
}

/** Pull messages with bounded concurrency; task execution and original replies stay in the runner. */
export async function consumeSource(runner, source, options = {}) {
  const { acknowledge = 'completed', concurrency = 4, signal, onOutcome, observerTimeoutMs = 1000,
    processingTimeoutMs = 60000, sourceTimeoutMs = 60000, acknowledgementTimeoutMs = 5000, replyTimeoutMs = 10000, closeTimeoutMs = 5000 } =
    optionsObject(options, ['acknowledge', 'concurrency', 'signal', 'onOutcome', 'observerTimeoutMs', 'processingTimeoutMs',
      'sourceTimeoutMs', 'acknowledgementTimeoutMs', 'replyTimeoutMs', 'closeTimeoutMs'], 'Source consumption options');
  assert(runner && typeof runner.submit === 'function' && typeof runner.execute === 'function', 'Source consumption requires a runner.');
  assert(source && typeof source.open === 'function', 'Incoming source requires open().');
  identifier(source.name, 'Incoming source name'); identifier(source.revision, 'Incoming source revision');
  const capabilities = snapshot(source.capabilities);
  assert(isRecord(capabilities) && ['incoming', 'both'].includes(capabilities.direction), 'Connector does not support incoming tasks.');
  assert(['portable', 'node'].includes(capabilities.runtime), 'Incoming connector must declare its runtime.');
  assert(['none', 'explicit', 'per-message'].includes(capabilities.acknowledgement), 'Incoming connector must declare its acknowledgement behavior.');
  assert(['completed', 'admitted', 'none'].includes(acknowledge), 'Unknown incoming acknowledgement milestone.');
  assert(acknowledge !== 'admitted' || runner.capabilities?.durable === true, 'Admission acknowledgement requires a durable task store.');
  assert(Number.isSafeInteger(concurrency) && concurrency >= 1 && concurrency <= 64, 'Incoming concurrency must be 1–64.');
  for (const [label, value] of Object.entries({ observerTimeoutMs, processingTimeoutMs, sourceTimeoutMs, acknowledgementTimeoutMs, replyTimeoutMs, closeTimeoutMs })) duration(value, label);
  validateSignal(signal);
  assert(onOutcome === undefined || typeof onOutcome === 'function', 'onOutcome must be a function.');
  assert(source.close === undefined || typeof source.close === 'function', 'Source close must be a function.');
  if (source[SOURCE_BINDING]) {
    assert(!source[SOURCE_BINDING].durableAdmission || runner.capabilities?.durable === true,
      'This source requires durable admission.');
    validateSourceBinding(source[SOURCE_BINDING], await runner.ready);
  }

  const counts = { received: 0, admitted: 0, completed: 0, failed: 0, unknown: 0, cancelled: 0,
    acknowledged: 0, unacknowledged: 0, acknowledgementFailed: 0, acknowledgementUnknown: 0,
    rejected: 0, rejectionFailed: 0, observerFailures: 0, sourceErrors: 0, closeErrors: 0 };
  const errors = [];
  const active = new Set();
  let iterator;
  let exhausted = false;
  let stopped = 'exhausted';
  const lifecycle = new AbortController();
  const combined = signal ? AbortSignal.any([signal, lifecycle.signal]) : lifecycle.signal;

  async function observe(outcome) {
    if (!onOutcome) return;
    try {
      await bounded(child => onOutcome(freeze(outcome), { signal: child }), { timeoutMs: observerTimeoutMs, stage: 'source-observer' });
    } catch { counts.observerFailures++; }
  }

  function remember(error) {
    if (errors.length < 100) errors.push(error);
  }

  async function handle(message, index) {
    let envelope;
    let taskId;
    let report;
    let error;
    let phase = 'source-message';
    let dispatched = false;
    let acknowledgement = { status: acknowledge === 'none' ? 'not_requested' : 'not_available' };
    let rejection;

    const context = child => Object.freeze({ source: source.name, ...(taskId ? { taskId } : {}),
      ...(report ? { report } : {}), milestone: acknowledge, signal: child });

    async function acknowledgeMessage() {
      if (acknowledge === 'none' || !envelope.acknowledge) return;
      if (combined.aborted) { acknowledgement = { status: 'not_requested' }; return; }
      let called = false;
      let externalAcknowledgement;
      try {
        const invoke = async child => {
          called = true;
          try {
            const receipt = await envelope.acknowledge.call(message, context(child));
            const value = receipt === undefined ? {} : captureEnvelope(receipt, {
              receipt: body => captureReceiptBody(body, runner.store?.limits?.maxBytes),
            });
            assert(value.status === undefined || ['acknowledged', 'submitted', 'unknown', 'failed'].includes(value.status),
              'Incoming acknowledgement returned an invalid receipt.', 'INVALID_RECEIPT');
            externalAcknowledgement = value.status === 'submitted' ? 'unknown' : value.status ?? 'acknowledged';
            return { ...value, status: externalAcknowledgement };
          } catch (error) {
            externalAcknowledgement = publicError(error).ambiguous === false ? 'failed' : 'unknown';
            throw error;
          }
        };
        // Durable sources use the same write-ahead attempt and recovery path as hosts.
        // Volatile runners still support acknowledgement after completed processing.
        acknowledgement = await bounded(async child => runner.capabilities?.durable === true
          ? runner.acknowledge(taskId, ({ signal: ackSignal }) => invoke(ackSignal), { signal: child, timeoutMs: acknowledgementTimeoutMs })
          : invoke(child), { signal: combined, timeoutMs: acknowledgementTimeoutMs, stage: 'source-acknowledgement' });
        if (acknowledgement.status === 'acknowledged') counts.acknowledged++;
        else if (acknowledgement.status === 'unknown') counts.acknowledgementUnknown++;
        else counts.acknowledgementFailed++;
      } catch (cause) {
        const failure = publicError(cause, 'source-acknowledgement');
        // Storage certainty does not establish whether the external ACK took effect.
        failure.ambiguous = called && externalAcknowledgement !== 'failed';
        acknowledgement = { status: failure.ambiguous ? 'unknown' : 'failed', error: failure };
        if (failure.ambiguous) counts.acknowledgementUnknown++;
        else counts.acknowledgementFailed++;
      }
      const failure = acknowledgementFailure(acknowledgement);
      if (failure) acknowledgement = { ...acknowledgement, error: publicError(failure) };
    }

    try {
      envelope = optionsObject(message, ['input', 'reply', 'acknowledge', 'reject'], 'Incoming message');
      assert(Object.hasOwn(envelope, 'input'), 'Incoming message requires an input.', 'INVALID_SOURCE_MESSAGE');
      for (const method of ['reply', 'acknowledge', 'reject']) assert(envelope[method] === undefined || typeof envelope[method] === 'function', `Incoming ${method} must be a callback.`, 'INVALID_SOURCE_MESSAGE');
      assert(capabilities.acknowledgement !== 'explicit' || typeof envelope.acknowledge === 'function',
        'This incoming connector requires an acknowledgement callback on each message.', 'SOURCE_CAPABILITY_MISMATCH');
      assert(capabilities.acknowledgement !== 'none' || envelope.acknowledge === undefined,
        'This incoming connector declares that acknowledgements are unavailable.', 'SOURCE_CAPABILITY_MISMATCH');
      acknowledgement = { status: acknowledge === 'none' ? 'not_requested' : envelope.acknowledge ? 'pending' : 'not_available' };
      assert(isRecord(envelope.input), 'Incoming task must be an object.', 'INVALID_SOURCE_MESSAGE');
      // The submission envelope combines independently bounded application values.
      const task = captureSubmission(envelope.input);
      if (combined.aborted) throw abortError('admission');
      phase = 'admission';
      const admitted = await bounded(child => { dispatched = true; return runner.submit(task, { source: source.name, signal: child }); },
        { signal: combined, timeoutMs: processingTimeoutMs, stage: phase });
      identifier(admitted?.taskId, 'Admitted task ID'); taskId = admitted.taskId; report = admitted;
      counts.admitted++;
      if (acknowledge === 'admitted') await acknowledgeMessage();
      phase = 'execution'; dispatched = false;
      const executed = await bounded(child => {
        dispatched = true;
        return runner.execute(taskId, { signal: child, waitMs: processingTimeoutMs });
      }, { signal: combined, timeoutMs: processingTimeoutMs, stage: phase });
      assert(isRecord(executed) && executed.taskId === taskId && ['completed', 'partial', 'failed', 'unknown', 'cancelled', 'queued', 'running'].includes(executed.status),
        'Runner returned an invalid execution report.', 'INVALID_RESPONSE');
      report = executed;
      if (envelope.reply) {
        phase = 'reply'; dispatched = false;
        const disposition = await bounded(child => {
          dispatched = true;
          return runner.reply(taskId, (value, replyContext) => envelope.reply.call(message, value, replyContext),
            { signal: child, timeoutMs: replyTimeoutMs });
        }, { signal: combined, timeoutMs: replyTimeoutMs, stage: phase });
        const failure = replyFailure(disposition);
        if (failure) throw failure;
      }
      if (report.status === 'completed') {
        counts.completed++;
        if (acknowledge === 'completed') await acknowledgeMessage();
      } else {
        if (report.status === 'unknown') counts.unknown++;
        else if (report.status === 'cancelled') counts.cancelled++;
        else counts.failed++;
        error = { code: 'TASK_UNSETTLED', message: `Task processing ended with ${report.status}.`, stage: 'execution',
          ...(report.status === 'unknown' ? { ambiguous: true } : {}) };
      }
    } catch (cause) {
      error = publicError(cause, phase);
      if (dispatched && ['TIMEOUT', 'ABORTED', 'UNEXPECTED_ERROR', 'INVALID_RESPONSE'].includes(error.code) && error.ambiguous === undefined) error.ambiguous = true;
      if (error.ambiguous) counts.unknown++;
      else if (error.code === 'ABORTED') counts.cancelled++;
      else counts.failed++;
    }
    if (error && typeof envelope?.reject === 'function' && !['acknowledged', 'unknown'].includes(acknowledgement.status)) {
      let called = false;
      try {
        const receipt = await bounded(child => { called = true; return envelope.reject.call(message, freeze(error), context(child)); },
          { timeoutMs: acknowledgementTimeoutMs, stage: 'source-rejection' });
        rejection = { status: 'rejected', ...(receipt === undefined ? {} : { receipt: snapshot(receipt) }) };
        counts.rejected++;
      } catch (cause) {
        const failure = publicError(cause, 'source-rejection');
        if (failure.ambiguous === undefined) failure.ambiguous = called;
        rejection = { status: failure.ambiguous ? 'unknown' : 'failed', error: failure };
        counts.rejectionFailed++;
      }
    }
    if (envelope?.acknowledge && !['acknowledged'].includes(acknowledgement.status)) counts.unacknowledged++;
    await observe({ source: source.name, index, ...(taskId ? { taskId } : {}), acknowledgement,
      ...(rejection ? { rejection } : {}), ...(report ? { report } : {}), ...(error ? { error } : {}) });
  }

  try {
    // open retains the lifecycle signal, rather than a completed call's temporary deadline signal.
    const opened = await bounded(() => source.open({ signal: combined }), { signal: combined, timeoutMs: sourceTimeoutMs, stage: 'source-open' });
    assert(opened && typeof opened[Symbol.asyncIterator] === 'function', 'Source open must return an async iterable.', 'INVALID_SOURCE');
    iterator = opened[Symbol.asyncIterator]();
    assert(iterator && typeof iterator.next === 'function', 'Source open did not provide an iterator.', 'INVALID_SOURCE');
    while (!combined.aborted) {
      if (active.size >= concurrency) {
        await bounded(() => Promise.race(active), { signal: combined,
          timeoutMs: Math.min(2147483647, 2 * processingTimeoutMs + 2 * acknowledgementTimeoutMs + replyTimeoutMs + observerTimeoutMs + 1000), stage: 'source-backpressure' });
        continue;
      }
      const step = await bounded(() => iterator.next(), { signal: combined, timeoutMs: sourceTimeoutMs, stage: 'source-read' });
      assert(step && typeof step === 'object' && (step.done === undefined || typeof step.done === 'boolean'), 'Source returned an invalid iteration result.', 'INVALID_SOURCE');
      if (step.done) { exhausted = true; break; }
      const index = counts.received++;
      const pending = handle(step.value, index).finally(() => active.delete(pending));
      active.add(pending);
    }
    if (combined.aborted) stopped = 'cancelled';
  } catch (cause) {
    const error = publicError(cause, 'source');
    if (combined.aborted || error.code === 'ABORTED') stopped = 'cancelled';
    else {
      stopped = 'failed'; counts.sourceErrors++;
      remember(error);
      await observe({ source: source.name, stage: 'source', error });
    }
  } finally {
    if (combined.aborted) lifecycle.abort();
    await Promise.allSettled([...active]);
    if (signal?.aborted) stopped = 'cancelled';
    // Stop cooperative open/read work before attempting independently bounded cleanup.
    lifecycle.abort();
    if (!exhausted && iterator && typeof iterator.return === 'function') {
      try { await bounded(() => iterator.return(), { timeoutMs: closeTimeoutMs, stage: 'source-close' }); }
      catch (cause) { counts.closeErrors++; remember(publicError(cause, 'source-close')); }
    }
    if (source.close) {
      try { await bounded(child => source.close({ signal: child, timeoutMs: closeTimeoutMs }), { timeoutMs: closeTimeoutMs, stage: 'source-close' }); }
      catch (cause) { counts.closeErrors++; remember(publicError(cause, 'source-close')); }
    }
  }
  return freeze({ source: source.name, stopped, ...counts, errors });
}
