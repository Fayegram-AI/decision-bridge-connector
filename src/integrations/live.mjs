import { createLiveDecisions, validateLiveEvent } from '@fayegram-ai/decision-bridge/live';
export { createLiveSource } from './live-source.mjs';
import { ConnectorError, abortError, assert, bounded, fingerprint, freeze, id, identifier, isRecord, keysOnly, publicError, snapshot, stableStringify, validateSignal } from '../util.mjs';
import { cleanupError } from '../cleanup.mjs';

/** Validate a normalized public notification while preserving native answers and correlation. */
export function validateLiveDecisionNotification(input) {
  const notification = snapshot(input);
  assert(isRecord(notification) && notification.type === 'decision' && notification.eventType === 'decision.event',
    'A Live decision notification is required.', 'VALIDATION_ERROR');
  identifier(notification.eventId, 'Live event ID');
  const correlation = notification.correlation;
  assert(isRecord(correlation) && typeof correlation.matched === 'boolean' && typeof correlation.stale === 'boolean',
    'Live correlation must preserve matched and stale flags.', 'VALIDATION_ERROR');
  for (const key of ['sourceEventId', 'delegationId', 'triggerId']) {
    assert(correlation[key] === null || typeof correlation[key] === 'string' && correlation[key].length > 0,
      'Live correlation IDs must be strings or null.', 'VALIDATION_ERROR');
  }
  assert(correlation.sourceEventType === null || ['session.input_transcript.delta', 'session.output_transcript.delta'].includes(correlation.sourceEventType),
    'Invalid Live source event type.', 'VALIDATION_ERROR');
  if (correlation.generation !== undefined) assert(Number.isSafeInteger(correlation.generation) && correlation.generation >= 0,
    'Invalid Live configuration generation.', 'VALIDATION_ERROR');
  if (correlation.reason !== undefined) assert(['source_or_configuration_unavailable', 'configuration_mismatch'].includes(correlation.reason),
    'Invalid Live correlation reason.', 'VALIDATION_ERROR');
  assert(!correlation.matched || correlation.generation !== undefined,
    'Matched Live correlation requires a known configuration generation.', 'VALIDATION_ERROR');
  // Public wire validation checks the native payload, including native refusals.
  // Correlation flags are observations from the supplied controller, not proof of answer accuracy.
  validateLiveEvent({ type: 'decision.event', event_id: notification.eventId,
    delegation_id: correlation.delegationId, trigger_id: correlation.triggerId,
    source_event_id: correlation.sourceEventId, source_event_type: correlation.sourceEventType,
    event: { type: 'decision.completed', decision: notification.decision } });
  return freeze(notification);
}

/** An existing Live result enters the shared pipeline without another model call. */
export function liveNotificationEvaluator({ revision = '1' } = {}) {
  identifier(revision, 'Live evaluator revision');
  const validate = request => {
    keysOnly(request, ['notification'], 'Live request');
    return freeze({ notification: validateLiveDecisionNotification(request.notification) });
  };
  return Object.freeze({ operation: 'live', revision, validate,
    validateSaved(result, request) {
      const saved = validate(result);
      const original = validate(request);
      assert(stableStringify(saved) === stableStringify(original),
        'Saved Live notification does not match its admitted event.', 'INVALID_SAVED_RESULT');
      return saved;
    },
    async execute(request, { signal } = {}) {
      validateSignal(signal);
      if (signal?.aborted) throw abortError('evaluation');
      return validate(request);
    },
  });
}

/** Attach to an existing public controller or a supplied application-owned transport. */
export function attachLiveController({ controller: suppliedController, transport, configuration, controllerOptions = {},
  runner, flow, taskDefaults = {}, source = 'live', sessionId = id('session'), concurrency = 4, maxPending = 128,
  processing = 'run', processingTimeoutMs = 60000, observerTimeoutMs = 1000, onNotification, onOutcome,
  ownsController = false, ownsConnection = false, closeConnection } = {}) {
  assert(!!suppliedController !== !!transport, 'Provide exactly one Live controller or transport.');
  assert(['run', 'admit'].includes(processing), 'Live processing must be run or admit.');
  assert(runner && typeof runner[processing === 'admit' ? 'submit' : 'run'] === 'function', 'Live ingress requires a runner.');
  identifier(flow, 'Live flow'); identifier(source, 'Live source'); identifier(sessionId, 'Live session ID');
  assert(Number.isSafeInteger(concurrency) && concurrency >= 1 && concurrency <= 64, 'Invalid Live concurrency.');
  assert(Number.isSafeInteger(maxPending) && maxPending >= concurrency && maxPending <= 100000, 'Invalid Live pending limit.');
  for (const [label, value] of Object.entries({ processingTimeoutMs, observerTimeoutMs })) {
    assert(Number.isSafeInteger(value) && value > 0 && value <= 2147483647, `Invalid ${label}.`);
  }
  assert(typeof ownsController === 'boolean' && typeof ownsConnection === 'boolean', 'Live ownership flags must be booleans.');
  assert(!ownsConnection || typeof closeConnection === 'function', 'An owned connection requires an explicit closeConnection callback.');
  for (const observer of [onNotification, onOutcome]) assert(observer === undefined || typeof observer === 'function', 'Live observers must be functions.');
  const defaults = snapshot(taskDefaults);
  keysOnly(defaults, ['questionRevision', 'correlationId', 'causationId', 'deadlineAt', 'context'], 'Live task defaults');
  const options = { ...controllerOptions };
  assert(!Object.hasOwn(options, 'transport') && !Object.hasOwn(options, 'configuration'), 'Use explicit transport and configuration options.');
  assert(!suppliedController || configuration === undefined && Object.keys(options).length === 0,
    'Existing controllers retain their own configuration and resource settings.');
  const controller = suppliedController ?? createLiveDecisions({ ...options, transport,
    ...(configuration === undefined ? {} : { configuration }) });
  assert(controller && typeof controller.subscribe === 'function' && typeof controller.dispose === 'function', 'Invalid Live controller.');
  const disposeController = !suppliedController || ownsController;
  const lifetime = new AbortController();
  const queue = [];
  const waiters = new Set();
  const observers = new Set();
  let active = 0;
  let stopped = false;
  let stopPromise;
  const counts = { received: 0, decisions: 0, admitted: 0, completed: 0, failed: 0, overflow: 0, observerErrors: 0 };

  function observed(callback, value) {
    if (!callback) return Promise.resolve();
    // A noncooperative observer must not bypass the bounded ingress buffer.
    if (observers.size >= maxPending) { counts.observerErrors++; return Promise.resolve(); }
    const promise = bounded(() => callback(freeze(value)), { timeoutMs: observerTimeoutMs, stage: 'observer' })
      .catch(() => { counts.observerErrors++; });
    observers.add(promise);
    promise.finally(() => observers.delete(promise));
    return promise;
  }
  function settled() {
    if (active || queue.length) return;
    for (const resolve of waiters) resolve();
    waiters.clear();
  }
  async function process(notification) {
    const task = { ...defaults, flow, kind: 'result', result: { notification } };
    let dispatched = false;
    try {
      task.idempotencyKey = `live:${await fingerprint([sessionId, notification.eventId])}`;
      const report = await bounded(signal => {
        dispatched = true;
        return processing === 'admit' ? runner.submit(task, { signal, source })
          : runner.run(task, { signal, source, waitMs: processingTimeoutMs });
      },
        { signal: lifetime.signal, timeoutMs: processingTimeoutMs, stage: 'live-ingress' });
      assert(isRecord(report) && ['completed', 'partial', 'failed', 'unknown', 'cancelled', 'queued', 'running'].includes(report.status),
        'Runner returned an invalid execution report.', 'INVALID_RESPONSE');
      counts.admitted++;
      if (processing === 'run') {
        if (report.status === 'completed') counts.completed++; else counts.failed++;
      }
      await observed(onOutcome, { notification, report });
    } catch (cause) {
      counts.failed++;
      const error = publicError(cause, 'live-ingress');
      if (dispatched && error.ambiguous === undefined && ['TIMEOUT', 'ABORTED', 'UNEXPECTED_ERROR', 'INVALID_RESPONSE'].includes(error.code)) error.ambiguous = true;
      await observed(onOutcome, { notification, error });
    }
  }
  function pump() {
    while (active < concurrency && queue.length && !lifetime.signal.aborted) {
      const notification = queue.shift(); active++;
      process(notification).finally(() => { active--; pump(); settled(); });
    }
    settled();
  }
  const unsubscribe = controller.subscribe(notification => {
    if (stopped) return;
    counts.received++;
    let value;
    try { value = snapshot(notification); assert(isRecord(value), 'Live notification must be an object.', 'VALIDATION_ERROR'); }
    catch (error) { counts.failed++; observed(onOutcome, { error: publicError(error, 'live-ingress') }); return; }
    observed(onNotification, value);
    // Lifecycle, transcript and duplicate notifications are observable, not new evaluations.
    if (value.type !== 'decision') return;
    counts.decisions++;
    try { value = validateLiveDecisionNotification(value); }
    catch (error) { counts.failed++; observed(onOutcome, { notification: value, error: publicError(error, 'live-ingress') }); return; }
    if (active + queue.length >= maxPending) {
      counts.overflow++;
      observed(onOutcome, { notification: value, error: publicError(new ConnectorError('RESOURCE_LIMIT',
        'Live ingress buffer is full; this event was not admitted.'), 'live-ingress') });
      return;
    }
    queue.push(value); pump();
  });
  assert(typeof unsubscribe === 'function', 'Live subscription must return an unsubscribe callback.');

  async function drain() {
    if (queue.length || active) await new Promise(resolve => waiters.add(resolve));
    while (observers.size) await Promise.allSettled([...observers]);
  }
  return Object.freeze({
    controller,
    sessionId,
    capabilities: freeze({ direction: 'incoming', runtime: 'portable', acknowledgement: 'none',
      idempotent: false, durableAdmission: false, backpressure: 'bounded-buffer' }),
    status() { return freeze({ state: stopped ? 'stopped' : 'listening', active, queued: queue.length, ...counts }); },
    drain,
    requestDecision(options) {
      assert(!stopped, 'Live attachment is stopped.', 'CONNECTION_CLOSED');
      return controller.requestDecision(options);
    },
    appendCommentary(content, options) {
      assert(!stopped, 'Live attachment is stopped.', 'CONNECTION_CLOSED');
      return controller.appendCommentary(content, options);
    },
    stop({ drain: finish = true } = {}) {
      if (stopPromise) return stopPromise;
      stopped = true;
      if (!finish) {
        lifetime.abort();
        for (const notification of queue.splice(0)) {
          counts.failed++;
          observed(onOutcome, { notification, error: publicError(abortError('live-ingress')) });
        }
        settled();
      }
      stopPromise = Promise.resolve().then(async () => {
        const failures = [];
        try {
          await bounded(() => unsubscribe(), { timeoutMs: processingTimeoutMs, stage: 'live-unsubscribe' });
        } catch (error) { counts.observerErrors++; failures.push(error); }
        if (disposeController) try {
          await bounded(() => controller.dispose(), { timeoutMs: processingTimeoutMs, stage: 'live-dispose' });
        } catch (error) { counts.observerErrors++; failures.push(error); }
        try { await drain(); } catch (error) { failures.push(error); }
        if (ownsConnection) try {
          await bounded(() => closeConnection(), { timeoutMs: processingTimeoutMs, stage: 'live-close' });
        } catch (error) { failures.push(error); }
        if (failures.length) throw cleanupError(failures);
      });
      return stopPromise;
    },
  });
}
