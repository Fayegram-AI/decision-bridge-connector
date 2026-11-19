import { validateResult } from '@fayegram-ai/decision-bridge';
import { assert, freeze, identifier, isRecord, keysOnly, snapshot, validateSignal } from './util.mjs';

function routingOptions(options) {
  keysOnly(options, ['route', 'candidates', 'labels', 'routingDescription', 'onAmbiguity', 'clarify', 'signal', 'timeoutMs'], 'Routing call options');
  assert(Object.getOwnPropertySymbols(options).length === 0 &&
    Object.values(Object.getOwnPropertyDescriptors(options)).every(descriptor => descriptor.enumerable && 'value' in descriptor),
  'Routing call options cannot contain symbols or accessors.');
  const { signal, onAmbiguity, ...json } = options;
  validateSignal(signal);
  assert(onAmbiguity === undefined || typeof onAmbiguity === 'function', 'onAmbiguity must be a function.');
  if (json.timeoutMs !== undefined) assert(Number.isSafeInteger(json.timeoutMs) && json.timeoutMs > 0 && json.timeoutMs <= 2147483647,
    'Invalid routing timeout.');
  // Clone configuration data while borrowing the explicit callback and signal.
  return Object.freeze({ ...freeze(snapshot(json)), ...(signal === undefined ? {} : { signal }),
    ...(onAmbiguity === undefined ? {} : { onAmbiguity }) });
}

function routingCall(configured, context = {}) {
  keysOnly(context, ['signal', 'timeoutMs'], 'Evaluator execution context');
  validateSignal(context.signal);
  if (context.timeoutMs !== undefined) assert(Number.isSafeInteger(context.timeoutMs) && context.timeoutMs > 0 && context.timeoutMs <= 2147483647,
    'Invalid evaluator timeout.');
  const signal = configured.signal && context.signal ? AbortSignal.any([configured.signal, context.signal]) : configured.signal ?? context.signal;
  const timeoutMs = configured.timeoutMs === undefined ? context.timeoutMs : context.timeoutMs === undefined
    ? configured.timeoutMs : Math.min(configured.timeoutMs, context.timeoutMs);
  return { ...configured, ...(signal === undefined ? {} : { signal }), ...(timeoutMs === undefined ? {} : { timeoutMs }) };
}

export function decisionEvaluator(bridge, { revision = '1', validationOptions = {} } = {}) {
  assert(bridge && typeof bridge.evaluate === 'function' && typeof bridge.validate === 'function', 'A public Bridge or decision router is required.');
  const resolver = typeof validationOptions === 'function' ? validationOptions : null;
  const fixedPolicy = resolver ? null : snapshot(validationOptions);
  return Object.freeze({ operation: 'decision', revision: identifier(revision, 'Evaluator revision'),
    // Public preflight can canonicalize model aliases. Dispatch the original
    // snapshot so Bridge can preserve the caller's requested model provenance.
    validate(request) { bridge.validate(request); return request; },
    execute: (request, options) => bridge.evaluate(request, options),
    validateSaved(result, request) {
      const resolved = bridge.validate(request);
      // Core returns an immutable JSON clone. Recapturing it would charge added
      // resolution metadata against the already-bounded application request.
      const selected = resolver ? resolver(resolved) : fixedPolicy;
      if (selected && typeof selected.then === 'function') {
        Promise.resolve(selected).catch(() => {});
        assert(false, 'Result validation options must be resolved synchronously.');
      }
      const policy = snapshot(selected);
      const checked = validateResult(result, request.questions, policy);
      const metadata = checked.meta;
      const valid = (condition, message) => assert(condition, message, 'INVALID_SAVED_OUTPUT');
      valid(isRecord(metadata) && metadata.providerKind === bridge.providerKind && metadata.provider === bridge.provider,
        'Saved result provenance does not match its configured evaluator.');
      valid(metadata.requestedModel === resolved.model && typeof metadata.probabilitySource === 'string' && metadata.probabilitySource.trim().length > 0,
        'Saved decision model or probability provenance is invalid.');
      valid(typeof metadata.bridgeVersion === 'string' && metadata.bridgeVersion.length > 0 &&
        Number.isSafeInteger(metadata.schemaVersion) && metadata.schemaVersion >= 1,
      'Saved decision version metadata is invalid.');
      valid(['totalDurationMs', 'observerDurationMs'].every(key => typeof metadata[key] === 'number' && metadata[key] >= 0) &&
        Array.isArray(metadata.observerFailures) && metadata.observerFailures.every(name => typeof name === 'string'),
      'Saved decision observation metadata is invalid.');
      if (metadata.confidenceSource !== undefined) valid(typeof metadata.confidenceSource === 'string' && metadata.confidenceSource.trim().length > 0,
        'Saved confidence provenance is invalid.');
      if (bridge.providerKind === 'generative') valid(metadata.probabilitySource === 'elicited' && metadata.confidenceSource === 'unavailable' &&
        Object.values(checked.answers).every(answer => !Object.hasOwn(answer, 'confidence')),
      'Generative saved results must preserve elicited probabilities without native confidence.');
      return checked;
    } });
}

export function routedDecisionEvaluator(router, { revision = '1', callOptions = {} } = {}) {
  assert(router && typeof router.evaluate === 'function' && typeof router.inspect === 'function', 'A public decision router is required.');
  const configured = routingOptions(callOptions);
  return Object.freeze({ operation: 'decision', revision: identifier(revision, 'Evaluator revision'),
    validate(request) { router.inspect(request, configured); return request; },
    execute: (request, options) => router.evaluate(request, routingCall(configured, options)) });
}

export function extractionEvaluator(provider, { revision = '1' } = {}) {
  assert(provider && typeof provider.extract === 'function' && typeof provider.validate === 'function' && typeof provider.validateResult === 'function', 'A public extraction provider is required.');
  return Object.freeze({ operation: 'extraction', revision: identifier(revision, 'Evaluator revision'),
    validate(request) { provider.validate(request); return request; },
    execute: (request, { signal }) => provider.extract(request, { signal }),
    validateSaved(result, request) {
      try { return freeze(snapshot(provider.validateResult(result, request))); }
      catch { assert(false, 'Saved extraction output or provenance is invalid.', 'INVALID_SAVED_OUTPUT'); }
    } });
}

export function routedExtractionEvaluator(router, { revision = '1', callOptions = {} } = {}) {
  assert(router && typeof router.extract === 'function' && typeof router.inspect === 'function', 'A public extraction router is required.');
  const configured = routingOptions(callOptions);
  return Object.freeze({ operation: 'extraction', revision: identifier(revision, 'Evaluator revision'),
    validate(request) { router.inspect(request, configured); return request; },
    execute: (request, options) => router.extract(request, routingCall(configured, options)) });
}

export function createCallbackConnector(send, { name = 'callback', revision = '1', idempotent = false,
  acknowledgement = 'accepted' } = {}) {
  assert(typeof send === 'function', 'A connector callback is required.');
  identifier(name, 'Connector name'); identifier(revision, 'Connector revision');
  assert(typeof idempotent === 'boolean', 'idempotent must be boolean.');
  assert(['none', 'accepted', 'completed'].includes(acknowledgement), 'Invalid acknowledgement capability.');
  return Object.freeze({ name, revision, capabilities: freeze({ direction: 'outgoing', runtime: 'portable', acknowledgement,
    idempotent }), send });
}
