import { assert, freeze, identifier, invalidInput as classifyInput, isRecord, keysOnly, snapshot } from '../util.mjs';
import { captureSubmission } from '../engine/input-capture.mjs';
import { validatePath } from '../mapping.mjs';
import { AGGREGATION_RULE } from '../engine/aggregation.mjs';
import { SOURCE_BINDING, sourceBinding, validateSourceBinding } from '../source-binding.mjs';

export { SOURCE_BINDING, sourceBinding, validateSourceBinding };

// Private metadata lets Node hosts preflight built-in bindings without changing custom source APIs.
export const SOURCE_PREFLIGHT = Symbol('source-preflight');
export const PREPARED_LISTENER = Symbol('prepared-listener');
export const SUSPEND_LISTENER = Symbol('suspend-listener');

/** Bind membership and selectors before any source can claim or acknowledge evidence. */
export function aggregationBinding(value, name, rule) {
    const binding = snapshot(value);
    keysOnly(binding, ['rule', 'member', 'correlationPath', 'eventIdPath'], 'Incoming aggregation binding');
    identifier(binding.rule, 'Aggregation rule');
    identifier(binding.member ?? name, 'Aggregation member');
    validatePath(binding.correlationPath);
    if (binding.eventIdPath !== undefined) validatePath(binding.eventIdPath);
    if (rule) assert(rule.sources.includes(binding.member ?? name), 'Unknown aggregation member.');
    return freeze(binding);
}

/** Capture host routing configuration once; source objects and callbacks stay borrowed. */
export function hostSources(sources, flows, aggregators, durable) {
    assert(isRecord(sources), 'Incoming sources must be a named registry.');
    return Object.entries(sources).map(([name, source]) => {
        identifier(name, 'Incoming source name');
        assert(typeof source?.start === 'function' || typeof source?.open === 'function', 'Incoming extension requires start() or open().');
        assert(source.close === undefined || typeof source.close === 'function', 'Incoming close must be callable.');
        assert(source.health === undefined || typeof source.health === 'function', 'Incoming health must be callable.');
        const binding = source[SOURCE_BINDING];
        assert(!binding?.durableAdmission || durable === true, 'Source acknowledgement requires durable admission.');
        validateSourceBinding(binding, flows, source.aggregation !== undefined);
        let aggregation;
        let aggregator;
        if (source.aggregation !== undefined) {
            aggregation = aggregationBinding(source.aggregation, name);
            aggregator = Object.hasOwn(aggregators, aggregation.rule) ? aggregators[aggregation.rule] : undefined;
            assert(typeof source.open === 'function' && source.start === undefined && typeof aggregator?.add === 'function',
                'Aggregation requires a registered rule and an iterable source.');
            aggregation = aggregationBinding(aggregation, name, aggregator[AGGREGATION_RULE]);
        }
        return { name, source, aggregation, aggregator };
    });
}

// Unknown admission/storage failures remain retryable; only definitive input errors quarantine.
const invalidInput = new Set(['INVALID_JSON', 'INPUT_TOO_LARGE', 'INVALID_RESULT', 'INVALID_SOURCE_MESSAGE', 'CONFIGURATION_ERROR',
    'FORBIDDEN', 'IDEMPOTENCY_CONFLICT', 'EXECUTION_RETIRED', 'DUPLICATE_MEMBER', 'LATE_EVENT']);

export function shouldQuarantine(error) {
    return error?.retryable !== true && error?.ambiguous !== true && (error?.inputInvalid === true || invalidInput.has(error?.code));
}

/** Capture the original source value before identity hashing or flow injection. */
export function captureIncomingValue(value, { inputMode = 'submission' } = {}) {
    return inputMode === 'submission' ? captureSubmission(value) : snapshot(value);
}

/** Incoming connectors bind routing authority before attempting admission. */
export function bindInput(value, { flow, inputMode = 'submission' }, key) {
    assert(typeof flow === 'string', 'Incoming connection requires a flow.');
    try {
        assert(inputMode !== 'submission' || isRecord(value), 'Incoming submission must be an object.');
        const captured = captureIncomingValue(value, { inputMode });
        const input = inputMode === 'submission' ? captured : { kind: inputMode, [inputMode]: captured };
        assert(isRecord(input), 'Incoming submission must be an object.');
        assert(!input.flow || input.flow === flow, 'Incoming flow cannot be overridden.', 'FORBIDDEN');
        return { ...input, flow, ...(key ? { idempotencyKey: key } : {}) };
    } catch (error) { throw classifyInput(error); }
}
