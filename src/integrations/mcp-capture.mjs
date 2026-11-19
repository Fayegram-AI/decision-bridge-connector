import { assert, isRecord, snapshot } from '../util.mjs';
import { captureEnvelope, captureOperationArguments, requireWireBudget } from '../engine/input-capture.mjs';

/** Capture middleware-parsed JSON-RPC without charging protocol wrappers to tool values. */
export function captureMcpBody(value, maxBytes) {
    function message(input) {
        if (!isRecord(input)) return snapshot(input, { maxBytes });
        const method = Object.getOwnPropertyDescriptor(input, 'method')?.value;
        return captureEnvelope(input, { params: params => {
            if (method !== 'tools/call') return snapshot(params, { maxBytes });
            const name = Object.getOwnPropertyDescriptor(params ?? {}, 'name')?.value;
            const action = typeof name === 'string' ? name.replace(/^connectors_/, '') : '';
            return captureEnvelope(params, { arguments: args => captureOperationArguments(action, args, maxBytes) }, maxBytes);
        } }, maxBytes);
    }
    if (!Array.isArray(value)) return requireWireBudget(message(value), maxBytes);
    assert(value.length < 500000, 'JSON batch exceeds its resource limit.', 'RESOURCE_LIMIT');
    const descriptors = Object.getOwnPropertyDescriptors(value);
    const keys = Object.keys(descriptors).filter(key => key !== 'length');
    assert(Object.getOwnPropertySymbols(value).length === 0 && keys.length === value.length &&
        keys.every((key, index) => key === String(index) && descriptors[key].enumerable && 'value' in descriptors[key]),
    'JSON batches must be dense arrays without accessors or extra fields.', 'VALIDATION_ERROR');
    const captured = []; let size = 2;
    for (const key of keys) {
        const next = message(descriptors[key].value);
        size += new TextEncoder().encode(JSON.stringify(next)).length + (captured.length ? 1 : 0);
        // Account incrementally, including batch delimiters, before retaining another message.
        assert(size <= maxBytes, 'Request exceeds its byte limit.', 'RESOURCE_LIMIT');
        captured.push(next);
    }
    return requireWireBudget(captured, maxBytes);
}
