import { assert, captureObject, isRecord, snapshot } from '../util.mjs';
import { captureAggregateEvent } from './aggregation-values.mjs';

const responseFields = new Set(['response', 'output']);
const completionFields = new Set(['evidence']);

/** Keep the application value's depth inside its bounded evidence envelope. */
export function captureReceiptBody(value, maxBytes) {
    return snapshot(value, { maxBytes, rootValueKeys: isRecord(value) && Object.hasOwn(value, 'response') ? responseFields : undefined });
}

export function captureCompletion(value, maxBytes) {
    return snapshot(value, { maxBytes, rootValueKeys: completionFields });
}

export function captureDeliveryResolution(value, maxBytes) {
    return captureEnvelope(value, {
        receipt: body => captureReceiptBody(body, maxBytes),
        evidence: evidence => snapshot(evidence, { maxBytes }),
    });
}

/** Transport metadata must not consume an independent application value's depth or node budget. */
export function captureSubmission(value, maxBytes = 67108864, aggregateField) {
    if (aggregateField) {
        const fields = Object.fromEntries(['request', 'event', 'result', 'context', 'validation']
            .map(key => [key, item => key === aggregateField ? captureAggregateEvent(item, maxBytes) : snapshot(item, { maxBytes })]));
        return captureEnvelope(value, fields);
    }
    return captureObject(value, ['request', 'event', 'result', 'context', 'validation'], maxBytes);
}

/** Borrow no caller objects or accessors while composing already bounded DTO fields. */
export function captureEnvelope(value, fields = {}, maxBytes = 65536) {
    assert(isRecord(value) && Object.getOwnPropertySymbols(value).length === 0, 'Use a plain JSON object.', 'VALIDATION_ERROR');
    const descriptors = Object.getOwnPropertyDescriptors(value);
    assert(Object.values(descriptors).every(item => item.enumerable && 'value' in item), 'JSON accessors are not supported.', 'VALIDATION_ERROR');
    return Object.fromEntries(Object.entries(descriptors).map(([name, descriptor]) => [name,
        Object.hasOwn(fields, name) ? fields[name](descriptor.value) : snapshot(descriptor.value, { maxBytes })]));
}

/** Preserve each operation's application fields; byte limits on the complete wire remain separate. */
export function captureOperationArguments(action, value, maxBytes = 1048576) {
    const fields = {};
    if (['submit', 'run', 'preview'].includes(action)) fields.input = input => captureSubmission(input, maxBytes);
    if (action === 'resolveEvaluation')
        fields.resolution = resolution => captureObject(resolution, ['result', 'receipt'], maxBytes);
    if (action === 'resolveDelivery') fields.resolution = resolution => captureDeliveryResolution(resolution, maxBytes);
    if (action === 'recordCompletion') fields.completion = completion => captureCompletion(completion, maxBytes);
    if (action === 'appendImport') fields.frame = frame => snapshot(frame, { maxBytes });
    return captureEnvelope(value, fields, Math.min(maxBytes, 65536));
}

export function requireWireBudget(value, maxBytes) {
    assert(new TextEncoder().encode(JSON.stringify(value)).length <= maxBytes, 'Request exceeds its byte limit.', 'RESOURCE_LIMIT');
    return value;
}
