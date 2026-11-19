import { assert, isRecord, keysOnly, snapshot } from '../util.mjs';

export const AGGREGATE_EVENT = Symbol('aggregate-event');
const memberValuePath = [null, 'event'];
const aggregateValuePath = ['events', ...memberValuePath];

/** Member evidence keeps its own depth, while the container keeps one byte and node budget. */
export function captureAggregateMembers(value, maxBytes) {
    return snapshot(value, { maxBytes, valueDepthPath: memberValuePath });
}

export function captureAggregateEvent(value, maxBytes) {
    const captured = snapshot(value, { maxBytes, valueDepthPath: aggregateValuePath });
    assert(isRecord(captured) && typeof captured.aggregateId === 'string' &&
        ['complete', 'incomplete'].includes(captured.status) && isRecord(captured.events) &&
        Object.keys(captured.events).length <= 64 && Array.isArray(captured.missingSources) &&
        captured.missingSources.length <= 64 && captured.missingSources.every(name => typeof name === 'string' && name.length <= 256) &&
        (typeof captured.correlationKey === 'string' || typeof captured.correlationKey === 'number'),
    'Invalid aggregate event.', 'VALIDATION_ERROR');
    keysOnly(captured, ['aggregateId', 'status', 'correlationKey', 'events', 'missingSources'], 'Aggregate event');
    return captured;
}
