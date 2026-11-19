import { assert, freeze, identifier } from './util.mjs';

// Portable metadata lets hosts validate routing before sources or workers have effects.
export const SOURCE_BINDING = Symbol('source-binding');

export function incomingMetadata(name, revision = '1', acknowledgement = 'none', replayable = false) {
    return freeze({ name: identifier(name, 'Incoming source name'), revision: identifier(revision, 'Incoming source revision'),
        capabilities: { direction: 'incoming', runtime: 'node', acknowledgement, backpressure: 'pull', replayable } });
}

export function sourceBinding({ flow, inputMode = 'submission' } = {}, durableAdmission = false) {
    identifier(flow, 'Incoming flow');
    assert(['submission', 'request', 'event', 'result'].includes(inputMode), 'Invalid incoming inputMode.');
    return freeze({ flow, inputMode, durableAdmission });
}

export function validateSourceBinding(binding, flows, aggregation = false) {
    if (!binding || aggregation) return;
    assert(flows && Object.hasOwn(flows, binding.flow), 'Incoming target flow is unavailable.');
    assert(binding.inputMode === 'submission' || flows[binding.flow].inputs.includes(binding.inputMode),
        'Incoming target flow does not accept its input mode.');
}
