import { assert, bounded, captureObject, freeze, isRecord, snapshot } from '../util.mjs';
import { LIMITS } from './flows.mjs';
import { registeredEntry } from './registry.mjs';

/** Retain caller validation metadata while binding the authoritative evaluated request. */
export function savedValidation(task) {
    const retained = task.input?.validation;
    return task.request === undefined ? retained ?? {} : { ...(isRecord(retained) ? retained : {}), request: task.request };
}

/** Resolve an uncertain evaluation using operator evidence, without invoking a provider. */
export async function validateResolution(record, resolution, options, { maxBytes = LIMITS.maxBytes, signal } = {}) {
    assert(record?.task, 'Execution not found.', 'NOT_FOUND');
    assert(['succeeded', 'failed'].includes(resolution?.status), 'Resolution must be succeeded or failed.');
    const value = captureObject(resolution, ['result'], maxBytes);
    assert(typeof value.reason === 'string' && value.reason.trim().length > 0, 'Evaluation resolution requires an audit reason.');
    if (value.status === 'failed') return value;
    assert(Object.hasOwn(value, 'result'), 'Successful resolution requires the recovered result.');
    const { task } = record;
    const validator = registeredEntry(options.validators, task.plan.validator);
    const evaluator = registeredEntry(options.evaluators, task.plan.evaluator);
    assert(task.plan.validator !== undefined
        ? validator?.revision === task.plan.validatorRevision && typeof validator.validate === 'function'
        : evaluator?.revision === task.plan.evaluatorRevision && typeof evaluator.validateSaved === 'function',
        'The original result validator revision is required.', 'REVISION_UNAVAILABLE');
    const checked = await bounded(signal => validator
        ? validator.validate(freeze(snapshot(value.result)), { signal, validation: savedValidation(task) })
        : evaluator.validateSaved(freeze(snapshot(value.result)), task.request, { signal, timeoutMs: task.plan.timeoutMs }),
    { signal, timeoutMs: task.plan.timeoutMs });
    return { ...value, result: snapshot(checked === undefined ? value.result : checked, { maxBytes }) };
}
