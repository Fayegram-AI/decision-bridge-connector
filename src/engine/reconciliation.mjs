import { assert, bounded, freeze, isRecord, snapshot } from '../util.mjs';
import { captureEvaluationResolution } from './input-capture.mjs';
import { LIMITS } from './flows.mjs';
import { registeredEntry } from './registry.mjs';
import { captureAggregateEvent } from './aggregation-values.mjs';

/** Both saved-result paths validate against the original request, including a normalized null. */
export function validateSavedValue(task, value, validator, evaluator, signal) {
    let validation = snapshot(task.input?.validation ?? {});
    if (task.request !== undefined) {
        // A validator may normalize its own copy. Persist the original evidence for the next replay.
        const request = task.aggregateRequest ? captureAggregateEvent(task.request) : snapshot(task.request);
        validation = { ...(isRecord(validation) ? validation : {}), request };
    }
    const result = freeze(snapshot(value));
    return validator ? validator.validate(result, { signal, validation })
        : evaluator.validateSaved(result, validation.request, { signal, timeoutMs: task.plan.timeoutMs });
}

/** Resolve an uncertain evaluation using operator evidence, without invoking a provider. */
export async function validateResolution(record, resolution, options, { maxBytes = LIMITS.maxBytes, signal } = {}) {
    assert(record?.task, 'Execution not found.', 'NOT_FOUND');
    assert(record.jobs.some(job => job.kind === 'evaluation' && job.state === 'unknown'),
        'Evaluation is not awaiting reconciliation.', 'CONFLICT');
    const value = captureEvaluationResolution(resolution, maxBytes);
    if (value.status === 'failed') return value;
    const { task } = record;
    const validator = registeredEntry(options.validators, task.plan.validator);
    const evaluator = registeredEntry(options.evaluators, task.plan.evaluator);
    assert(task.plan.validator !== undefined
        ? validator?.revision === task.plan.validatorRevision && typeof validator.validate === 'function'
        : evaluator?.revision === task.plan.evaluatorRevision && typeof evaluator.validateSaved === 'function',
        'The original result validator revision is required.', 'REVISION_UNAVAILABLE');
    const checked = await bounded(signal => validateSavedValue(task, value.result, validator, evaluator, signal),
        { signal, timeoutMs: task.plan.timeoutMs });
    return { ...value, result: snapshot(checked === undefined ? value.result : checked, { maxBytes }) };
}
