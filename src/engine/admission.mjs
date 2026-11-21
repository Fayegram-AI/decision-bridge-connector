import { assert, bounded, fingerprint, fingerprintFields, invalidInput, freeze, id, isRecord, snapshot } from '../util.mjs';
import { project, projectCaptured } from '../mapping.mjs';
import { LIMITS, normalizeSubmission, RECORD_VERSION } from './flows.mjs';
import { validateSavedValue } from './reconciliation.mjs';
import { registeredEntry } from './registry.mjs';
import { captureAggregateEvent } from './aggregation-values.mjs';

/** Build immutable work before admission; result validation never invokes inference. */
async function prepareInput(input, { flows, options, source, namespace, lineage, evaluationId, validationRequest, aggregateRequest, aggregateEvent,
    policyOnly = false, admissionOwner, signal, maxBytes = LIMITS.maxBytes }) {
    assert(isRecord(input), 'Submission must be an object.');
    const selected = Object.getOwnPropertyDescriptor(input, 'flow')?.value;
    const plan = typeof selected === 'string' && Object.hasOwn(flows, selected) ? flows[selected] : undefined;
    const aggregateField = aggregateEvent ? 'event' : aggregateRequest && validationRequest === undefined ? 'request' : undefined;
    const submission = normalizeSubmission(input, plan, source, namespace, maxBytes, aggregateField);
    if (aggregateRequest && validationRequest !== undefined) validationRequest = captureAggregateEvent(validationRequest, maxBytes);
    const createdAt = Date.now();
    const taskId = id('task');
    const task = { id: taskId, schemaVersion: RECORD_VERSION, namespace, source,
        flow: plan.id, flowRevision: plan.revision, state: 'active', createdAt, updatedAt: createdAt,
        input: submission, plan, evaluationId: evaluationId ?? id('evaluation'), policyOnly, policies: {}, errors: [],
        evidenceAt: submission.evidenceAt ?? new Date(createdAt).toISOString(),
        lineage: lineage ?? { rootId: taskId, depth: 0, path: [] },
        ...(aggregateEvent ? { aggregateEvent: true } : {}),
        ...(aggregateField === 'request' ? { aggregateInputRequest: true } : {}),
        ...(admissionOwner ? { admissionOwner } : {}),
        fingerprint: await fingerprintFields({ ...submission, ...(validationRequest === undefined ? {} : { validationRequest }) },
            [aggregateField, aggregateRequest ? 'validationRequest' : undefined]) };
    assert(task.lineage.depth < plan.maxDepth, 'Flow chain depth reached.', 'RESOURCE_LIMIT');
    task.rootId = task.lineage.rootId;
    task.lineage = { ...task.lineage, parentId: task.lineage.path.at(-1) ?? null,
        maxDepth: Math.min(task.lineage.maxDepth ?? plan.maxDepth, plan.maxDepth),
        maxExecutions: Math.min(task.lineage.maxExecutions ?? plan.maxExecutions, plan.maxExecutions) };
    assert(task.lineage.depth < task.lineage.maxDepth, 'Inherited flow chain depth reached.', 'RESOURCE_LIMIT');
    if (submission.kind !== 'result') {
        const wholeAggregate = aggregateEvent && (plan.template?.$path === '$' || plan.template?.$path === '');
        const request = submission.kind !== 'event' ? submission.request : wholeAggregate ? submission.event :
            (aggregateEvent ? projectCaptured : project)(submission.event, plan.template);
        const captureRequest = wholeAggregate || aggregateField === 'request'
            ? value => captureAggregateEvent(value, maxBytes) : value => snapshot(value, { maxBytes });
        const evaluator = registeredEntry(options.evaluators, plan.evaluator);
        // Preflight must use the same pinned evaluator contract as execution.
        assert(evaluator?.revision === plan.evaluatorRevision && typeof evaluator.execute === 'function' &&
            (evaluator.validate === undefined || typeof evaluator.validate === 'function'),
        'The configured evaluator revision is required.', 'REVISION_UNAVAILABLE');
        const checked = evaluator.validate ? await bounded(() => evaluator.validate(freeze(captureRequest(request))), { signal, timeoutMs: plan.timeoutMs }) : request;
        const normalized = checked === undefined ? request : checked;
        if (wholeAggregate || aggregateField === 'request') {
            try { task.request = snapshot(normalized, { maxBytes }); }
            catch (error) {
                if (error?.code !== 'RESOURCE_LIMIT') throw error;
                task.request = captureRequest(normalized);
                task.aggregateRequest = true;
            }
        } else task.request = captureRequest(normalized);
    } else {
        if (validationRequest !== undefined) {
            task.request = aggregateRequest ? validationRequest : snapshot(validationRequest, { maxBytes });
            if (aggregateRequest) task.aggregateRequest = true;
        }
        const validator = registeredEntry(options.validators, plan.validator);
        const evaluator = registeredEntry(options.evaluators, plan.evaluator);
        assert(plan.validator !== undefined
            ? validator?.revision === plan.validatorRevision && typeof validator.validate === 'function'
            : evaluator?.revision === plan.evaluatorRevision && typeof evaluator.validateSaved === 'function',
        'The configured result validator revision is required.', 'REVISION_UNAVAILABLE');
        const checked = await bounded(signal => validateSavedValue(task, submission.result, validator, evaluator, signal),
            { signal, timeoutMs: plan.timeoutMs, stage: 'result-validation' });
        task.result = snapshot(checked === undefined ? submission.result : checked, { maxBytes });
        task.validation = { validator: plan.validator ?? plan.evaluator, revision: plan.validatorRevision, provenance: validator?.provenance ?? 'structural' };
        task.evaluatedAt = createdAt;
    }
    const common = { namespace, taskId, flow: plan.id, revision: plan.runtimeRevision, createdAt, runAt: createdAt, priority: 0, required: 1, leaseUntil: null, token: null };
    const jobs = [{ ...common, id: `${taskId}:evaluation`, kind: 'evaluation', state: 'pending', stage: 'prepared', attempts: [] },
        ...plan.connections.map(connection => ({ ...common, id: `${taskId}:delivery:${connection.id}`, kind: 'delivery', state: 'blocked',
            stage: 'prepared', destination: connection.destinationKey, placement: connection.placement ?? null,
            orderingKey: connection.ordering ? `${connection.destinationKey}:${connection.ordering.key}` : null,
            priority: connection.required ? 0 : 1, required: connection.required ? 1 : 0,
            expiresAt: createdAt + plan.deliveryTtlMs, plan: connection, attempts: [], completion: { status: 'untracked' } }))];
    const dedupKey = submission.idempotencyKey ? await fingerprint([namespace, source, plan.id, submission.idempotencyKey]) : null;
    for (const connection of plan.connections.filter(item => item.nextFlow)) jobs.push({ ...common,
        id: `${taskId}:chain:${connection.id}`, kind: 'chain', state: 'blocked', stage: 'prepared',
        required: connection.required ? 1 : 0, priority: connection.required ? 0 : 1,
        deliveryId: `${taskId}:delivery:${connection.id}`, plan: connection, attempts: [] });
    if (plan.connections.some(connection => connection.when === 'failure')) jobs.push({ ...common,
        id: `${taskId}:failure`, kind: 'failure', state: 'skipped', stage: 'prepared', attempts: [] });
    return { task, jobs, dedupKey };
}

/** Definite local validation errors retain their code and explicit quarantine disposition. */
export async function prepare(...args) {
    try { return await prepareInput(...args); } catch (error) { throw invalidInput(error); }
}
