import { abortError, assert, bounded, freeze, id, publicError, snapshot } from '../util.mjs';
import { applyPolicy } from '../policy.mjs';
import { project } from '../mapping.mjs';
import { transform } from './transform.mjs';
import { staleDisposition } from './freshness.mjs';
import { registeredEntry } from './registry.mjs';

export function eligible(condition, task, decision) {
    if (condition === 'always') return true;
    if (condition === 'failure') return task.errors.length > 0;
    if (condition === 'evaluation.succeeded') return task.result !== undefined;
    return condition === `decision.${decision?.status}`;
}

export function selectPayload(kind, task, decision) {
    if (kind === 'raw-result') { assert(task.result !== undefined, 'Raw result is unavailable.'); return task.result; }
    if (kind === 'final-decision') { assert(decision, 'Decision is unavailable.'); return decision; }
    if (kind === 'final-decision-with-result') { assert(decision && task.result !== undefined, 'Decision or result unavailable.'); return { decision, result: task.result }; }
    if (kind === 'failure') return { taskId: task.id, errors: task.errors };
    return { taskId: task.id, evaluationId: task.evaluationId, policies: task.policies, errors: task.errors, result: task.result ?? null };
}

export async function planOutputs(task, options, { failureOnly = false, signal, maxBytes = 1048576 } = {}) {
    if (signal?.aborted) throw abortError('planning');
    const policies = failureOnly ? task.policies : Object.create(null);
    if (!failureOnly && task.result !== undefined) for (const [name, policy] of Object.entries(task.plan.policies)) {
        try {
            const module = policy.type === 'module' ? registeredEntry(options.policies, policy.name) : undefined;
            if (policy.type === 'module') assert(module?.revision === policy.revision && typeof module.execute === 'function',
                'The configured policy revision is unavailable.', 'REVISION_UNAVAILABLE');
            const value = policy.type === 'module'
                ? await bounded(child => module.execute(freeze(snapshot(task.result)), policy.options ?? {}, { signal: child }), { signal, timeoutMs: task.plan.timeoutMs })
                : applyPolicy(task.result, policy, { operation: task.plan.operation,
                    ...(task.input.correlationId ? { correlation: task.input.correlationId } : {}),
                    ...(task.input.questionRevision ? { questionRevision: task.input.questionRevision } : {}) });
            assert(['accepted', 'review', 'abstained'].includes(value.status), 'Invalid policy decision.');
            policies[name] = { status: 'succeeded', decision: snapshot({ ...snapshot(value), id: id('decision'),
                evaluationId: task.evaluationId, policyId: name, policyRevision: policy.revision }, { maxBytes }) };
        } catch (error) {
            if (signal?.aborted) throw error;
            policies[name] = { status: 'failed', error: publicError(error, 'policy') };
        }
    }
    // Failure-only planning reuses saved policies and their existing diagnostics.
    const policyErrors = failureOnly ? [] : Object.values(policies).filter(x => x.error).map(x => x.error);
    task = { ...task, policies, errors: [...task.errors, ...policyErrors] };
    const stale = staleDisposition(task);
    const jobs = [];
    for (const connection of task.plan.connections) {
        if (failureOnly && connection.when !== 'failure') continue;
        const job = { id: `${task.id}:delivery:${connection.id}`, state: 'skipped' };
        const decision = registeredEntry(policies, connection.policy)?.decision;
        if (!task.policyOnly && eligible(connection.when, task, decision)) {
            if (stale && connection.when !== 'failure') job.state = stale;
            else try {
                let payload = selectPayload(connection.payload, task, decision);
                if (connection.projection !== undefined) payload = project(payload, connection.projection);
                // Capture the selected instances before yielding; later registry changes
                // cannot replace an authorized transformation between check and invocation.
                const modules = Object.create(null);
                for (const step of connection.transforms ?? []) if (step.type === 'module') {
                    const selected = registeredEntry(options.transforms, step.name);
                    assert(selected?.revision === task.plan.transformRevisions[step.name] && typeof selected.execute === 'function',
                        'The configured transform revision is unavailable.', 'REVISION_UNAVAILABLE');
                    modules[step.name] = selected;
                }
                payload = await bounded(() => transform(payload, connection.transforms, modules, { maxBytes }), { signal, timeoutMs: connection.timeoutMs, stage: 'transform' });
                if (connection.outputSchema !== undefined) {
                    assert(options.validateOutput, 'Output schema validator is required.');
                    const valid = await bounded(() => options.validateOutput(connection.outputSchema, payload),
                        { signal, timeoutMs: connection.timeoutMs, stage: 'output-validation' });
                    assert(valid === undefined || typeof valid === 'boolean', 'Output validator must return a boolean or no value.', 'OUTPUT_SCHEMA');
                    assert(valid !== false, 'Outgoing payload does not match its schema.', 'OUTPUT_SCHEMA');
                }
                if (connection.encoding === 'text') assert(typeof payload === 'string', 'Text encoding requires a string payload.');
                job.payload = payload; job.state = 'pending';
            } catch (error) {
                if (signal?.aborted) throw error;
                job.state = 'failed'; job.error = { ...publicError(error, 'output-planning'), optional: !connection.required }; task.errors.push(job.error);
            }
        }
        jobs.push(job);
    }
    return { task: { policies, errors: task.errors, plannedAt: Date.now() }, jobs };
}
