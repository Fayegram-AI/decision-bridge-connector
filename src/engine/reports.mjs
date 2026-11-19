import { freeze } from '../util.mjs';

/** Business completion is independent of optional deliveries and caller waits. */
export function reportDetails(record) {
    if (!record) return null;
    const { task, jobs } = record;
    const evaluation = jobs.find(job => job.kind === 'evaluation');
    const deliveries = jobs.filter(job => job.kind === 'delivery').map(job => ({ id: job.id,
        connectionId: job.plan.id, connector: job.plan.connector, required: Boolean(job.required), status: job.state,
        attempts: job.attempts, completion: job.completion, payload: job.payload ?? null, receipt: job.receipt ?? null,
        error: job.error ?? null, nextAttemptAt: job.runAt, expiresAt: job.expiresAt }));
    const required = deliveries.filter(job => job.required);
    const chains = jobs.filter(job => job.kind === 'chain').map(job => ({ connectionId: job.plan.id, flow: job.plan.nextFlow,
        status: job.state, childTaskId: job.childTaskId ?? null, error: job.error ?? null,
        admissionAttempts: job.admissionAttempts ?? 0, nextAttemptAt: job.state === 'retry_pending' ? job.runAt : null,
        deadlineAt: job.deadlineAt ?? null }));
    const terminal = ['acknowledged', 'skipped', 'failed', 'expired', 'cancelled', 'unknown', 'held'];
    const failurePlanning = jobs.some(job => job.kind === 'failure' && ['pending', 'active'].includes(job.state)) &&
        jobs.some(connection => connection.kind === 'delivery' && connection.required && connection.plan.when === 'failure');
    const unresolvedEvaluation = ['unknown', 'cancelled'].includes(evaluation?.state);
    const requiredSettled = !failurePlanning && (unresolvedEvaluation || Boolean(task.plannedAt)) &&
        jobs.filter(job => job.kind === 'delivery' && job.required).every(job => terminal.includes(job.state) ||
            unresolvedEvaluation && job.state === 'blocked' && job.plan.when !== 'failure') &&
        !jobs.some(job => job.kind === 'chain' && job.required && ['pending', 'retry_pending', 'active'].includes(job.state));
    let status = 'running';
    if (evaluation?.state === 'unknown') status = 'unknown';
    else if (task.errors.some(error => error.stage === 'evaluation')) status = 'failed';
    else if (task.plannedAt) {
        if (required.some(job => ['unknown', 'held'].includes(job.status))) status = 'unknown';
        else if (required.some(job => ['failed', 'expired', 'cancelled'].includes(job.status))) status = 'partial';
        else if (required.every(job => ['acknowledged', 'skipped'].includes(job.status))) {
            const requiredChains = jobs.filter(job => job.kind === 'chain' && job.required);
            if (requiredChains.some(job => ['unknown', 'held'].includes(job.state))) status = 'unknown';
            else if (requiredChains.some(job => ['failed', 'expired', 'cancelled'].includes(job.state))) status = 'partial';
            else if (requiredChains.every(job => ['acknowledged', 'skipped'].includes(job.state))) status = task.errors.some(error => error.stage !== 'delivery' && !error.optional) ? 'partial' : 'completed';
        }
    }
    if (evaluation?.state === 'cancelled') status = 'cancelled';
    return freeze({ schemaVersion: 3, taskId: task.id, namespace: task.namespace, flow: task.flow,
        flowRevision: task.flowRevision, status, settled: task.state === 'complete', requiredSettled, createdAt: task.createdAt,
        evaluation: { id: task.evaluationId, status: evaluation?.state === 'unknown' ? 'unknown' : task.result !== undefined ? 'succeeded' :
            task.errors.some(error => error.stage === 'evaluation') ? 'failed' : evaluation?.state ?? 'pending', result: task.result ?? null },
        policies: task.policies, deliveries, chains, errors: task.errors, lineage: task.lineage, reply: task.reply ?? { status: 'not_requested' },
        sourceAcknowledgement: task.sourceAcknowledgement ?? { status: 'not_requested' } });
}

/** State projection reads only bounded metadata and never opens an application data part. */
export function report(record) {
    if (!record) return null;
    const { task, jobs } = record;
    const errorStats = task.errors?.stats ?? { count: task.errors?.length ?? 0,
        evaluation: task.errors?.filter(error => error.stage === 'evaluation').length ?? 0,
        requiredOther: task.errors?.filter(error => error.stage !== 'delivery' && !error.optional).length ?? 0 };
    // Reuse the business-state rules with compact observations, not hydrated values.
    const observed = reportDetails({ task: { ...task, result: task.result === undefined ? undefined : null,
        errors: [...(errorStats.evaluation ? [{ code: 'EVALUATION_ERROR', message: '', stage: 'evaluation' }] : []),
            ...(errorStats.requiredOther ? [{ code: 'EXECUTION_ERROR', message: '', stage: 'planning' }] : [])],
        policies: {}, lineage: { rootId: task.rootId, depth: task.lineage?.depth ?? 0, path: [] },
        reply: { status: task.reply?.status ?? 'not_requested' },
        sourceAcknowledgement: { status: task.sourceAcknowledgement?.status ?? 'not_requested' } },
        jobs: jobs.map(job => ({ ...job, payload: null, receipt: null, error: null, attempts: [],
            completion: { status: job.completion?.status ?? 'untracked' } })) });
    const counts = kind => {
        const out = {};
        for (const job of jobs.filter(item => item.kind === kind)) out[job.state] = (out[job.state] ?? 0) + 1;
        return out;
    };
    return freeze({ schemaVersion: 3, taskId: task.id, namespace: task.namespace, flow: task.flow,
        flowRevision: task.flowRevision, status: observed.status, settled: observed.settled,
        requiredSettled: observed.requiredSettled, createdAt: task.createdAt, updatedAt: task.updatedAt,
        evaluation: { id: task.evaluationId, status: observed.evaluation.status, hasResult: task.result !== undefined },
        policyCounts: task.policies?.stats ?? {}, deliveryCounts: counts('delivery'), chainCounts: counts('chain'),
        errorCount: errorStats.count, lineage: { rootId: task.rootId, depth: task.lineage?.depth ?? 0,
            maxDepth: task.lineage?.maxDepth ?? task.plan.maxDepth, maxExecutions: task.lineage?.maxExecutions ?? task.plan.maxExecutions },
        reply: observed.reply, sourceAcknowledgement: observed.sourceAcknowledgement });
}
