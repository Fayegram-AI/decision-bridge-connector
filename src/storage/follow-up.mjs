import { chainReady } from '../engine/chaining.mjs';
import { captureDiagnostic, errorEntries, errorIndex } from './diagnostics.mjs';

/** Completion, expiry and recovery publish the same diagnostic and follow-up work. */
export function createFollowUp(data) {
    return async function followUp(tx, job, planningFailed = false) {
        if (job.kind === 'delivery' && job.plan.nextFlow) {
            const state = job.state === 'acknowledged' ? 'pending' :
                ['failed', 'expired', 'cancelled'].includes(job.state) ? 'skipped' : null;
            if (state) {
                const chained = await tx.get('jobs', `${job.taskId}:chain:${job.plan.id}`);
                if (chained?.state === 'blocked' || chained?.state === 'skipped')
                    await tx.put('jobs', state === 'pending' ? chainReady(chained, Date.now()) : { ...chained, state });
            }
        }
        const terminal = job.kind === 'delivery' && ['failed', 'unknown', 'expired', 'cancelled'].includes(job.state) ||
            job.kind === 'chain' && ['failed', 'expired'].includes(job.state);
        const uncertainEvaluation = job.kind === 'evaluation' && job.state === 'unknown';
        if ((terminal || uncertainEvaluation) && !job.diagnosticRecorded) {
            const task = await tx.get('tasks', job.taskId, true);
            const entries = await errorEntries(data, tx, task);
            const diagnostic = job.error ? await data.hydrate(tx, job.namespace, job.error) : {
                code: `${job.kind.toUpperCase()}_${job.state.toUpperCase()}`, stage: job.kind,
                message: `${job.kind === 'chain' ? 'Child admission' : job.kind === 'evaluation' ? 'Evaluation' : 'Delivery'} has no confirmed successful outcome.` };
            // Output planning has already retained its own error, including legacy records.
            if (diagnostic.stage !== 'output-planning' && !entries.some(error => error.originJobId === job.id ||
                uncertainEvaluation && error.stage === 'evaluation')) {
                entries.push(await data.putSmall(tx, job.namespace,
                    captureDiagnostic({ ...diagnostic, originJobId: job.id, optional: !job.required }), 'diagnostic',
                    { code: diagnostic.code, stage: diagnostic.stage ?? job.kind, originJobId: job.id, optional: !job.required }));
                task.errors = await errorIndex(data, tx, job.namespace, entries);
                await tx.put('tasks', task);
            }
            const retained = { ...job, diagnosticRecorded: true };
            if (retained.error === undefined) delete retained.error;
            await tx.put('jobs', retained);
        }
        const failed = terminal && job.plan?.when !== 'failure' ||
            job.kind === 'evaluation' && (job.state === 'unknown' || planningFailed);
        if (!failed) return;
        const failure = await tx.get('jobs', `${job.taskId}:failure`, true);
        if (failure?.state === 'skipped') await tx.put('jobs', { ...failure, state: 'pending' });
    };
}
