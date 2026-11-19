import { assert, bounded, id, publicError, snapshot } from '../util.mjs';
import { planOutputs } from './planning.mjs';
import { deliveryDisposition, retryDeadline, unconfirmedAcceptance } from './freshness.mjs';
import { executeChain } from './chaining.mjs';
import { incompleteReceipt, validateReceipt } from './receipts.mjs';
import { registeredEntry } from './registry.mjs';

/** Storage failures must escape evaluator handling and retain the write-ahead state. */
async function evaluate(task, job, { store, options, signal }) {
    const evaluator = registeredEntry(options.evaluators, task.plan.evaluator);
    const deadline = Math.min(Date.now() + task.plan.timeoutMs,
        task.input.deadlineAt ? Date.parse(task.input.deadlineAt) : Infinity);
    try {
        assert(evaluator?.revision === task.plan.evaluatorRevision && typeof evaluator.execute === 'function',
            'Evaluator revision is unavailable.', 'REVISION_UNAVAILABLE');
        assert(deadline > Date.now(), 'Evaluation deadline elapsed.', 'EXPIRED');
    } catch (error) { return { error }; }
    await store.commit(job, { job: { stage: 'invoked' } });
    let result; let invoked = false;
    try {
        // Marker persistence consumes the same budget as the provider operation.
        const timeoutMs = deadline - Date.now();
        assert(timeoutMs > 0, 'Evaluation deadline elapsed before provider dispatch.', 'EXPIRED');
        result = snapshot(await bounded(child => { invoked = true; return evaluator.execute(task.request, { signal: child, timeoutMs }); },
            { signal, timeoutMs, stage: 'evaluation' }), { maxBytes: store.limits.maxBytes });
    } catch (error) { return { error, invoked }; }
    const updated = { ...task, result, evaluatedAt: Date.now() };
    await store.commit(job, { task: { result, evaluatedAt: updated.evaluatedAt }, job: { stage: 'result_saved' } });
    return { task: updated };
}

/** Execute a lease with renewal. External effects are always outside store transactions. */
export async function executeClaim(claim, { store, options, signal, onEvent, chain }) {
    const { job } = claim;
    let task = claim.task;
    const controller = new AbortController();
    const combined = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal;
    let renewing = false;
    const timer = setInterval(async () => {
        if (renewing) return;
        renewing = true;
        try { await store.renew(job); }
        catch { controller.abort(); }
        finally { renewing = false; }
    }, Math.max(10, Math.floor(store.limits.leaseMs / 3)));
    try {
        if (job.kind === 'failure') {
            const record = await store.get(task.id, task.namespace, { jobData: false });
            const planned = await planOutputs(record.task, options, { failureOnly: true, signal: combined, maxBytes: store.limits.maxBytes });
            planned.jobs = planned.jobs.filter(update => ['blocked', 'skipped'].includes(record.jobs.find(item => item.id === update.id)?.state));
            await store.commit(job, { ...planned, job: { state: 'acknowledged' } });
        } else if (job.kind === 'chain') {
            await executeChain(task, job, { store, chain, signal: combined });
        } else if (job.kind === 'evaluation') {
            if (task.result === undefined && task.errors.length === 0) {
                const outcome = await evaluate(task, job, { store, options, signal: combined });
                if (Object.hasOwn(outcome, 'error')) {
                    const { error } = outcome;
                    const failure = publicError(error, 'evaluation');
                    if (failure.code === 'STALE_LEASE') throw error;
                    // Once invoked, a code cannot prove that the provider did not act.
                    // Explicit definite rejection also applies to timeout/abort errors.
                    const unknown = Boolean(outcome.invoked) && failure.ambiguous !== false;
                    task = { ...task, errors: [...task.errors, failure] };
                    await store.commit(job, { task: { errors: task.errors }, job: { state: unknown ? 'unknown' : 'active', stage: 'failed' } });
                    if (unknown) return;
                } else task = outcome.task;
            }
            const planned = await planOutputs(task, options, { signal: combined, maxBytes: store.limits.maxBytes });
            const current = await store.get(task.id, task.namespace, { jobData: false });
            planned.jobs = planned.jobs.filter(update => current.jobs.find(item => item.id === update.id)?.state === 'blocked');
            await store.commit(job, { ...planned, job: { state: 'acknowledged', stage: 'planned', token: null, leaseUntil: null } });
            await onEvent?.({ type: 'evaluation.settled', taskId: task.id, flow: task.flow,
                durationMs: Date.now() - task.createdAt, status: task.result !== undefined ? 'succeeded' : 'failed' });
        } else {
            // Older pending records may have been retried after planning failed. Never dispatch unvalidated output.
            if (!Object.hasOwn(job, 'payload')) {
                const ambiguous = unconfirmedAcceptance(job);
                await store.commit(job, { job: { state: ambiguous ? 'unknown' : 'failed', stage: 'prepared', token: null, leaseUntil: null,
                    error: { code: 'OUTPUT_NOT_PREPARED', message: 'Delivery has no prepared payload.', stage: 'output-planning', ambiguous } } });
                return;
            }
            const connector = registeredEntry(options.connectors, job.plan.connector);
            if (connector?.revision !== job.plan.connectorRevision || typeof connector.send !== 'function') {
                await store.commit(job, { job: { state: 'held', error: { code: 'REVISION_UNAVAILABLE', message: 'Connector revision is unavailable.' } } });
                return;
            }
            const attempt = { id: id('attempt'), status: 'in_flight', startedAt: Date.now() };
            const attempts = [...job.attempts, attempt];
            await store.commit(job, { job: { stage: 'invoked', attempts } });
            const expiry = deliveryDisposition(task, job);
            if (expiry) {
                // Persistence can itself outlast the remaining budget; no recipient was invoked.
                await store.commit(job, { job: { state: expiry, stage: 'prepared', attempts: job.attempts, token: null, leaseUntil: null } });
                return;
            }
            let disposition; let acceptance; let accepting = true;
            try {
                const receipt = await bounded(signal => connector.send(job.payload, {
                    signal, taskId: task.id, eventId: task.id, deliveryId: job.id, attemptId: attempt.id,
                    idempotencyKey: job.id, destination: job.plan.destination, encoding: job.plan.encoding,
                    async onAcceptance(value) {
                        assert(accepting && !signal.aborted && !combined.aborted, 'Acceptance callback is no longer active.', 'STALE_ATTEMPT');
                        assert(!acceptance, 'Acceptance was already recorded for this attempt.', 'CONFLICT');
                        const checked = validateReceipt(value, store.limits.maxBytes);
                        assert(checked.status === 'acknowledged', 'Acceptance requires an acknowledged receipt.', 'INVALID_RECEIPT');
                        acceptance = checked;
                        await store.commit(job, { job: { acceptance: { attemptId: attempt.id, receipt: checked },
                            attempts: [...job.attempts, { ...attempt, status: 'acknowledged' }] } });
                    },
                }), { signal: combined, timeoutMs: Math.max(1, Math.min(job.plan.timeoutMs, job.expiresAt - Date.now())), stage: 'delivery' });
                const saved = validateReceipt(receipt, store.limits.maxBytes);
                assert(!acceptance || saved.status === 'acknowledged', 'A final receipt cannot withdraw confirmed acceptance.', 'INVALID_RECEIPT');
                attempt.status = saved.status === 'acknowledged' ? 'acknowledged' : 'unknown';
                disposition = { state: attempt.status, receipt: saved, completion: saved.completion ?? { status: 'untracked' } };
                if (attempt.status === 'acknowledged') disposition.error = null;
            } catch (error) {
                // An error code alone cannot prove that the recipient did not act.
                // This also covers failures while validating or saving its receipt.
                const failure = publicError(error, 'delivery');
                if (acceptance) {
                    const receipt = incompleteReceipt(acceptance);
                    attempt.status = 'acknowledged';
                    disposition = { state: 'acknowledged', receipt, completion: receipt.completion ?? { status: 'untracked' }, error: null };
                } else {
                    const unknown = failure.ambiguous !== false;
                    attempt.status = unknown ? 'unknown' : 'failed';
                    attempt.error = failure;
                    disposition = { state: attempt.status, error: attempt.error };
                    if (!unknown && failure.retryable === false) disposition.exhausted = true;
                }
            } finally { accepting = false; }
            attempt.finishedAt = Date.now();
            if (disposition.state === 'failed' && unconfirmedAcceptance({ ...job, attempts })) {
                disposition.state = 'unknown';
                disposition.error = { code: 'ACCEPTANCE_UNCONFIRMED', stage: 'delivery', ambiguous: true,
                    message: 'An earlier delivery attempt has no confirmed acceptance outcome.' };
            }
            const retry = job.plan.retry;
            if (attempts.length < retry.maxAttempts && !disposition.exhausted &&
                (disposition.state === 'failed' || disposition.state === 'unknown' && retry.ambiguous === 'retry')) {
                disposition.state = 'retry_pending';
                const backoff = Math.min(retry.maxDelayMs, retry.delayMs * 2 ** (attempts.length - 1));
                disposition.runAt = Math.min(retryDeadline(task, job), Date.now() + Math.max(attempt.error?.retryAfterMs ?? 0,
                    Math.ceil(backoff * (0.5 + Math.random() * 0.5))));
            }
            await store.commit(job, { job: { ...disposition, attempts, stage: 'settled', token: null, leaseUntil: null } });
            await onEvent?.({ type: 'delivery.settled', taskId: task.id, flow: task.flow, deliveryId: job.id, status: disposition.state, durationMs: attempt.finishedAt - attempt.startedAt });
        }
    } finally { clearInterval(timer); }
}
