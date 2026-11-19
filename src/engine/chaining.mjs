import { assert, bounded, publicError } from '../util.mjs';

/** Admission policy is independent of recipient delivery and has no implicit budget. */
export function validateChainRetry(policy) {
    assert(policy && typeof policy === 'object' && !Array.isArray(policy), 'nextFlow requires an explicit chainRetry policy.');
    assert(Object.keys(policy).every(key => ['delayMs', 'maxDelayMs', 'maxAttempts', 'timeoutMs'].includes(key)), 'Unknown chainRetry field.');
    assert(policy.maxAttempts !== undefined || policy.timeoutMs !== undefined, 'chainRetry requires maxAttempts or timeoutMs.');
    for (const key of ['delayMs', 'maxDelayMs', 'maxAttempts', 'timeoutMs']) {
        if (['delayMs', 'maxDelayMs'].includes(key) || policy[key] !== undefined)
            assert(Number.isSafeInteger(policy[key]) && policy[key] > 0 &&
                (key === 'maxAttempts' || policy[key] <= 2147483647), `Invalid chainRetry.${key}.`);
    }
    assert(policy.maxDelayMs >= policy.delayMs, 'chainRetry.maxDelayMs must be at least delayMs.');
}

export function chainReady(job, timestamp) {
    return { ...job, state: 'pending', runAt: timestamp, readyAt: job.readyAt ?? timestamp,
        admissionAttempts: job.admissionAttempts ?? 0,
        deadlineAt: job.deadlineAt ?? (job.plan.chainRetry?.timeoutMs ? timestamp + job.plan.chainRetry.timeoutMs : null) };
}

export function chainBudget(job, timestamp = Date.now()) {
    if (job.deadlineAt !== null && job.deadlineAt !== undefined && job.deadlineAt <= timestamp)
        return { state: 'expired', error: { code: 'CHAIN_ADMISSION_EXPIRED', message: 'Child admission deadline elapsed.', stage: 'chain' } };
    if (job.plan.chainRetry.maxAttempts !== undefined && job.admissionAttempts >= job.plan.chainRetry.maxAttempts)
        return { state: 'failed', error: { code: 'CHAIN_ADMISSION_EXHAUSTED', message: 'Child admission attempts exhausted.', stage: 'chain' } };
    return null;
}

/** Persist bounded admission state; replay uses the stable chain job identity. */
export async function executeChain(task, job, { store, chain, signal }) {
    const finish = disposition => store.commit(job, { job: { ...disposition, token: null, leaseUntil: null } });
    const existing = await store.findChildAdmission(task.namespace, job);
    if (existing) return finish({ state: 'acknowledged', childTaskId: existing, error: null });
    const exhausted = chainBudget(job);
    if (exhausted) return finish(exhausted);
    const admissionAttempts = (job.admissionAttempts ?? 0) + 1;
    await store.commit(job, { job: { admissionAttempts } });
    let child; let admissionSignal;
    try {
        const delivered = await store.getJob(task.namespace, job.deliveryId);
        const response = delivered?.receipt?.receipt?.response;
        assert(response !== undefined, 'Chaining requires a captured downstream response.', 'RESPONSE_UNAVAILABLE');
        if (job.deadlineAt !== null && job.deadlineAt !== undefined) {
            assert(job.deadlineAt > Date.now(), 'Child admission deadline elapsed.', 'ADMISSION_EXPIRED');
            child = await bounded(childSignal => {
                admissionSignal = childSignal;
                return chain(job.plan.nextFlow, response, task, job, childSignal);
            }, {
                signal, timeoutMs: Math.max(1, job.deadlineAt - Date.now()), stage: 'chain',
            });
        } else child = await chain(job.plan.nextFlow, response, task, job, signal);
    } catch (error) {
        const failure = publicError(error, 'chain');
        if (['STALE_LEASE', 'ABORTED', 'UNEXPECTED_ERROR'].includes(failure.code) || signal.aborted) throw error;
        // The owned monotonic timeout may fire while wall time still precedes the saved deadline.
        // A nested timeout before that boundary still requires admission lookup during recovery.
        if (failure.code === 'TIMEOUT' && !admissionSignal?.aborted && (job.deadlineAt == null || job.deadlineAt > Date.now())) throw error;
        const exhausted = chainBudget({ ...job, admissionAttempts });
        if (failure.code === 'ADMISSION_EXPIRED' || failure.code === 'TIMEOUT') return finish({ state: 'expired', error: {
            code: 'CHAIN_ADMISSION_EXPIRED', message: 'Child admission deadline elapsed.', stage: 'chain' } });
        if (failure.code !== 'RESOURCE_LIMIT' || failure.retryable !== true || failure.ambiguous !== false)
            return finish({ state: 'failed', error: failure });
        if (exhausted) return finish(exhausted);
        const retry = job.plan.chainRetry;
        const backoff = Math.min(retry.maxDelayMs, retry.delayMs * 2 ** Math.min(1023, admissionAttempts - 1));
        return finish({ state: 'retry_pending', error: failure,
            runAt: Math.min(job.deadlineAt ?? Infinity, Date.now() + Math.ceil(backoff * (0.5 + Math.random() * 0.5))) });
    }
    // Link persistence failures retain the lease for idempotent crash recovery.
    return finish({ state: 'acknowledged', childTaskId: child.taskId, error: null });
}
