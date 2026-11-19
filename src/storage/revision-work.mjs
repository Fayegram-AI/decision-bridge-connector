export const unsettled = ['blocked', 'pending', 'retry_pending', 'active', 'unknown', 'held'];
export const recoveryAwareQueries = new WeakSet();

export function hasRetryAllowance(job, timestamp) {
    return job.attempts.length < job.plan.retry.maxAttempts && job.expiresAt > timestamp;
}

/** A failed delivery may still create work through retry or chain reconciliation. */
export async function needsRevision(job, findChain, timestamp) {
    if (unsettled.includes(job.state)) return true;
    if (job.state !== 'failed' || job.kind !== 'delivery') return false;
    if (Object.hasOwn(job, 'payload') && hasRetryAllowance(job, timestamp)) return true;
    if (!job.plan.nextFlow) return false;
    const chain = await findChain(`${job.taskId}:chain:${job.plan.id}`);
    return chain?.state === 'skipped' || unsettled.includes(chain?.state);
}

/** Older custom stores can supply the same facts through their original read API. */
export async function hasRevisionWork(store, namespace, flow) {
    if (await store.pendingJobs(namespace, flow.runtimeRevision)) return true;
    if (recoveryAwareQueries.has(store.pendingJobs))
        return Boolean(await store.pendingJobs(namespace, flow.runtimeRevision, { includeRecoverable: true }));
    let after;
    do {
        // Scan both task states. A concurrent retry can change the state between reads.
        const page = await store.list(namespace, { flow: flow.id, after, limit: 100 });
        for (const task of page.items) {
            if (task.plan?.runtimeRevision && task.plan.runtimeRevision !== flow.runtimeRevision) continue;
            const saved = await store.get(task.id, namespace, { metadata: true });
            if (!saved) continue;
            for (const job of saved.jobs) {
                if (job.revision === flow.runtimeRevision &&
                    await needsRevision(job, id => saved.jobs.find(candidate => candidate.id === id), Date.now())) return true;
            }
        }
        after = page.next;
    } while (after);
    return false;
}
