/** Replay limits cannot settle uncertain recipient effects from earlier attempts. */
export function unconfirmedAcceptance(job) {
    return job.attempts.slice(job.resolvedAttempts ?? 0).some(attempt => ['unknown', 'in_flight'].includes(attempt.status));
}

export function staleDisposition(task, timestamp = Date.now()) {
    return task.plan.maxAgeMs !== undefined && timestamp - Date.parse(task.evidenceAt) > task.plan.maxAgeMs
        ? task.plan.stale === 'expire' ? 'expired' : 'held' : null;
}

/** Wake a delayed retry in time to settle its stopping deadline without replaying early. */
export function retryDeadline(task, job) {
    return Math.min(job.expiresAt, job.plan.when === 'failure' || task.plan.maxAgeMs === undefined ? Infinity
        : Date.parse(task.evidenceAt) + task.plan.maxAgeMs + 1);
}

export function deliveryDisposition(task, job, timestamp = Date.now()) {
    const stopped = job.expiresAt <= timestamp ? 'expired' : job.plan.when === 'failure' ? null : staleDisposition(task, timestamp);
    // Expiry prevents another attempt; it cannot settle an earlier recipient effect.
    return stopped && unconfirmedAcceptance(job) ? 'unknown' : stopped;
}
