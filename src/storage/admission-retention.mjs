import { fingerprint } from '../util.mjs';

/** Internal producers can replay admission until their durable linkage is settled. */
export async function admissionKey(task) {
    return task?.admissionKey ?? (task?.input?.idempotencyKey
        ? fingerprint([task.namespace, task.source, task.flow, task.input.idempotencyKey]) : null);
}

export async function admissionPending(tx, task, key, lock = false) {
    if (!task || !key) return false;
    const binding = task.admissionOwner;
    if (binding?.kind === 'chain') {
        const owner = await tx.get('jobs', binding.id, lock);
        return owner?.namespace === task.namespace && owner.kind === 'chain' &&
            ['blocked', 'pending', 'retry_pending', 'active', 'unknown', 'held'].includes(owner.state);
    }
    if (binding?.kind === 'aggregate') {
        const owner = await tx.get('aggregates', JSON.stringify([task.namespace, `aggregate:${binding.key}`]), lock);
        return owner?.aggregateId === binding.aggregateId && !owner.emitted && owner.state !== 'discarded';
    }
    if (binding?.kind === 'import') {
        if (lock) {
            const owner = await tx.get('owners', binding.id, true);
            return owner?.namespace === task.namespace && owner.kind === 'import' && owner.verified &&
                owner.expiresAt > Date.now() && owner.rootId === key && owner.flow === task.flow &&
                owner.runtimeRevision === task.plan.runtimeRevision;
        }
        // Several sessions can share one admission; closing one cannot release the others.
        return await tx.count('owners', { namespace: task.namespace, rootId: key,
            kind: { in: ['import', 'import-complete'] }, expiresAt: { gt: Date.now() } }) > 0;
    }
    return false;
}
