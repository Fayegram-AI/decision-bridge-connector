import { snapshot } from '../util.mjs';

/** Origin metadata must fit the same budget as the diagnostic it annotates. */
export function captureDiagnostic(value) {
    try { return snapshot(value, { maxBytes: 65536 }); }
    catch {
        const names = ['code', 'stage', 'ambiguous', 'retryable', 'optional', 'originJobId'];
        return { ...Object.fromEntries(names.filter(name => value?.[name] !== undefined).map(name => [name, value[name]])),
            message: 'Error details exceeded the diagnostic retention limit.', detailsUnavailable: true };
    }
}

/** Diagnostic indexes are bounded metadata; update them under the task lock. */
export async function errorEntries(data, tx, task) {
    return task.errors?.$data ? data.read(tx, task.namespace, task.errors.$data) : task.errors ?? [];
}

export async function errorIndex(data, tx, namespace, entries) {
    return data.putSmall(tx, namespace, entries, 'index', { stats: { count: entries.length,
        evaluation: entries.filter(error => error.stage === 'evaluation').length,
        requiredOther: entries.filter(error => error.stage !== 'delivery' && !error.optional).length } });
}

/** Merge captured histories as multisets: preserve equal legacy diagnostics and new origins. */
export async function mergeErrors(data, tx, task, incoming) {
    const existing = await errorEntries(data, tx, task);
    const received = await errorEntries(data, tx, { ...task, errors: incoming });
    const counts = new Map();
    const key = error => error.$data?.sha256 ?? JSON.stringify(error);
    for (const error of existing) counts.set(key(error), (counts.get(key(error)) ?? 0) + 1);
    const merged = [...existing];
    for (const error of received) {
        const identity = key(error); const available = counts.get(identity) ?? 0;
        if (available) counts.set(identity, available - 1); else merged.push(error);
    }
    return errorIndex(data, tx, task.namespace, merged);
}
