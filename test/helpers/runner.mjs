import { createRunner as createProductionRunner } from '../../src/engine/runner.mjs';
import { reportDetails } from '../../src/engine/reports.mjs';
import { createConfiguredRunner as configured } from '../../src/node/configuration.mjs';

/** Existing business-state assertions inspect complete tiny fixtures. Public summary
 * and paged-data contracts are exercised separately against the production runner. */
export function createRunner(options) {
    return inspectFixture(createProductionRunner(options));
}
export async function createConfiguredRunner(...args) { return inspectFixture(await configured(...args)); }
function inspectFixture(runner) {
    const details = async value => value?.taskId ? reportDetails(await runner.store.get(value.taskId, runner.namespace)) : value;
    const result = { ...runner };
    for (const method of ['submit', 'run', 'execute', 'get', 'retryDelivery', 'resolveDelivery', 'resolveEvaluation', 'recordCompletion',
        'cancel', 'reprocess', 'redeliver', 'reevaluate', 'importTask']) result[method] = async (...args) => details(await runner[method](...args));
    result.list = async (...args) => {
        const page = await runner.list(...args);
        return { ...page, items: await Promise.all(page.items.map(details)) };
    };
    result.preview = async (...args) => {
        const value = await runner.preview(...args);
        try {
            const record = await runner.store.snapshotRecord(runner.namespace, value.snapshot.id);
            return { ...value, revision: record.task.flowRevision, inputKind: record.task.input.kind, request: record.task.request ?? null,
                outputs: record.jobs.filter(job => job.kind === 'delivery'), policies: record.task.policies };
        } finally { await runner.closeSnapshot(value.snapshot.id); }
    };
    return result;
}

export async function collectExport(runner, taskId) {
    const chunks = []; for await (const chunk of runner.exportTask(taskId)) chunks.push(chunk); return chunks;
}
