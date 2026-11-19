import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createCallbackConnector } from '../src/index.mjs';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';

test('a pending legacy failure job hydrates and retains its triggering diagnostic before planning', async () => {
    const repository = createMemoryRepository(); const store = createExecutionStore(repository); const notices = [];
    const runner = createRunner({ store, validators: { v: { revision: '1', validate: value => value } }, connectors: {
        sink: createCallbackConnector(value => { notices.push(value); return { status: 'acknowledged' }; }) },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
        { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
        { id: 'notice', connector: 'sink', when: 'failure', payload: 'failure' },
    ] } } });
    try {
        const { taskId } = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
        const empty = (await store.get(taskId, runner.namespace, { metadata: true })).task.errors;
        const claim = await store.claim(runner.namespace, 'evaluation', { taskId, worker: 'setup' });
        await store.commit(claim.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' }, jobs: [
            { id: `${taskId}:delivery:out`, state: 'failed', error: { code: 'REJECTED', message: 'Legacy message.', stage: 'delivery' } },
            { id: `${taskId}:delivery:notice`, state: 'skipped' },
        ] });
        // Reconstruct the earlier schema-v3 shape without invoking a legacy writer.
        await repository.transaction(async tx => {
            const task = await tx.get('tasks', taskId); task.errors = empty; await tx.put('tasks', task);
            const source = await tx.get('jobs', `${taskId}:delivery:out`); delete source.diagnosticRecorded; await tx.put('jobs', source);
            const failure = await tx.get('jobs', `${taskId}:failure`); failure.error = source.error; await tx.put('jobs', failure);
        });
        const report = await runner.execute(taskId, { waitFor: 'settled', waitMs: 3000 });
        assert.equal(report.errorCount, 1); assert.equal(notices.length, 1); assert.equal(notices[0].errors[0].message, 'Legacy message.');
        assert.equal(notices[0].errors[0].$data, undefined);
    } finally { await runner.close(); await store.close(); }
});
