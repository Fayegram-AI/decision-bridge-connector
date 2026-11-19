import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) test(`${name}: result persistence failures preserve recoverable evidence before any output`, { timeout: 30000 }, async context => {
    const store = await factory();
    try {
        for (const committed of [false, true]) {
            let calls = 0; let sends = 0; let inject = true; const failed = Promise.withResolvers();
            const runner = createRunner({ store: { ...store, async commit(claim, update) {
                if (inject && update.job?.stage === 'result_saved') {
                    inject = false;
                    if (committed) await store.commit(claim, update);
                    throw new Error('Result persistence response lost.');
                }
                return store.commit(claim, update);
            } }, namespace: `result-write-${crypto.randomUUID()}`,
            onEvent(event) { if (event.type === 'worker.error') failed.resolve(); },
            evaluators: { e: { revision: '1', execute: async () => { calls++; return { value: 42 }; } } },
            connectors: { sink: createCallbackConnector(async () => { sends++; return { status: 'acknowledged' }; }) },
            flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e', connections: [
                { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
            ] } } });
            try {
                const admitted = await runner.submit({ flow: 'a', kind: 'request', request: {} });
                await runner.tick({ taskId: admitted.taskId }); await failed.promise;
                const saved = await store.get(admitted.taskId, runner.namespace);
                const job = saved.jobs.find(item => item.kind === 'evaluation');
                assert.equal(job.state, 'active'); assert.equal(job.stage, committed ? 'result_saved' : 'invoked');
                assert.deepEqual(saved.task.errors, []); assert.equal(saved.task.plannedAt, undefined);
                assert.equal(sends, 0); assert.equal(calls, 1);
                assert.deepEqual(saved.task.result, committed ? { value: 42 } : undefined);
                await store.commit(job, { job: { leaseUntil: 0 } }); await runner.recover();
                let result;
                // A wait budget bounds observation; it does not guarantee settlement.
                // The surrounding test timeout still bounds this recovery check.
                do { result = await runner.execute(admitted.taskId, { waitMs: 1000, signal: context.signal }); } while (result.status === 'running');
                assert.equal(result.status, committed ? 'completed' : 'unknown');
                assert.equal(calls, 1); assert.equal(sends, committed ? 1 : 0);
            } finally { await runner.close(); }
        }
    } finally { await store.close(); }
});

test('evaluation dispatch rechecks explicit and operation deadlines after marker persistence', async t => {
    let clock = Date.now(); t.mock.method(Date, 'now', () => clock);
    for (const mode of ['deadline', 'budget', 'remaining']) {
        const store = createMemoryStore(); let calls = 0; let markers = 0;
        const start = clock; const timeoutMs = mode === 'budget' ? 200 : 2000;
        const runner = createRunner({ store: { ...store, async commit(claim, update) {
            await store.commit(claim, update);
            if (update.job?.stage === 'invoked') { markers++; clock += mode === 'remaining' ? 400 : 201; }
        } }, evaluators: { e: { revision: '1', async execute(request, context) {
            calls++; assert.equal(context.timeoutMs, 600); return 1;
        } } }, flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e', timeoutMs, overrides: ['deadlineAt'] } } });
        try {
            const result = await runner.run({ flow: 'a', kind: 'request', request: {},
                ...(mode === 'budget' ? {} : { deadlineAt: new Date(start + (mode === 'remaining' ? 1000 : 200)).toISOString() }) });
            assert.equal(markers, 1); assert.equal(calls, mode === 'remaining' ? 1 : 0);
            assert.equal(result.status, mode === 'remaining' ? 'completed' : 'failed');
            if (mode !== 'remaining') assert.equal(result.errors[0].code, 'EXPIRED');
        } finally { await runner.close(); await store.close(); }
    }
});
