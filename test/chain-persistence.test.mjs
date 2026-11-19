import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) test(`${name}: chain recovery retains one child after uncertain persistence`, { timeout: 30000 }, async () => {
    const store = await factory();
    try {
        for (const mode of ['before-link', 'after-link', 'child-readback', 'budget-race']) {
            const failed = Promise.withResolvers(); const waiter = new AbortController(); let inject = true; let calls = 0;
            const runner = createRunner({ namespace: `chain-write-${crypto.randomUUID()}`, store: { ...store,
                async findChildAdmission(...args) {
                    // Model admission becoming visible between lookup and terminal settlement.
                    if (mode === 'budget-race' && !inject) return null;
                    return store.findChildAdmission(...args);
                },
                async commit(claim, update) {
                    if (inject && mode !== 'child-readback' && claim.kind === 'chain' && update.job?.state === 'acknowledged') {
                        inject = false; if (mode === 'after-link') await store.commit(claim, update);
                        throw new Error('Link response lost.');
                    }
                    return store.commit(claim, update);
                },
                async get(...args) {
                    const saved = await store.get(...args);
                    if (inject && mode === 'child-readback' && saved?.task.flow === 'child') { inject = false; throw null; }
                    return saved;
                },
            }, onEvent(event) { if (event.type === 'worker.error') { failed.resolve(); waiter.abort(); } },
            validators: { v: { revision: '1', validate: value => value } },
            evaluators: { e: { revision: '1', execute: async value => { calls++; return value; } } },
            connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged', receipt: { response: 42 } })) },
            flows: { parent: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                { id: 'next', connector: 'sink', when: 'always', payload: 'raw-result', nextFlow: 'child', chainRetry: { maxAttempts: 1, delayMs: 10, maxDelayMs: 50 } },
            ] }, child: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
            try {
                const parent = await runner.submit({ flow: 'parent', kind: 'result', result: 1 });
                await runner.execute(parent.taskId, { signal: waiter.signal }).catch(error => {
                    assert(error.name === 'AbortError' || error.code === 'ABORTED');
                });
                await bounded(() => failed.promise, { timeoutMs: 5000 }); assert.equal(inject, false);
                const job = (await store.get(parent.taskId, runner.namespace)).jobs.find(item => item.kind === 'chain');
                assert.equal(job.state, mode === 'after-link' ? 'acknowledged' : 'active');
                assert.equal((await runner.list()).items.length, 2); assert.equal(calls, 0);
                if (job.state === 'active') await store.commit(job, { job: { leaseUntil: 0 } });
                await runner.recover();
                const resumed = await runner.execute(parent.taskId);
                assert.equal(resumed.status, 'completed'); assert(resumed.chains[0].childTaskId);
                assert.equal((await runner.list()).items.length, 2);
                assert.equal((await runner.execute(resumed.chains[0].childTaskId)).evaluation.result, 42);
                await runner.recover(); await runner.execute(parent.taskId); assert.equal(calls, 1);
            } finally { waiter.abort(); await runner.close(); }
        }
    } finally { await store.close(); }
});
