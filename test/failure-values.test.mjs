import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';
import { createRunner as createProductionRunner } from '../src/engine/runner.mjs';

const stores = [['memory', async () => ({ store: createMemoryStore(), cleanup: async () => {} })], ['sqlite', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-failure-values-'));
    return { store: createSQLiteStore(join(directory, 'state.sqlite')), cleanup: () => rm(directory, { recursive: true }) };
}], ...(process.env.DBC_POSTGRES_URL ? [['postgres', async () => ({
    store: await createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL }), cleanup: async () => {},
})]] : [])];

for (const [name, factory] of stores) test(`${name}: failure notification planning preserves each policy diagnostic once`, async () => {
    const { store, cleanup } = await factory(); let notices = 0;
    const runner = createProductionRunner({ store,
        validators: { v: { revision: '1', validate: value => value } },
        policies: { bad: { revision: '1', execute() { throw new ConnectorError('POLICY_FAILURE', 'Policy failed.'); } } },
        connectors: {
            reject: createCallbackConnector(() => { throw new ConnectorError('REJECTED', 'Recipient refused.', { ambiguous: false, retryable: false }); }),
            notice: createCallbackConnector(() => { notices++; return { status: 'acknowledged' }; }),
        },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v',
            policies: { first: { type: 'module', name: 'bad' }, second: { type: 'module', name: 'bad' } },
            connections: [{ id: 'out', connector: 'reject', when: 'always', payload: 'raw-result' },
                { id: 'notice', connector: 'notice', when: 'failure', payload: 'failure' }] } },
    });
    try {
        const result = await runner.run({ flow: 'a', kind: 'result', result: 1 }, { waitFor: 'settled' });
        assert.equal(result.status, 'partial'); assert.equal(notices, 1); assert.equal(result.errorCount, 3);
        const saved = await store.get(result.taskId, runner.namespace);
        assert.deepEqual(saved.task.errors.map(error => error.code), ['POLICY_FAILURE', 'POLICY_FAILURE', 'REJECTED']);
    } finally { await runner.close(); await store.close(); await cleanup(); }
});

for (const [name, factory] of stores) test(`${name}: arbitrary thrown values settle invoked effects conservatively`, { timeout: 60000 }, async () => {
    const { store, cleanup } = await factory();
    const unreadable = Object.defineProperty(new ConnectorError('FAILURE', 'Private failure.'), 'code', { get() { throw new Error('Unreadable error.'); } });
    try {
        for (const thrown of [null, undefined, 'private failure', unreadable]) {
            let evaluations = 0; let sends = 0;
            const runner = createRunner({ store, namespace: `failure-value-${crypto.randomUUID()}`,
                validators: { v: { revision: '1', validate: value => value } },
                evaluators: { e: { revision: '1', execute() { evaluations++; throw thrown; } } },
                connectors: { sink: createCallbackConnector(() => { sends++; throw thrown; }) },
                flows: { a: { revision: '1', inputs: ['request', 'result'], validator: 'v', evaluator: 'e', connections: [
                    { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
                ] } } });
            try {
                // This tests terminal error classification, not a short caller wait budget.
                const delivered = await runner.run({ flow: 'a', kind: 'result', result: 1 });
                assert.equal(delivered.status, 'unknown'); assert.equal(delivered.deliveries[0].status, 'unknown');
                assert.equal(delivered.deliveries[0].attempts[0].status, 'unknown');
                assert.equal(delivered.deliveries[0].error.code, 'UNEXPECTED_ERROR');
                await runner.tick({ taskId: delivered.taskId }); assert.equal(sends, 1);
                const evaluated = await runner.run({ flow: 'a', kind: 'request', request: 1 });
                assert.equal(evaluated.status, 'unknown'); assert.equal(evaluations, 1);
                assert.equal(evaluated.errors[0].code, 'UNEXPECTED_ERROR');
                if (store.capabilities.durable) {
                    const ack = await runner.acknowledge(delivered.taskId, () => { throw thrown; });
                    assert.equal(ack.status, 'unknown'); assert.equal(ack.error.code, 'UNEXPECTED_ERROR');
                    assert.equal((await runner.get(delivered.taskId)).sourceAcknowledgement.status, 'unknown');
                }
            } finally { await runner.close(); }
        }
    } finally { await store.close(); await cleanup(); }
});

test('the worker observer handles a storage operation that rejects with null', async () => {
    const store = createMemoryStore(); const observed = Promise.withResolvers(); let fail = true; let calls = 0;
    const runner = createRunner({ store: { ...store, async commit(claim, update) {
        if (fail && update.job?.stage === 'invoked') { fail = false; throw null; }
        return store.commit(claim, update);
    } }, onEvent(event) { if (event.type === 'worker.error') observed.resolve(event); },
    evaluators: { e: { revision: '1', execute: async () => { calls++; return 1; } } },
    flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e' } } });
    try {
        const task = await runner.submit({ flow: 'a', kind: 'request', request: 1 }); await runner.tick({ taskId: task.taskId });
        assert.equal((await bounded(() => observed.promise, { timeoutMs: 1000 })).error.code, 'UNEXPECTED_ERROR');
        const job = (await store.get(task.taskId, runner.namespace)).jobs.find(item => item.kind === 'evaluation');
        await store.commit(job, { job: { leaseUntil: 0 } }); await runner.recover();
        assert.equal((await runner.execute(task.taskId)).status, 'completed'); assert.equal(calls, 1);
    } finally { await runner.close(); await store.close(); }
});
