import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', options => createMemoryStore(options)], ['sqlite', options => createSQLiteStore(':memory:', options)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', options => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...options })]] : [])];

for (const [name, factory] of stores) test(`${name}: operator evidence is bounded before durable delivery mutation`, async () => {
    const store = await factory({ maxBytes: 1024 }); let calls = 0;
    const runner = createRunner({ store, namespace: `operator-evidence-${crypto.randomUUID()}`,
        validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async () => { calls++; return { status: 'unknown' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
        ] } } });
    try {
        const original = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        const deliveryId = original.deliveries[0].id;
        await assert.rejects(runner.resolveDelivery(deliveryId, { status: 'acknowledged', receipt: 'x'.repeat(1024) }), { code: 'RESOURCE_LIMIT' });
        await assert.rejects(runner.resolveDelivery(deliveryId, { status: 'acknowledged', receipt: { invalid: undefined } }), { code: 'VALIDATION_ERROR' });
        assert.deepEqual(await runner.get(original.taskId), original);
        const accepted = await runner.resolveDelivery(deliveryId, { status: 'acknowledged', receipt: false });
        assert.equal(accepted.deliveries[0].receipt.receipt, false);
        await assert.rejects(runner.recordCompletion(deliveryId, { status: 'completed', evidence: 'x'.repeat(1024) }), { code: 'RESOURCE_LIMIT' });
        await assert.rejects(runner.recordCompletion(deliveryId, { status: 'completed', evidence: { invalid: undefined } }), { code: 'VALIDATION_ERROR' });
        assert.deepEqual(await runner.get(original.taskId), accepted);
        const completed = await runner.recordCompletion(deliveryId, { status: 'completed', evidence: null });
        assert.deepEqual(completed.deliveries[0].completion, { status: 'completed', evidence: null });
        assert.equal(calls, 1);
    } finally { await runner.close(); await store.close(); }
});
