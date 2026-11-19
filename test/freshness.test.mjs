import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];
for (const [name, factory] of stores) test(`${name}: queued evidence is rechecked for freshness before delivery`, async () => {
    const store = await factory(); let sends = 0;
    for (const stale of ['hold', 'expire']) {
        const runner = createRunner({ store, namespace: `freshness-${crypto.randomUUID()}`, validators: { v: { revision: '1', validate: value => value } },
            connectors: { sink: createCallbackConnector(async () => { sends++; return { status: 'acknowledged' }; }) },
            flows: { a: { revision: '1', inputs: ['result'], validator: 'v', maxAgeMs: 1000, stale,
                connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] } } });
        try {
            const admitted = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
            const planning = await store.claim(runner.namespace, 'evaluation', { worker: 'planner', taskId: admitted.taskId });
            // Persist a planned delivery whose evidence has aged while waiting in the queue.
            await store.commit(planning.job, { task: { plannedAt: Date.now(), evidenceAt: '2020-01-01T00:00:00Z' },
                job: { state: 'acknowledged' }, jobs: [{ id: `${admitted.taskId}:delivery:out`, payload: 1, state: 'pending' }] });
            const result = await runner.execute(admitted.taskId);
            assert.equal(result.deliveries[0].status, stale === 'hold' ? 'held' : 'expired');
            assert.equal(result.deliveries[0].attempts.length, 0); assert.equal(sends, 0);
        } finally { await runner.close(); }
    }
    await store.close();
});

test('delivery expiry during invocation-marker persistence prevents recipient invocation', { timeout: 10000 }, async () => {
    const store = createMemoryStore(); let sends = 0;
    const runner = createRunner({ store: { ...store, async commit(claim, update) {
        await store.commit(claim, update);
        if (claim.kind === 'delivery' && update.job?.stage === 'invoked') await sleep(1100);
    } }, validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async () => { sends++; return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', deliveryTtlMs: 1000,
            connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] } } });
    try {
        const result = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        assert.equal(result.deliveries[0].status, 'expired'); assert.equal(sends, 0);
        assert.equal(result.deliveries[0].attempts.length, 0);
    } finally { await runner.close(); await store.close(); }
});
