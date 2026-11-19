import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', createMemoryStore], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) test(`${name}: operator failure resolution records history and dispatches one notification`, async () => {
    const store = await factory(); const notifications = [];
    const runner = createRunner({ store, namespace: `resolution-${crypto.randomUUID()}`, validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(value => { notifications.push(value); return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', maxAgeMs: 1, stale: 'hold', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
            { id: 'notice', connector: 'sink', when: 'failure', payload: 'failure' },
        ] } } });
    try {
        const first = await runner.run({ flow: 'a', kind: 'result', result: 1, evidenceAt: new Date(Date.now() - 1000).toISOString() },
            { waitFor: 'settled', waitMs: name === 'postgres' ? 15000 : 3000 });
        assert.equal(first.status, 'unknown');
        const resolved = await runner.resolveDelivery(`${first.taskId}:delivery:out`, { status: 'failed', receipt: { operator: 'confirmed' } });
        assert.equal(resolved.requiredSettled, false);
        const final = await runner.execute(first.taskId, { waitFor: 'settled', waitMs: name === 'postgres' ? 15000 : 3000 });
        assert.equal(final.errorCount, 1); assert.equal(notifications.length, 1);
        assert.equal(notifications[0].errors[0].code, 'DELIVERY_RESOLVED_FAILED');
        await runner.resolveDelivery(`${first.taskId}:delivery:out`, { status: 'failed' }); await runner.tick();
        assert.equal((await runner.get(first.taskId)).errorCount, 1); assert.equal(notifications.length, 1);
    } finally { await runner.close(); await store.close(); }
});
