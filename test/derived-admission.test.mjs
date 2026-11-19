import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) test(`${name}: derived executions reject explicit invalid keys before admission`, async () => {
    const store = await factory();
    const runner = createRunner({ store, namespace: `derived-key-${crypto.randomUUID()}`,
        validators: { v: { revision: '1', validate: value => value } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    try {
        const original = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        for (const method of ['reprocess', 'redeliver']) for (const key of ['', null, false, 0]) {
            await assert.rejects(runner[method](original.taskId, { flow: 'a', idempotencyKey: key }), { code: 'CONFIGURATION_ERROR' });
            assert.equal((await runner.list()).items.length, 1);
        }
        const first = await runner.reprocess(original.taskId); const second = await runner.reprocess(original.taskId, { idempotencyKey: undefined });
        assert.notEqual(first.taskId, second.taskId);
    } finally { await runner.close(); await store.close(); }
});

for (const [name, factory] of stores) test(`${name}: derived admission keys bind delivery intent and original execution`, { timeout: 30000 }, async () => {
    const store = await factory(); let delivered = 0;
    const runner = createRunner({ store, namespace: `derived-intent-${crypto.randomUUID()}`,
        validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async () => { delivered++; return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
        ] } } });
    try {
        const input = { flow: 'a', kind: 'result', result: 1, evidenceAt: new Date().toISOString() };
        const first = await runner.run(input); const second = await runner.run(input);
        assert.notEqual(first.taskId, second.taskId); assert.equal(delivered, 2);
        const policy = await runner.reprocess(first.taskId, { idempotencyKey: 'policy' });
        assert.equal((await runner.reprocess(first.taskId, { idempotencyKey: 'policy' })).taskId, policy.taskId);
        await assert.rejects(runner.redeliver(first.taskId, { flow: 'a', idempotencyKey: 'policy' }), { code: 'IDEMPOTENCY_CONFLICT' });
        await assert.rejects(runner.reprocess(second.taskId, { idempotencyKey: 'policy' }), { code: 'IDEMPOTENCY_CONFLICT' });
        assert.equal((await runner.execute(policy.taskId)).status, 'completed'); assert.equal(delivered, 2);
        const replay = await runner.redeliver(first.taskId, { flow: 'a', idempotencyKey: 'delivery' });
        await assert.rejects(runner.reprocess(first.taskId, { idempotencyKey: 'delivery' }), { code: 'IDEMPOTENCY_CONFLICT' });
        await assert.rejects(runner.redeliver(second.taskId, { flow: 'a', idempotencyKey: 'delivery' }), { code: 'IDEMPOTENCY_CONFLICT' });
        assert.equal((await runner.execute(replay.taskId)).status, 'completed'); assert.equal(delivered, 3);
        assert.equal((await runner.redeliver(first.taskId, { flow: 'a', idempotencyKey: 'delivery' })).taskId, replay.taskId);
        assert.equal((await runner.list()).items.length, 4); assert.equal(delivered, 3);
        // Ordinary SDK sources retain their original key semantics even with this name.
        const ordinary = { ...input, idempotencyKey: 'ordinary' };
        const submitted = await runner.submit(ordinary, { source: 'reprocess' });
        assert.equal((await runner.submit(ordinary, { source: 'reprocess' })).taskId, submitted.taskId);
    } finally { await runner.close(); await store.close(); }
});
