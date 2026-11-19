import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createMemoryStore } from '../src/index.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { CHUNK_BYTES } from '../src/storage/values.mjs';

const limits = { maxBytes: 8388608, retentionMs: 1 };
const stores = [['memory', () => createMemoryStore(limits)], ['sqlite', () => createSQLiteStore(':memory:', limits)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...limits })]] : [])];

/** Reusing a value between bounded collection passes must create intact evidence. */
for (const [name, factory] of stores) test(`${name}: re-admission cannot reuse partially collected large values`, async t => {
    let clock = Date.now(); t.mock.method(Date, 'now', () => clock);
    const store = await factory();
    const runner = createRunner({ store, namespace: `collection-${crypto.randomUUID()}`,
        validators: { saved: { revision: '1', validate: value => value } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'saved' } } });
    const value = 'x'.repeat(CHUNK_BYTES * 101); let lease;
    try {
        const first = await runner.run({ flow: 'a', kind: 'result', result: value });
        assert.equal(first.status, 'completed');
        const original = (await store.get(first.taskId, runner.namespace, { metadata: true })).task.result.$data;
        assert(original.bytes > CHUNK_BYTES * 100);
        clock += 2;
        assert.equal((await runner.maintain()).removed, 1);
        assert.equal(await runner.get(first.taskId), null);

        const second = await runner.submit({ flow: 'a', kind: 'result', result: value });
        const replacement = (await store.get(second.taskId, runner.namespace, { metadata: true })).task.result.$data;
        assert.notEqual(replacement.id, original.id, 'A partially collected blob cannot be reused.');
        assert.equal(replacement.sha256, original.sha256);
        assert.equal((await runner.execute(second.taskId)).status, 'completed');
        lease = await runner.openSnapshot(second.taskId);
        // Finish old collection and retire the new task; its snapshot still owns intact data.
        clock += 2;
        for (let pass = 0; pass < 3; pass++) await runner.maintain();
        const parts = await runner.listSnapshot(lease.id, { collection: 'data' });
        const result = parts.items.find(part => part.entity === 'task' && part.path.join('.') === 'result');
        assert(result);
        assert(await runner.readValue(lease.id, result.id) === value, 'Re-admitted evidence must survive later collection.');
    } finally {
        if (lease) await runner.closeSnapshot(lease.id);
        await runner.close(); await store.close();
    }
});
