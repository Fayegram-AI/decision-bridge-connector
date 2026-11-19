import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createDurableAggregator } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];
for (const [name, factory] of stores) test(`${name}: a blocked aggregation rule cannot starve another rule's due window`, async () => {
    const store = await factory(); const namespace = `queue-${crypto.randomUUID()}`;
    const runner = createRunner({ store, namespace, evaluators: { e: { revision: '1', execute: async value => value } },
        flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    const aggregate = createDurableAggregator({ runner, rule: { id: 'healthy', revision: '1', flow: 'a', sources: ['one', 'two'], windowMs: 10, incomplete: 'emit' } });
    try {
        // These expired, un-emitted groups sort before the healthy group's ID.
        for (let i = 0; i < 100; i++) await store.aggregate(namespace, `!${String(i).padStart(3, '0')}`, () => ({
            ruleId: 'blocked', runtimeRevision: 'blocked-owner', state: 'complete', emitted: false, expiresAt: 0,
        }));
        await aggregate.add({ source: 'one', eventId: 'one', correlationKey: 'case', event: 1 }); await sleep(20);
        const flushed = await aggregate.flush(); assert.equal(flushed.length, 1); assert.equal(flushed[0].emitted, true);
        const [task] = (await runner.list()).items;
        assert.equal((await runner.execute(task.taskId)).evaluation.result.status, 'incomplete');
    } finally { await runner.close(); await store.close(); }
});

test('aggregation ownership is recorded at insertion and due queries remain revision scoped', async () => {
    const store = createExecutionStore(createMemoryRepository()); const namespace = 'ownership';
    try {
        for (let i = 0; i < 102; i++) await store.aggregate(namespace, String(i).padStart(3, '0'), () => ({
            state: 'pending', expiresAt: 0, runtimeRevision: i === 101 ? 'mine' : 'other' }));
        const owned = await store.dueAggregates(namespace, 'mine');
        assert.equal(owned.length, 1); assert.equal(owned[0].runtimeRevision, 'mine');
        assert.equal((await store.dueAggregates(namespace, 'other')).length, 100);
        assert.equal(await store.aggregate(namespace, 'missing', () => null), null);
    } finally { await store.close(); }
});

test('expired emitted groups do not block a pending emission owned by the same rule', async () => {
    const store = createMemoryStore(); let blocked = false;
    const runner = createRunner({ store, evaluators: { e: { revision: '1', execute: async value => value } },
        flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    const aggregate = createDurableAggregator({ runner: { ...runner, submit(...args) {
        if (blocked) throw Object.assign(new Error('Full'), { code: 'RESOURCE_LIMIT' });
        return runner.submit(...args);
    } }, rule: { id: 'one', revision: '1', flow: 'a', sources: ['one'], windowMs: 1 } });
    try {
        const seed = await aggregate.add({ source: 'one', eventId: 'seed', correlationKey: 'seed', event: 1 });
        for (let i = 0; i < 100; i++) await store.aggregate(runner.namespace, `!${i}`, () => ({
            runtimeRevision: seed.runtimeRevision, state: 'complete', emitted: true, expiresAt: 0,
        }));
        blocked = true;
        await assert.rejects(aggregate.add({ source: 'one', eventId: 'queued', correlationKey: 'queued', event: 2 }), { code: 'RESOURCE_LIMIT' });
        await sleep(10); blocked = false;
        assert.equal((await aggregate.flush()).length, 1);
        assert.equal((await runner.list()).items.length, 2);
    } finally { await runner.close(); await store.close(); }
});

test('invalid asynchronous aggregation updates leave existing evidence unchanged', async () => {
    const store = createMemoryStore();
    try {
        await store.aggregate('sync-update', 'one', () => ({ state: 'pending', evidence: 1 }));
        await assert.rejects(store.aggregate('sync-update', 'one', async () => ({ state: 'pending', evidence: 2 })), { code: 'CONFIGURATION_ERROR' });
        assert.equal((await store.aggregate('sync-update', 'one', value => value)).evidence, 1);
    } finally { await store.close(); }
});
