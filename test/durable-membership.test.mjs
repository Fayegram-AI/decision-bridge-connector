import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createDurableAggregator } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) test(`${name}: durable fan-in counts only supplied own members`, { timeout: 30000 }, async () => {
    const store = await factory();
    const runner = createRunner({ store, namespace: `membership-${crypto.randomUUID()}`,
        evaluators: { e: { revision: '1', execute: async value => value } },
        flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    const sources = ['ordinary', 'constructor', '__proto__', 'toString', 'hasOwnProperty'];
    const rule = { id: 'reserved', revision: '1', sources, flow: 'a', windowMs: 60000 };
    try {
        assert.throws(() => createDurableAggregator({ runner, rule: { ...rule, sources: 'ordinary' } }), { code: 'CONFIGURATION_ERROR' });
        const aggregate = createDurableAggregator({ runner, rule });
        for (const [index, source] of sources.entries()) {
            const group = await aggregate.add({ source, eventId: `${source}-one`, correlationKey: 'case', event: index });
            assert.equal(group.state, index === sources.length - 1 ? 'complete' : 'pending');
            if (index < sources.length - 1) assert.equal((await runner.list()).items.length, 0);
            if (index === 0) await assert.rejects(aggregate.add({ source, eventId: 'different', correlationKey: 'case', event: 10 }), { code: 'DUPLICATE_MEMBER' });
        }
        const [task] = (await runner.list()).items;
        const completed = await runner.execute(task.taskId);
        assert.deepEqual(completed.evaluation.result.missingSources, []);
        for (const source of sources) {
            assert(Object.hasOwn(completed.evaluation.result.events, source));
            assert.equal(completed.evaluation.result.events[source].eventId, `${source}-one`);
        }
        const incomplete = createDurableAggregator({ runner, rule: { ...rule, id: 'missing', sources: ['ordinary', 'constructor'], incomplete: 'emit' } });
        const group = await incomplete.add({ source: 'ordinary', eventId: 'only-one', correlationKey: 'case', event: 1 });
        assert.equal(group.state, 'pending');
        await store.aggregate(runner.namespace, group.key, current => ({ ...current, expiresAt: 0 }));
        assert.equal((await incomplete.flush()).length, 1);
        const missing = (await runner.list()).items.find(item => item.taskId !== task.taskId);
        const result = (await runner.execute(missing.taskId)).evaluation.result;
        assert.equal(result.status, 'incomplete'); assert.deepEqual(result.missingSources, ['constructor']);
        assert.equal(Object.hasOwn(result.events, 'constructor'), false);
    } finally { await runner.close(); await store.close(); }
});
