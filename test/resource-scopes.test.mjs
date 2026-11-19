import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createDurableAggregator } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { checkStore } from '../src/testing.mjs';

const stores = [['memory', options => createMemoryStore(options)], ['sqlite', options => createSQLiteStore(':memory:', options)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', options => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...options })]] : [])];

for (const [name, factory] of stores) {
    test(`${name}: conformance supports a one-task admission allowance`, async () => {
        const store = await factory({ maxPending: 1, pageSize: 1 });
        try { assert.equal((await checkStore(store)).passed, true); } finally { await store.close(); }
    });

    test(`${name}: mixed job kinds and competing workers share one flow limit`, async () => {
        const store = await factory(); const namespace = `mixed-${crypto.randomUUID()}`;
        try {
            for (const [index, kind, flow] of [[0, 'evaluation', 'a'], [1, 'delivery', 'a'], [2, 'chain', 'a'], [3, 'failure', 'a'], [4, 'evaluation', 'b']]) {
                const taskId = `task-${namespace}-${index}`;
                await store.admit({ id: taskId, namespace, flow, rootId: taskId, state: 'active',
                    plan: { maxExecutions: 10, concurrency: 1 } }, [{ id: `${taskId}:job`, namespace, taskId, flow,
                    kind, destination: 'sink', state: 'pending', priority: 0, required: 1, createdAt: 0, runAt: 0, expiresAt: Date.now() + 100000,
                    attempts: [], plan: { dependsOn: [] } }]);
            }
            const claims = await Promise.all(['evaluation', 'delivery', 'chain', 'failure'].map((kind, index) =>
                store.claim(namespace, kind, { worker: `worker-${index}`, flow: 'a' })));
            assert.equal(claims.filter(Boolean).length, 1);
            const independent = await store.claim(namespace, 'evaluation', { worker: 'independent', flow: 'b' }); assert(independent);
            const occupied = claims.find(Boolean); await store.commit(occupied.job, { job: { state: 'acknowledged' } });
            const next = await store.claim(namespace, occupied.job.kind === 'chain' ? 'failure' : 'chain', { worker: 'next', flow: 'a' }); assert(next);
            const excessId = `legacy-${namespace}`;
            const excess = { id: `${excessId}:job`, namespace, taskId: excessId, flow: 'a', kind: 'delivery', state: 'active',
                token: 'legacy-owner', leaseUntil: Date.now() + 100000, priority: 0, required: 1, plan: {} };
            await store.admit({ ...next.task, id: excessId, rootId: excessId }, [excess]);
            await assert.rejects(store.commit(excess, { job: { acceptance: { receipt: { status: 'acknowledged' } } } }), { code: 'STALE_ATTEMPT' });
            assert.equal(await store.claim(namespace, 'evaluation', { flow: 'a' }), null);
            await store.commit(excess, { job: { state: 'acknowledged' } });
            assert.equal(await store.claim(namespace, 'evaluation', { flow: 'a' }), null);
            await store.commit(next.job, { job: { state: 'acknowledged' } });
            const drained = await Promise.all(['evaluation', 'delivery', 'chain', 'failure'].map(kind => store.claim(namespace, kind, { worker: `drained-${kind}`, flow: 'a' })));
            assert.equal(drained.filter(Boolean).length, 1);
        } finally { await store.close(); }
    });

    test(`${name}: rule quotas isolate names and count retained revisions`, async () => {
        const store = await factory(); const runner = createRunner({ store, namespace: `quota-${crypto.randomUUID()}`,
            evaluators: { e: { revision: '1', execute: value => value } },
            flows: { a: { revision: '1', inputs: ['event'], template: { $path: '$' }, evaluator: 'e' } } });
        const rule = { id: 'one', revision: '1', flow: 'a', sources: ['x', 'y'], windowMs: 100000, maxGroups: 1 };
        const event = key => ({ source: 'x', eventId: key, correlationKey: key, event: 1 });
        try {
            await store.aggregate(runner.namespace, 'legacy', () => ({ state: 'pending' }), 1);
            const unrelated = createDurableAggregator({ runner, rule: { ...rule, id: 'other', maxGroups: 10 } });
            await unrelated.add(event('other'));
            const first = createDurableAggregator({ runner, rule }); const retained = await first.add(event('first'));
            const revised = createDurableAggregator({ runner, rule: { ...rule, revision: '2' } });
            await assert.rejects(revised.add(event('second')), { code: 'RESOURCE_LIMIT' });
            await store.aggregate(runner.namespace, retained.key, current => ({ ...current, emitted: true, state: 'complete', expiresAt: Date.now() + 100000 }));
            await assert.rejects(revised.add(event('third')), { code: 'RESOURCE_LIMIT' });
            // Existing groups can update at quota; old evidence remains intact.
            await store.aggregate(runner.namespace, retained.key, current => ({ ...current, evidence: 'retained' }), 1);
            assert.equal((await store.aggregate(runner.namespace, retained.key, value => value)).evidence, 'retained');
            const parallel = createDurableAggregator({ runner, rule: { ...rule, id: 'parallel' } });
            const results = await Promise.allSettled([parallel.add(event('a')), parallel.add(event('b'))]);
            assert.equal(results.filter(item => item.status === 'fulfilled').length, 1);
            assert.equal(results.find(item => item.status === 'rejected').reason.code, 'RESOURCE_LIMIT');
            await assert.rejects(store.aggregate(runner.namespace, 'legacy-overflow', () => ({ state: 'pending' }), 1), { code: 'RESOURCE_LIMIT' });
        } finally { await runner.close(); await store.close(); }
    });

    test(`${name}: default list size and explicit limits preserve cursors`, async () => {
        const store = await factory({ pageSize: 1 }); const runner = createRunner({ store, namespace: `paging-${crypto.randomUUID()}`,
            validators: { v: { revision: '1', validate: value => value } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
        try {
            for (let index = 0; index < 3; index++) await runner.submit({ flow: 'a', kind: 'result', result: index });
            const first = await runner.list(); assert.equal(first.items.length, 1); assert(first.next);
            const rest = await runner.list({ after: first.next, limit: 2 }); assert.equal(rest.items.length, 2);
            assert.equal(new Set([...first.items, ...rest.items].map(task => task.taskId)).size, 3);
            assert.equal((await runner.list({ after: rest.next })).items.length, 0);
            await assert.rejects(runner.list({ limit: 101 }), { code: 'CONFIGURATION_ERROR' });
        } finally { await runner.close(); await store.close(); }
    });
}

test('invalid page defaults fail before opening durable storage', async () => {
    for (const pageSize of [0, 1.5, 101]) {
        assert.throws(() => createMemoryStore({ pageSize }), { code: 'CONFIGURATION_ERROR' });
        assert.throws(() => createSQLiteStore('unopened-page.sqlite', { pageSize }), { code: 'CONFIGURATION_ERROR' });
        await assert.rejects(createPostgresStore({ connectionString: 'unopened', pageSize }), { code: 'CONFIGURATION_ERROR' });
    }
});
