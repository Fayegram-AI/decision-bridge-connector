import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector, createDurableAggregator } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

const stores = [['memory', options => createMemoryStore(options)], ['sqlite', options => createSQLiteStore(':memory:', options)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', options => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...options })]] : [])];

for (const [name, factory] of stores) test(`${name}: uncertain child links pin admission beyond deduplication and task retention`, { timeout: 30000 }, async () => {
    const store = await factory({ retentionMs: 1, dedupeMs: 1 });
    try {
        for (const maintain of [false, true]) {
            let inject = true; let calls = 0; const failed = Promise.withResolvers();
            const runner = createRunner({ store: { ...store, async commit(claim, update) {
                if (inject && claim.kind === 'chain' && update.job?.state === 'acknowledged') { inject = false; throw new Error('Link write failed.'); }
                return store.commit(claim, update);
            } }, namespace: `chain-retention-${crypto.randomUUID()}`,
            onEvent(event) { if (event.type === 'worker.error') failed.resolve(); },
            validators: { v: { revision: '1', validate: value => value } },
            evaluators: { e: { revision: '1', execute: async value => { calls++; return value; } } },
            connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged', receipt: { response: 42 } })) },
            flows: { parent: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                { id: 'next', connector: 'sink', when: 'always', payload: 'raw-result', nextFlow: 'child', chainRetry: { maxAttempts: 3, delayMs: 10, maxDelayMs: 50 } },
            ] }, child: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
            try {
                const parent = await runner.submit({ flow: 'parent', kind: 'result', result: 1 });
                await bounded(async () => { while (inject) { await runner.tick({ taskId: parent.taskId }); await sleep(5); } await failed.promise; }, { timeoutMs: 5000 });
                const child = (await runner.list({ flow: 'child' })).items[0]; assert(child);
                await runner.execute(child.taskId); assert.equal(calls, 1);
                await sleep(10);
                if (maintain) { await runner.maintain(); assert(await runner.get(child.taskId)); }
                const owner = (await store.get(parent.taskId, runner.namespace)).jobs.find(job => job.kind === 'chain');
                await store.commit(owner, { job: { leaseUntil: 0 } }); await runner.recover();
                const linked = await runner.execute(parent.taskId);
                assert.equal(linked.status, 'completed'); assert.equal(linked.chains[0].childTaskId, child.taskId);
                assert.equal((await runner.list({ flow: 'child' })).items.length, 1); assert.equal(calls, 1);
                await sleep(10); await runner.maintain(); await runner.maintain();
                assert.equal((await runner.list()).items.length, 0);
            } finally { await runner.close(); }
        }
    } finally { await store.close(); }
});

for (const [name, factory] of stores) test(`${name}: uncertain aggregation emission pins the admitted execution until linkage settles`, { timeout: 30000 }, async () => {
    const store = await factory({ retentionMs: 1, dedupeMs: 1 }); let inject = true; let calls = 0; let groupKey;
    const runner = createRunner({ store: { ...store, aggregate(namespace, key, update, ...args) {
        return store.aggregate(namespace, key, current => {
            const next = update(current);
            if (inject && next?.emitted) { inject = false; groupKey = key; throw new Error('Emission write failed.'); }
            return next;
        }, ...args);
    } }, namespace: `aggregate-retention-${crypto.randomUUID()}`,
    evaluators: { e: { revision: '1', execute: async value => { calls++; return value; } } },
    flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    const aggregate = createDurableAggregator({ runner, rule: { id: 'one', revision: '1', sources: ['one'], flow: 'a', windowMs: 60000 } });
    try {
        await assert.rejects(aggregate.add({ source: 'one', eventId: 'first', correlationKey: 'case', event: 1 }), /Emission write failed/);
        // Expire completed evidence explicitly; population must not race a one-millisecond window.
        await store.aggregate(runner.namespace, groupKey, current => ({ ...current, expiresAt: 0 }));
        const [task] = (await runner.list()).items; assert(task);
        await runner.execute(task.taskId); assert.equal(calls, 1); await sleep(10);
        await runner.maintain(); assert(await runner.get(task.taskId));
        const emitted = await aggregate.flush(); assert.equal(emitted.length, 1); assert.equal(emitted[0].emitted, true);
        assert.equal((await runner.list()).items.length, 1); assert.equal(calls, 1);
        await runner.maintain(); assert.equal((await runner.list()).items.length, 0);
    } finally { await runner.close(); await store.close(); }
});

for (const [name, factory] of stores) test(`${name}: a stale aggregation producer cannot recreate a retired execution`, { timeout: 30000 }, async () => {
    const store = await factory({ retentionMs: 1, dedupeMs: 1 }); const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    let hold = true; let calls = 0;
    const runner = createRunner({ store, namespace: `stale-producer-${crypto.randomUUID()}`,
        evaluators: { e: { revision: '1', execute: async value => { calls++; return value; } } },
        flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    const aggregate = createDurableAggregator({ runner: { ...runner, async submit(...args) {
        if (hold) { hold = false; entered.resolve(); await release.promise; }
        return runner.submit(...args);
    } }, rule: { id: 'one', revision: '1', sources: ['one'], flow: 'a', windowMs: 60000 } });
    const input = { source: 'one', eventId: 'one', correlationKey: 'case', event: 1 };
    const delayed = aggregate.add(input);
    // Observe the deliberate rejection before releasing the stale producer.
    const rejected = assert.rejects(delayed, { code: 'ADMISSION_OWNER_SETTLED' });
    try {
        await bounded(() => entered.promise, { timeoutMs: 5000 });
        assert.equal((await aggregate.add(input)).emitted, true);
        const [task] = (await runner.list()).items;
        await runner.execute(task.taskId); await sleep(10); await runner.maintain();
        assert.equal((await runner.list()).items.length, 0);
        release.resolve(); await rejected;
        assert.equal((await runner.list()).items.length, 0); assert.equal(calls, 1);
    } finally { release.resolve(); await rejected; await runner.close(); await store.close(); }
});

// This fixture performs 101 admissions and several full retirement pages. Its
// harness guard includes native PostgreSQL I/O; it is not a maintenance-time SLA.
for (const [name, factory] of stores) test(`${name}: retained admissions cannot hide later eligible retirement pages`, { timeout: name === 'postgres' ? 120000 : 60000 }, async () => {
    const store = await factory({ dedupeMs: 1 }); const namespace = `retirement-pages-${crypto.randomUUID()}`;
    const owner = await store.aggregate(namespace, 'owner', () => ({ aggregateId: 'owner', state: 'complete', emitted: false, expiresAt: 0 }));
    try {
        // Populate complete domain records directly; no provider or delivery work is involved.
        for (let index = 0; index <= 100; index++) {
            const taskId = `${namespace}:${String(index).padStart(3, '0')}`;
            await store.admit({ id: taskId, namespace, source: 'aggregate:one', rootId: taskId, flow: 'a', state: 'complete', expiresAt: 0,
                lineage: { rootId: taskId, depth: 0, path: [] }, input: { idempotencyKey: taskId }, fingerprint: taskId, plan: { maxExecutions: 1 },
                ...(index < 100 ? { admissionOwner: { kind: 'aggregate', key: 'owner', aggregateId: owner.aggregateId } } : {}),
            }, [], taskId);
        }
        await sleep(5);
        assert.equal((await store.maintain(namespace)).removed, 0);
        assert(await store.get(`${namespace}:100`, namespace));
        assert.equal((await store.maintain(namespace)).removed, 1);
        assert.equal(await store.get(`${namespace}:100`, namespace), null);
        assert(await store.get(`${namespace}:000`, namespace));
        await store.aggregate(namespace, 'owner', current => ({ ...current, emitted: true }));
        // Wrap to the earlier pinned page after it becomes eligible.
        assert.equal((await store.maintain(namespace)).removed, 100);
        assert.equal(await store.get(`${namespace}:000`, namespace), null);
    } finally { await store.close(); }
});
