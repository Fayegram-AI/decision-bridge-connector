import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createDurableAggregator, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];
const configuration = (store, validate = value => value) => ({ store, namespace: `aggregate-progress-${crypto.randomUUID()}`,
    evaluators: { e: { revision: '1', validate, execute: async value => value } },
    flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
const rule = { id: 'pair', revision: '1', sources: ['one', 'two'], windowMs: 60000, incomplete: 'emit', flow: 'a' };
const reject = () => { throw new ConnectorError('INVALID_RESULT', 'Rejected group.'); };

for (const [name, factory] of stores) {
    test(`${name}: a complete group retries admission before its original window expires`, async () => {
        const store = await factory(); let blocked = true;
        const runner = createRunner(configuration(store, value => {
            if (blocked) throw new ConnectorError('RESOURCE_LIMIT', 'Fixture capacity.', { retryable: true, ambiguous: false });
            return value;
        }));
        const aggregate = createDurableAggregator({ runner, rule: { ...rule, sources: ['one'], windowMs: 3600000 } });
        try {
            await assert.rejects(aggregate.add({ source: 'one', eventId: 'one', correlationKey: 'case', event: 42 }), { code: 'RESOURCE_LIMIT' });
            assert.equal((await runner.list()).items.length, 0); blocked = false;
            const emitted = await aggregate.flush(); assert.equal(emitted.length, 1);
            assert.equal(emitted[0].emitted, true); assert.equal(emitted[0].events.one.event, 42);
            assert.equal((await runner.list()).items.length, 1); assert.deepEqual(await aggregate.flush(), []);
        } finally { await runner.close(); await store.close(); }
    });

    test(`${name}: mixed due pages retain fair ordering without duplicate or early pending groups`, async () => {
        const store = await factory(); const namespace = `aggregate-merge-${crypto.randomUUID()}`;
        try {
            const saved = (id, patch = {}) => ({ aggregateId: id, runtimeRevision: 'r', state: 'complete', emitted: false,
                expiresAt: Date.now() + 3600000, ...patch });
            // Closed legacy groups retain a future expiry; an overlap is eligible in both queries.
            await store.aggregate(namespace, '!overlap', () => saved('!overlap', { expiresAt: 0 }));
            for (let i = 0; i < 100; i++) await store.aggregate(namespace, `closed-${String(i).padStart(3, '0')}`, () => saved(`closed-${i}`));
            await store.aggregate(namespace, 'z-due', () => saved('due', { state: 'pending', expiresAt: 0 }));
            await store.aggregate(namespace, '!not-due', () => saved('pending', { state: 'pending' }));
            await store.aggregate(namespace, '!emitted', () => saved('emitted', { emitted: true }));
            await store.aggregate(namespace, '!other', () => saved('other', { runtimeRevision: 'other' }));
            const first = await store.dueAggregates(namespace, 'r'); const second = await store.dueAggregates(namespace, 'r');
            assert.equal(first.length, 100); assert.equal(second.length, 2);
            const ids = [...first, ...second].map(row => row.aggregateId);
            assert.equal(new Set(ids).size, 102); assert(ids.includes('!overlap')); assert(ids.includes('due'));
            for (const id of ['pending', 'emitted', 'other']) assert(!ids.includes(id));
            assert.equal((await store.dueAggregates(namespace, 'r'))[0].aggregateId, first[0].aggregateId);
        } finally { await store.close(); }
    });

    test(`${name}: a rejected group does not prevent another due group in its rule from emitting`, async () => {
        const store = await factory(); let blocked;
        const runner = createRunner(configuration(store, value => value.correlationKey === blocked ? reject() : value));
        const aggregate = createDurableAggregator({ runner, rule });
        try {
            const groups = [];
            for (const key of ['first', 'second']) groups.push(await aggregate.add({ source: 'one', eventId: key, correlationKey: key, event: key }));
            groups.sort((a, b) => a.key < b.key ? -1 : 1); blocked = groups[0].correlationKey;
            for (const group of groups) await store.aggregate(runner.namespace, group.key, current => ({ ...current, expiresAt: 0 }));
            await assert.rejects(aggregate.flush(), { code: 'INVALID_RESULT' });
            const page = await runner.list(); assert.equal(page.items.length, 1);
            assert.equal((await runner.execute(page.items[0].taskId)).evaluation.result.correlationKey, groups[1].correlationKey);
            const retained = await store.aggregate(runner.namespace, groups[0].key, current => current);
            assert.equal(retained.state, 'incomplete'); assert.equal(retained.emitted, false);
            blocked = undefined; assert.equal((await aggregate.flush()).length, 1);
            assert.equal((await runner.list()).items.length, 2); assert.deepEqual(await aggregate.flush(), []);
        } finally { await runner.close(); await store.close(); }
    });

    test(`${name}: due scanning advances past a full page of failed groups and wraps for retries`, { timeout: 60000 }, async () => {
        const store = await factory(); const runner = createRunner(configuration(store,
            value => value.correlationKey.startsWith('blocked') ? reject() : value));
        const aggregate = createDurableAggregator({ runner, rule });
        try {
            const seed = await aggregate.add({ source: 'one', eventId: 'valid', correlationKey: 'valid', event: 1 });
            await store.aggregate(runner.namespace, seed.key, current => ({ ...current, expiresAt: 0 }));
            for (let i = 0; i < 100; i++) await store.aggregate(runner.namespace, `!${String(i).padStart(3, '0')}`,
                () => ({ ...seed, key: `!${String(i).padStart(3, '0')}`, aggregateId: `blocked-${i}`,
                    correlationKey: `blocked-${i}`, state: 'incomplete', expiresAt: 0 }));
            await assert.rejects(aggregate.flush(), { code: 'INVALID_RESULT' }); assert.equal((await runner.list()).items.length, 0);
            const emitted = await aggregate.flush(); assert.equal(emitted.length, 1); assert.equal(emitted[0].aggregateId, seed.aggregateId);
            await assert.rejects(aggregate.flush(), { code: 'INVALID_RESULT' });
            assert.equal((await runner.list()).items.length, 1);
        } finally { await runner.close(); await store.close(); }
    });

    test(`${name}: aggregate retirement progresses beyond a full page of pinned emissions`, { timeout: 60000 }, async () => {
        const store = await factory(); const namespace = `aggregate-retire-${crypto.randomUUID()}`;
        try {
            for (let i = 0; i < 100; i++) await store.aggregate(namespace, `!${String(i).padStart(3, '0')}`,
                () => ({ aggregateId: `blocked-${i}`, state: 'complete', emitted: false, expiresAt: 0 }));
            await store.aggregate(namespace, 'zz-retire', () => ({ aggregateId: 'retire', state: 'complete', emitted: true, expiresAt: 0 }));
            await store.maintain(namespace); await store.maintain(namespace);
            await store.aggregate(namespace, 'zz-retire', current => { assert.equal(current, null); return null; });
            const pinned = await store.aggregate(namespace, '!000', current => current);
            assert.equal(pinned.aggregateId, 'blocked-0'); assert.equal(pinned.emitted, false);
        } finally { await store.close(); }
    });
}

test('aggregation scans preserve repository collation across two full candidate pages', async () => {
    const repository = createMemoryRepository(); const collator = new Intl.Collator('en');
    // Model a database whose native key order differs from JavaScript code points.
    const store = createExecutionStore({ ...repository, transaction: operation => repository.transaction(tx => operation({ ...tx,
        async find(table, where, query = {}) {
            if (table !== 'aggregates') return tx.find(table, where, query);
            const rows = await tx.find(table, where, { limit: 1000 });
            return rows.sort((a, b) => collator.compare(a.id, b.id))
                .filter(row => !query.after || collator.compare(row.id, query.after.id) > 0).slice(0, query.limit ?? 100);
        },
    })) });
    try {
        for (let i = 0; i < 100; i++) {
            const suffix = String(i).padStart(3, '0');
            await store.aggregate('collation', `a${suffix}`, () => ({ aggregateId: `a${suffix}`, runtimeRevision: 'r',
                state: 'complete', emitted: false, expiresAt: Date.now() + 3600000 }));
            await store.aggregate('collation', `Z${suffix}`, () => ({ aggregateId: `Z${suffix}`, runtimeRevision: 'r',
                state: 'pending', emitted: false, expiresAt: 0 }));
        }
        const first = await store.dueAggregates('collation', 'r'); const second = await store.dueAggregates('collation', 'r');
        assert.equal(first.length, 100); assert.equal(second.length, 100);
        assert(first.every(row => row.state === 'complete')); assert(second.every(row => row.state === 'pending'));
        assert.equal(new Set([...first, ...second].map(row => row.aggregateId)).size, 200);
        assert.equal((await store.dueAggregates('collation', 'r'))[0].aggregateId, first[0].aggregateId);
    } finally { await store.close(); }
});

test('aggregation scan cursors expire without retiring pinned evidence', async t => {
    const repository = createMemoryRepository(); const store = createExecutionStore(repository); let clock = Date.now();
    t.mock.method(Date, 'now', () => clock);
    try {
        await store.aggregate('cursor', 'one', () => ({ aggregateId: 'one', runtimeRevision: 'owner', state: 'complete', emitted: false, expiresAt: 0 }));
        assert.equal((await store.dueAggregates('cursor', 'owner')).length, 1);
        const cursors = () => repository.transaction(tx => tx.find('controls', { namespace: 'cursor', kind: 'aggregate-cursor' }));
        assert.equal((await cursors()).length, 1); clock += 600001; await store.maintain('cursor');
        assert.deepEqual(await cursors(), []); assert.equal((await store.dueAggregates('cursor', 'owner')).length, 1);
    } finally { await store.close(); }
});

test('maximum-length aggregation names emit with stable bounded sources and preserve ordinary identities', async () => {
    const runner = createRunner(configuration(createMemoryStore()));
    try {
        for (const id of ['short', 'r'.repeat(256)]) {
            const aggregate = createDurableAggregator({ runner, rule: { ...rule, id, sources: ['one'] } });
            const group = await aggregate.add({ source: 'one', eventId: 'one', correlationKey: 'case', event: 1 });
            assert.equal(group.ruleId, id); assert.equal(group.emitted, true);
            assert.equal((await aggregate.add({ source: 'one', eventId: 'one', correlationKey: 'case', event: 1 })).aggregateId, group.aggregateId);
        }
        const page = await runner.list(); assert.equal(page.items.length, 2); const sources = [];
        for (const task of page.items) sources.push((await runner.store.get(task.taskId, runner.namespace)).task.source);
        assert(sources.includes('aggregate:short'));
        assert(sources.some(value => /^aggregate:[0-9a-f]{64}$/.test(value))); assert(sources.every(value => value.length <= 256));
    } finally { await runner.close(); await runner.store.close(); }
});
test('depth-100 aggregate evidence survives SQLite emission, evaluation, and transfer', async () => {
    const store = createSQLiteStore(':memory:');
    let actual;
    const runner = createRunner({ store, evaluators: { e: { revision: '1',
        execute(value) { actual = value.events.one.event; return 42; }, validateSaved: value => value,
    } }, flows: { a: { revision: '1', inputs: ['event', 'request', 'result'], evaluator: 'e', template: { $path: '$' } } } });
    try {
        const aggregator = createDurableAggregator({ runner, rule: { id: 'deep', revision: '1', flow: 'a',
            sources: ['one', 'two'], windowMs: 10000 } });
        let event = 1;
        for (let index = 0; index < 100; index++) event = { value: event };
        const rejected = { source: 'one', eventId: 'too-deep', correlationKey: 'group', event: { value: event } };
        await assert.rejects(aggregator.add(rejected), { code: 'RESOURCE_LIMIT' });
        const first = await aggregator.add({ ...rejected, eventId: 'accepted', event });
        assert.equal(first.state, 'pending');
        const finished = await aggregator.add({ source: 'two', eventId: 'second', correlationKey: 'group', event: 2 });
        assert.equal(finished.emitted, true);
        assert.deepEqual(finished.events.one.event, event);
        const [task] = (await runner.list()).items;
        assert.equal((await runner.execute(task.taskId)).status, 'completed');
        assert.deepEqual(actual, event);
        const imported = await runner.importTask(runner.exportTask(task.taskId), { flow: 'a' });
        assert.equal((await runner.execute(imported.taskId)).status, 'completed');
        const saved = await store.get(imported.taskId, runner.namespace);
        assert.deepEqual(saved.task.request.events.one.event, event);
        const rerun = await runner.reevaluate(task.taskId);
        assert.equal((await runner.execute(rerun.taskId)).status, 'completed');
    } finally { await runner.close(); await store.close(); }
});
