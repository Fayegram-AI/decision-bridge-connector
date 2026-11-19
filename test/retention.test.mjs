import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';

const stores = [['memory', limits => createMemoryStore(limits)], ['sqlite', limits => createSQLiteStore(':memory:', limits)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', limits => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...limits })]] : [])];
function setup(store, namespace = crypto.randomUUID()) {
    let fail = true;
    return createRunner({ store, namespace, validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async () => {
            if (fail) { fail = false; throw new ConnectorError('REJECTED', 'Rejected', { ambiguous: false, retryable: false }); }
            return { status: 'acknowledged' };
        }) }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
        ] } } });
}

for (const [name, factory] of stores) test(`${name}: reopened executions receive a fresh retention interval on completion`, async () => {
    const store = await factory({ retentionMs: 1000 }); const runner = setup(store);
    try {
        const failed = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        const previousExpiry = (await store.get(failed.taskId, runner.namespace)).task.expiresAt;
        await sleep(20); await runner.retryDelivery(failed.deliveries[0].id);
        assert.equal((await store.get(failed.taskId, runner.namespace)).task.expiresAt, undefined);
        assert.equal((await runner.execute(failed.taskId)).status, 'completed');
        assert((await store.get(failed.taskId, runner.namespace)).task.expiresAt > previousExpiry);
    } finally { await runner.close(); await store.close(); }
});

// Model a stale candidate page without timing dependencies; locked reads must decide retirement.
test('retention rechecks task, dedup and aggregate eligibility after candidate selection', async () => {
    const repository = createMemoryRepository(); let replacement;
    const store = createExecutionStore({ ...repository, transaction: callback => repository.transaction(tx => callback({ ...tx,
        async find(table, where, options) {
            const rows = await tx.find(table, where, options);
            if (replacement && rows.length && table === replacement.table && where.expiresAt) {
                const change = replacement; replacement = null;
                await tx.put(table, { ...rows[0], ...change.patch });
            }
            return rows;
        },
    })) });
    const runner = setup(store);
    try {
        const task = await runner.run({ flow: 'a', kind: 'result', result: 1, idempotencyKey: 'key' });
        await repository.transaction(async tx => {
            const saved = await tx.get('tasks', task.taskId); await tx.put('tasks', { ...saved, expiresAt: 0 });
        });
        replacement = { table: 'tasks', patch: { state: 'active', expiresAt: null } };
        await runner.maintain(); assert(await runner.get(task.taskId));
        await repository.transaction(async tx => {
            const [key] = await tx.find('dedup', { namespace: runner.namespace }); await tx.put('dedup', { ...key, expiresAt: 0 });
            const saved = await tx.get('tasks', task.taskId); await tx.put('tasks', { ...saved, state: 'complete', expiresAt: Date.now() + 60000 });
        });
        replacement = { table: 'dedup', patch: { expiresAt: Date.now() + 60000 } };
        await runner.maintain();
        assert.equal((await runner.submit({ flow: 'a', kind: 'result', result: 1, idempotencyKey: 'key' })).taskId, task.taskId);
        await store.aggregate(runner.namespace, 'group', () => ({ state: 'complete', emitted: true, expiresAt: 0 }));
        replacement = { table: 'aggregates', patch: { state: 'pending', emitted: false, expiresAt: Date.now() + 60000 } };
        await runner.maintain();
        const group = await store.aggregate(runner.namespace, 'group', current => current);
        assert.equal(group.state, 'pending'); assert.equal(group.emitted, false);
    } finally { await runner.close(); await store.close(); }
});

test('PostgreSQL retention cannot erase a retry or a replacement aggregation window', { skip: !process.env.DBC_POSTGRES_URL, timeout: 60000 }, async () => {
    const { Pool } = await import('pg'); const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL,
        connectionTimeoutMillis: 10000, statement_timeout: 10000 });
    const enteredOrFailed = (operation, hook) => Promise.race([hook.entered.promise, operation.then(() => {
        throw new Error('Maintenance completed without reaching the expected interception.');
    })]);
    let intercept; let indexDdl = 0;
    const hookedPool = { async connect() {
        const client = await pool.connect();
        return { release: () => client.release(), async query(sql, parameters) {
            if (typeof sql === 'string' && sql.includes('CREATE INDEX')) indexDdl++;
            const result = await client.query(sql, parameters);
            if (intercept && typeof sql === 'string' && sql.startsWith(`SELECT value,data FROM dbc3_${intercept.table} WHERE`) && sql.includes('expiresAt')) {
                const hook = intercept; intercept = null; hook.entered.resolve(); await hook.release.promise;
            }
            return result;
        } };
    } };
    const store = await createPostgresStore({ pool, retentionMs: 20, dedupeMs: 20 });
    const maintenance = await createPostgresStore({ pool: hookedPool }); const runner = setup(store);
    let running; let hook; let admitting;
    try {
        assert.equal(indexDdl, 0, 'Reopening an initialized store must not lock tables for index DDL.');
        const task = await runner.run({ flow: 'a', kind: 'result', result: 1 }); await sleep(30);
        hook = { table: 'tasks', entered: Promise.withResolvers(), release: Promise.withResolvers() }; intercept = hook;
        running = maintenance.maintain(runner.namespace); await enteredOrFailed(running, hook);
        await runner.retryDelivery(task.deliveries[0].id);
        hook.release.resolve(); await running;
        assert(await runner.get(task.taskId)); assert.equal((await runner.execute(task.taskId)).status, 'completed');
        await store.aggregate(runner.namespace, 'window', () => ({ aggregateId: 'old', state: 'complete', emitted: true, expiresAt: 0 }));
        hook = { table: 'aggregates', entered: Promise.withResolvers(), release: Promise.withResolvers() }; intercept = hook;
        running = maintenance.maintain(runner.namespace); await enteredOrFailed(running, hook);
        await store.aggregate(runner.namespace, 'window', () => ({ aggregateId: 'replacement', state: 'pending', emitted: false, expiresAt: Date.now() + 60000 }));
        hook.release.resolve(); await running;
        assert.equal((await store.aggregate(runner.namespace, 'window', current => current)).aggregateId, 'replacement');
        const input = { flow: 'a', kind: 'result', result: 3, idempotencyKey: 'retired-key' };
        const original = await runner.run(input); await sleep(30);
        hook = { table: 'dedup', entered: Promise.withResolvers(), release: Promise.withResolvers() }; intercept = hook;
        running = maintenance.maintain(runner.namespace); await enteredOrFailed(running, hook);
        let admitted = false; admitting = runner.submit(input).then(value => { admitted = true; return value; });
        await sleep(20); assert.equal(admitted, false, 'Admission must wait while its old dedup identity is retired.');
        hook.release.resolve(); await running; const next = await admitting;
        assert.notEqual(next.taskId, original.taskId);
        assert.equal((await runner.submit(input)).taskId, next.taskId);
    } finally { hook?.release.resolve(); await running; await admitting; await runner.close(); await maintenance.close(); await store.close(); await pool.end(); }
});
