import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

const stores = [['memory', () => createMemoryStore({ retentionMs: 1 })], ['sqlite', () => createSQLiteStore(':memory:', { retentionMs: 1 })],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, retentionMs: 1 })]] : [])];

for (const [name, factory] of stores) test(`${name}: report pagination omits retired items and preserves its progress cursor`, async () => {
    const store = await factory(); let retire = false; let selectedCursor;
    const runner = createRunner({ store: { ...store, async list(namespace, query) {
        const page = await store.list(namespace, query);
        if (retire) { retire = false; selectedCursor = page.next; await sleep(10); await store.maintain(namespace); }
        return page;
    } }, namespace: `read-page-${crypto.randomUUID()}`, validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    try {
        await runner.run({ flow: 'a', kind: 'result', result: 1 }); await runner.run({ flow: 'a', kind: 'result', result: 2 });
        retire = true; const page = await runner.list({ limit: 1 });
        assert(selectedCursor); assert.equal(page.next, selectedCursor); assert.deepEqual(page.items, []);
        assert.deepEqual(await runner.list({ after: page.next, limit: 1 }), { items: [], next: null });
    } finally { await runner.close(); await store.close(); }
});

test('PostgreSQL reports/export hold one execution view across retirement and reopening', { skip: !process.env.DBC_POSTGRES_URL, timeout: 30000 }, async () => {
    const { Pool } = await import('pg'); const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL, statement_timeout: 10000 });
    let interception;
    const hookedPool = { async connect() {
        const client = await pool.connect();
        return { release: () => client.release(), async query(sql, parameters) {
            const result = await client.query(sql, parameters);
            if (interception && sql.startsWith('SELECT value,data FROM dbc3_tasks WHERE id=$1') && parameters[0] === interception.taskId) {
                const hook = interception; interception = null; hook.entered.resolve(); await hook.release.promise;
            }
            return result;
        } };
    } };
    const store = await createPostgresStore({ pool, retentionMs: 1 }); const readerStore = await createPostgresStore({ pool: hookedPool });
    try {
        for (const mode of ['retirement', 'reopening']) {
            const options = { namespace: `read-view-${crypto.randomUUID()}`, validators: { v: { revision: '1', validate: value => value } },
                connectors: { sink: createCallbackConnector(async () => { throw new ConnectorError('REJECTED', 'Rejected.', { ambiguous: false, retryable: false }); }) },
                flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                    { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
                ] } } };
            const runner = createRunner({ ...options, store }); const reader = createRunner({ ...options, store: readerStore });
            const hook = { entered: Promise.withResolvers(), release: Promise.withResolvers() }; let reading; let writing;
            try {
                const task = await runner.run({ flow: 'a', kind: 'result', result: 1 }); assert.equal(task.status, 'partial');
                await sleep(10); hook.taskId = task.taskId; interception = hook; reading = reader.openSnapshot(task.taskId);
                await bounded(() => hook.entered.promise, { timeoutMs: 5000 });
                assert.equal(await bounded(() => store.get(task.taskId, 'another-namespace'), { timeoutMs: 1000 }), null);
                let written = false;
                writing = (mode === 'retirement' ? runner.maintain() : runner.retryDelivery(task.deliveries[0].id)).then(value => { written = true; return value; });
                await sleep(30); assert.equal(written, false, 'A saved view must finish before its execution changes.');
                hook.release.resolve(); const lease = await reading; await writing;
                const saved = { record: await reader.store.snapshotRecord(reader.namespace, lease.id) }; await reader.closeSnapshot(lease.id);
                assert.equal(saved.record.task.state, 'complete');
                assert.equal(saved.record.jobs.find(job => job.kind === 'delivery').state, 'failed');
                assert.equal(saved.record.jobs.length, 2);
                const current = await store.get(task.taskId, runner.namespace);
                if (mode === 'retirement') assert.equal(current, null);
                else { assert.equal(current.task.state, 'active'); assert.equal(current.task.expiresAt, undefined); }
            } finally { hook.release.resolve(); await Promise.allSettled([reading, writing]); await reader.close(); await runner.close(); }
        }
    } finally { await readerStore.close(); await store.close(); await pool.end(); }
});
