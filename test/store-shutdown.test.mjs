import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createMemoryStore } from '../src/storage/memory.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

for (const backend of ['memory', 'sqlite']) test(`${backend}: close fences new transactions and drains its accepted queue`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-store-close-'));
    const filename = join(directory, 'state.sqlite');
    const store = backend === 'memory' ? createMemoryStore() : createSQLiteStore(filename);
    const namespace = `close-${crypto.randomUUID()}`;
    try {
        const queued = [1, 2, 3].map(index => store.aggregate(namespace, 'evidence', current => {
            if (index === 2) throw new Error('Rejected update');
            return { state: 'pending', evidence: [...(current?.evidence ?? []), index] };
        }));
        const results = Promise.allSettled(queued);
        const closing = store.close();
        assert.equal(store.close(), closing);
        await assert.rejects(store.inspect(namespace), { code: 'STORE_CLOSED' });
        const outcomes = await results;
        assert.deepEqual(outcomes.map(value => value.status), ['fulfilled', 'rejected', 'fulfilled']);
        assert.match(outcomes[1].reason.message, /Rejected update/);
        assert.deepEqual(outcomes[2].value.evidence, [1, 3]);
        await closing; await store.close();
        await assert.rejects(store.inspect(namespace), { code: 'STORE_CLOSED' });
        if (backend === 'sqlite') {
            const reopened = createSQLiteStore(filename);
            try { assert.deepEqual((await reopened.aggregate(namespace, 'evidence', value => value)).evidence, [1, 3]); }
            finally { await reopened.close(); }
        }
    } finally { await store.close(); await rm(directory, { recursive: true }); }
});

// Intercept real clients to cover waits before acquisition, rollback/retry and release.
for (const phase of ['acquisition', 'statement', 'retry', 'commit']) test(`PostgreSQL borrowed-pool close drains ${phase}`, {
    skip: !process.env.DBC_POSTGRES_URL, timeout: 20000,
}, async () => {
    const { Pool } = await import('pg');
    const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL, statement_timeout: 10000 });
    const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    let intercept = false; let clients = 0; let ends = 0; let acquisitions = 0; let store; let operation; let closing;
    const failure = new Error('Acquisition failed');
    const borrowed = {
        async connect() {
            acquisitions++;
            if (intercept && phase === 'acquisition') {
                intercept = false; entered.resolve(); await release.promise; throw failure;
            }
            const client = await pool.connect(); clients++;
            return {
                release() { clients--; client.release(); },
                async query(sql, parameters) {
                    const result = await client.query(sql, parameters);
                    if (intercept && (phase === 'commit' ? sql === 'COMMIT' : sql.startsWith('SELECT count(*)'))) {
                        intercept = false; entered.resolve(); await release.promise;
                        if (phase === 'retry') throw Object.assign(new Error('Serialization failure'), { code: '40001' });
                    }
                    return result;
                },
            };
        },
        async end() { ends++; await pool.end(); },
    };
    try {
        store = await createPostgresStore({ pool: borrowed }); acquisitions = 0; intercept = true;
        operation = store.inspect(`close-${crypto.randomUUID()}`);
        const result = phase === 'acquisition' ? assert.rejects(operation, error => error === failure) : operation;
        await bounded(() => entered.promise, { timeoutMs: 5000 });
        let closed = false;
        closing = store.close(); closing.then(() => { closed = true; });
        assert.equal(store.close(), closing);
        await assert.rejects(store.inspect('after-close'), { code: 'STORE_CLOSED' });
        await new Promise(resolve => setImmediate(resolve)); assert.equal(closed, false);
        release.resolve(); await result; await closing;
        assert.equal(clients, 0); assert.equal(ends, 0);
        assert.equal(acquisitions, phase === 'retry' ? 2 : 1);
        assert.equal((await pool.query('SELECT 1 AS available')).rows[0].available, 1);
        await store.close(); assert.equal(ends, 0);
    } finally { release.resolve(); await Promise.allSettled([operation, closing]); await store?.close(); await pool.end(); }
});

test('PostgreSQL owned-pool close shares its drain and remains idempotent', {
    skip: !process.env.DBC_POSTGRES_URL, timeout: 20000,
}, async () => {
    const store = await createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, max: 1 });
    try {
        const queued = [store.inspect('first'), store.inspect('second')];
        const closing = store.close(); assert.equal(store.close(), closing);
        await assert.rejects(store.inspect('late'), { code: 'STORE_CLOSED' });
        await Promise.all(queued); await closing; await store.close();
    } finally { await store.close(); }
});
