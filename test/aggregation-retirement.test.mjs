import test from 'node:test';
import assert from 'node:assert/strict';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

for (const first of ['update', 'retirement']) test(`PostgreSQL ${first} orders aggregation retirement with group updates at capacity`, {
    skip: !process.env.DBC_POSTGRES_URL, timeout: 30000,
}, async () => {
    const { Pool } = await import('pg');
    const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL, statement_timeout: 10000 });
    const namespace = `retire-group-${crypto.randomUUID()}`;
    const recordId = JSON.stringify([namespace, 'aggregate:first']);
    const read = Promise.withResolvers(); const releaseRead = Promise.withResolvers();
    const retiring = Promise.withResolvers(); const releaseRetirement = Promise.withResolvers();
    const waiting = Promise.withResolvers();
    let observe = false; let paused = false; let updaterGate = false; let updater; let maintenance; let updating; let maintaining;
    const wrapped = role => ({ async connect() {
        const client = await pool.connect();
        return { release: () => client.release(), async query(sql, parameters) {
            const groupGate = sql.includes('pg_advisory_xact_lock(hashtext($1))') && parameters?.[0] === recordId;
            const groupRead = sql.startsWith('SELECT value,data FROM dbc3_aggregates WHERE id=$1') && parameters[0] === recordId;
            if (observe && groupGate && role === 'updater') updaterGate = true;
            if (observe && groupGate && role === (first === 'update' ? 'maintenance' : 'updater')) waiting.resolve();
            // A retiring writer now also blocks the protected hydration read.
            if (observe && first === 'retirement' && groupRead && role === 'updater' && !updaterGate) waiting.resolve();
            const result = await client.query(sql, parameters);
            // Hold the write phase, after the optimistic read released its pins.
            if (observe && groupRead && role === 'updater' && updaterGate && sql.includes('FOR UPDATE') && !paused) {
                paused = true; read.resolve(); await releaseRead.promise;
            }
            if (observe && first === 'retirement' && groupRead && role === 'maintenance' && sql.includes('FOR UPDATE')) {
                retiring.resolve(); await releaseRetirement.promise;
            }
            return result;
        } };
    } });
    const replacement = current => ({ ...current, ruleId: 'one', aggregateId: 'replacement', state: 'pending',
        emitted: false, expiresAt: Date.now() + 60000, evidence: ['replacement'] });
    try {
        updater = await createPostgresStore({ pool: wrapped('updater') });
        maintenance = await createPostgresStore({ pool: wrapped('maintenance') });
        await maintenance.aggregate(namespace, 'first', () => ({ ruleId: 'one', aggregateId: 'old',
            state: 'complete', emitted: true, expiresAt: 0, evidence: ['old'] }), 1);
        observe = true;
        if (first === 'update') {
            updating = updater.aggregate(namespace, 'first', replacement, 1);
            await bounded(() => read.promise, { timeoutMs: 5000 });
            let finished = false;
            maintaining = maintenance.maintain(namespace).then(value => { finished = true; return value; });
            await bounded(() => waiting.promise, { timeoutMs: 5000 });
            await new Promise(resolve => setImmediate(resolve)); assert.equal(finished, false);
            await assert.rejects(maintenance.aggregate(namespace, 'second', () => ({ ruleId: 'one', state: 'pending' }), 1), { code: 'RESOURCE_LIMIT' });
            releaseRead.resolve(); await updating; await maintaining;
            const current = await updater.aggregate(namespace, 'first', value => value);
            assert.equal(current.aggregateId, 'replacement'); assert.deepEqual(current.evidence, ['replacement']);
        } else {
            maintaining = maintenance.maintain(namespace);
            await bounded(() => retiring.promise, { timeoutMs: 5000 });
            updating = updater.aggregate(namespace, 'first', replacement, 1);
            updating.catch(() => {});
            await bounded(() => waiting.promise, { timeoutMs: 5000 });
            releaseRetirement.resolve(); await maintaining;
            await bounded(() => read.promise, { timeoutMs: 5000 });
            await maintenance.aggregate(namespace, 'second', () => ({ ruleId: 'one', aggregateId: 'second',
                state: 'pending', evidence: ['second'] }), 1);
            const rejected = assert.rejects(updating, { code: 'RESOURCE_LIMIT' });
            releaseRead.resolve(); await rejected;
            assert.equal(await updater.aggregate(namespace, 'first', value => value), null);
            assert.deepEqual((await updater.aggregate(namespace, 'second', value => value)).evidence, ['second']);
        }
        const { rows } = await pool.query('SELECT count(*)::int AS count FROM dbc3_aggregates WHERE namespace=$1 AND ruleId=$2', [namespace, 'one']);
        assert.equal(rows[0].count, 1);
    } finally {
        observe = false; releaseRead.resolve(); releaseRetirement.resolve();
        await Promise.allSettled([updating, maintaining]); await updater?.close(); await maintenance?.close(); await pool.end();
    }
});
