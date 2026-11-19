import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createCallbackConnector } from '../src/index.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded, delay } from '../src/util.mjs';

/** Pause a metadata reader while a real PostgreSQL writer replaces its evidence. */
async function concurrentEvidence(table, operation) {
    const { Pool } = await import('pg');
    const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL });
    const captured = Promise.withResolvers(); const resume = Promise.withResolvers();
    let armed = false; let readerPaused = false; let writerPid;
    const wrapped = role => ({ async connect() {
        const client = await pool.connect();
        return { release: () => client.release(), async query(sql, parameters) {
            const metadata = armed && sql.startsWith(`SELECT value,data FROM dbc3_${table} WHERE id=$1`);
            if (metadata && role === 'writer') writerPid = (await client.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
            const result = await client.query(sql, parameters);
            if (metadata && role === 'reader' && !readerPaused) {
                readerPaused = true; captured.resolve(); await resume.promise;
            }
            return result;
        } };
    } });
    const reader = await createPostgresStore({ pool: wrapped('reader') });
    const writer = await createPostgresStore({ pool: wrapped('writer') });
    const collector = await createPostgresStore({ pool });
    const namespace = `hydration-${crypto.randomUUID()}`; const pending = [];
    try {
        await operation({ reader, writer, namespace, async replaceWhileReading(read, replace) {
            armed = true;
            const reading = read(); reading.catch(() => {}); pending.push(reading);
            await bounded(() => captured.promise, { timeoutMs: 5000 });
            let replaced = false;
            const replacing = replace().then(value => { replaced = true; return value; }, error => { replaced = true; throw error; });
            replacing.catch(() => {}); pending.push(replacing);
            // Old code permits replacement to finish. Protected reads block the writer.
            // Detect either state without depending on an arbitrary scheduling delay.
            await bounded(async signal => {
                while (!replaced && !signal.aborted) {
                    if (writerPid && (await pool.query('SELECT cardinality(pg_blocking_pids($1)) AS blockers', [writerPid])).rows[0].blockers > 0) return;
                    await delay(5);
                }
            }, { timeoutMs: 5000 });
            await collector.maintain(namespace);
            resume.resolve();
            const value = await reading; await replacing; return value;
        } });
    } finally {
        armed = false; resume.resolve(); await Promise.allSettled(pending);
        await reader.close(); await writer.close(); await collector.close(); await pool.end();
    }
}

test('PostgreSQL aggregation reads retain evidence through concurrent replacement and collection',
    { skip: !process.env.DBC_POSTGRES_URL, timeout: 15000 }, async () => {
        await concurrentEvidence('aggregates', async ({ reader, writer, namespace, replaceWhileReading }) => {
            await writer.aggregate(namespace, 'group', () => ({ state: 'pending', events: { evidence: 'old' } }));
            const observed = [];
            await replaceWhileReading(() => reader.aggregate(namespace, 'group', current => {
                observed.push(current.events.evidence); return { ...current, context: 'reader' };
            }), () => writer.aggregate(namespace, 'group', current => ({ ...current, events: { evidence: 'new' } })));
            assert.equal(observed[0], 'old');
            const final = await writer.aggregate(namespace, 'group', current => current);
            assert.deepEqual(final.events, { evidence: 'new' }); assert.equal(final.context, 'reader');
        });
    });

test('PostgreSQL delivery reads retain completion evidence through concurrent replacement and collection',
    { skip: !process.env.DBC_POSTGRES_URL, timeout: 15000 }, async () => {
        await concurrentEvidence('jobs', async ({ reader, writer, namespace, replaceWhileReading }) => {
            const runner = createRunner({ store: writer, namespace, validators: { v: { revision: '1', validate: value => value } },
                connectors: { sink: createCallbackConnector(() => ({ status: 'acknowledged', receipt: { response: 'child-input' } })) },
                flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                    { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
                ] } } });
            try {
                const task = await runner.run({ flow: 'a', kind: 'result', result: 1 }); const jobId = `${task.taskId}:delivery:out`;
                await runner.recordCompletion(jobId, { status: 'completed', evidence: 'old' });
                const saved = await replaceWhileReading(() => reader.getJob(namespace, jobId),
                    () => writer.mutateJob(namespace, jobId, 'completion', { status: 'completed', evidence: 'new' }));
                assert.deepEqual(saved.completion, { status: 'completed', evidence: 'old' });
                assert.equal(saved.receipt.receipt.response, 'child-input');
                assert.deepEqual((await writer.getJob(namespace, jobId)).completion, { status: 'completed', evidence: 'new' });
            } finally { await runner.close(); }
        });
    });
