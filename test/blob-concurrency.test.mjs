import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createCallbackConnector } from '../src/index.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

test('PostgreSQL concurrent planning and delivery share immutable blobs without deadlocking', { skip: !process.env.DBC_POSTGRES_URL, timeout: 30000 }, async () => {
    const { Pool } = await import('pg'); const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL });
    const planningHeld = Promise.withResolvers(); const deliveryHeld = Promise.withResolvers();
    let observe = false; let rootBlob; let connectionBlob; let deadlocks = 0; const paused = new Set();
    const wrapped = role => ({ async connect() {
        const client = await pool.connect(); return { release: () => client.release(), async query(sql, parameters) {
            let result;
            try { result = await client.query(sql, parameters); } catch (error) { if (error.code === '40P01') deadlocks++; throw error; }
            if (observe && !paused.has(role) && sql.startsWith('SELECT value,data FROM dbc3_blobs WHERE id=$1') && /FOR (UPDATE|KEY SHARE)$/.test(sql) &&
                parameters[0] === (role === 'planning' ? rootBlob : connectionBlob)) {
                paused.add(role);
                (role === 'planning' ? planningHeld : deliveryHeld).resolve();
                await bounded(() => (role === 'planning' ? deliveryHeld : planningHeld).promise, { timeoutMs: 5000 });
            }
            return result;
        } };
    } });
    const planning = await createPostgresStore({ pool: wrapped('planning') }); const delivery = await createPostgresStore({ pool: wrapped('delivery') });
    const namespace = `blob-locks-${crypto.randomUUID()}`;
    const runner = createRunner({ store: planning, namespace, validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(() => ({ status: 'acknowledged' })) }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v',
            connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] } } });
    let first; let second;
    try {
        const a = await runner.submit({ flow: 'a', kind: 'result', result: 1 }); const b = await runner.submit({ flow: 'a', kind: 'result', result: 2 });
        const prepared = await planning.get(a.taskId, namespace, { metadata: true });
        rootBlob = prepared.task.plan.$data.id; connectionBlob = prepared.jobs.find(job => job.kind === 'delivery').plan.$data.id;
        const evaluationB = await planning.claim(namespace, 'evaluation', { taskId: b.taskId, worker: 'setup' });
        await planning.commit(evaluationB.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' },
            jobs: [{ id: `${b.taskId}:delivery:out`, state: 'pending', payload: 2 }] });
        const evaluationA = await planning.claim(namespace, 'evaluation', { taskId: a.taskId, worker: 'planning' });
        const outgoingB = await delivery.claim(namespace, 'delivery', { taskId: b.taskId, worker: 'delivery' });
        observe = true;
        first = planning.commit(evaluationA.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' },
            jobs: [{ id: `${a.taskId}:delivery:out`, state: 'pending', payload: 1 }] });
        second = delivery.commit(outgoingB.job, { job: { state: 'acknowledged' } });
        await Promise.all([first, second]); assert.equal(paused.size, 2); assert.equal(deadlocks, 0);
        assert.equal((await delivery.get(b.taskId, namespace)).task.state, 'complete');
    } finally {
        observe = false; planningHeld.resolve(); deliveryHeld.resolve(); await Promise.allSettled([first, second]);
        await runner.close(); await planning.close(); await delivery.close(); await pool.end();
    }
});
