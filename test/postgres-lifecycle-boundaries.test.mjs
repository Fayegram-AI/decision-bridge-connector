import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';
import { createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { ADMISSION_OWNER } from '../src/engine/admission-owner.mjs';
import { bounded } from '../src/util.mjs';

const enabled = !!process.env.DBC_POSTGRES_URL;

/** Pause after the database acquired a row lock, keeping actual PostgreSQL semantics. */
function interceptedPool(pool, predicate, entered, release) {
    let intercept = true;
    return { async connect() {
        const client = await pool.connect();
        return { release: () => client.release(), async query(sql, parameters) {
            const result = await client.query(sql, parameters);
            if (intercept && predicate(sql, parameters)) { intercept = false; entered.resolve(); await release.promise; }
            return result;
        } };
    } };
}

for (const filtered of [false, true]) test(`PostgreSQL ${filtered ? 'filtered' : 'unfiltered'} expiry/failure claims gate flows before job rows`, { skip: !enabled, timeout: 30000 }, async () => {
    const { Pool } = await import('pg');
    const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL, statement_timeout: 10000 });
    const namespace = `flow-order-${crypto.randomUUID()}`;
    const flowHeld = Promise.withResolvers(); const failureWaiting = Promise.withResolvers(); let deadlocks = 0;
    const wrap = kind => ({ async connect() {
        const client = await pool.connect();
        return { release: () => client.release(), async query(sql, args) {
            const flowGate = sql.includes('pg_advisory_xact_lock(hashtext($1))') && args?.[0] === JSON.stringify([namespace, 'claim-flow:a']);
            if (flowGate && kind === 'failure') failureWaiting.resolve();
            try {
                const result = await client.query(sql, args);
                if (flowGate && kind === 'delivery') { flowHeld.resolve(); await bounded(() => failureWaiting.promise, { timeoutMs: 5000 }); }
                return result;
            } catch (error) { if (error.code === '40P01') deadlocks++; throw error; }
        } };
    } });
    let delivery; let failure; let one; let two;
    try {
        delivery = await createPostgresStore({ pool: wrap('delivery') });
        failure = await createPostgresStore({ pool: wrap('failure') });
        const taskId = `task-${namespace}`;
        const common = { namespace, taskId, flow: 'a', state: 'pending', runAt: 0, priority: 0, required: 1, attempts: [] };
        await delivery.admit({ id: taskId, namespace, rootId: taskId, flow: 'a', state: 'active', plan: { concurrency: 4, maxExecutions: 10 } }, [
            { ...common, id: `${taskId}:delivery`, kind: 'delivery', stage: 'prepared', expiresAt: 0, destination: 'sink', plan: { when: 'always', dependsOn: [] } },
            { ...common, id: `${taskId}:failure`, kind: 'failure', plan: {} },
        ]);
        one = delivery.claim(namespace, 'delivery', { worker: 'delivery', ...(filtered ? { flow: 'a' } : {}) });
        await bounded(() => flowHeld.promise, { timeoutMs: 5000 });
        two = failure.claim(namespace, 'failure', { worker: 'failure', ...(filtered ? { flow: 'a' } : {}) });
        const [expired, claimed] = await Promise.all([one, two]);
        assert.equal(expired, null); assert.equal(claimed.job.kind, 'failure'); assert.equal(deadlocks, 0);
        assert.equal((await delivery.get(taskId, namespace)).jobs.find(job => job.kind === 'delivery').state, 'expired');
    } finally { failureWaiting.resolve(); await Promise.allSettled([one, two]); await delivery?.close(); await failure?.close(); await pool.end(); }
});

test('PostgreSQL claims revalidate cancellation after waiting for a flow gate', { skip: !enabled, timeout: 20000 }, async () => {
    const { Pool } = await import('pg');
    const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL, statement_timeout: 10000 });
    const namespace = `flow-revalidate-${crypto.randomUUID()}`; const gate = JSON.stringify([namespace, 'claim-flow:a']);
    const waiting = Promise.withResolvers(); let held; let store; let claiming; let cancellation;
    try {
        held = await pool.connect();
        store = await createPostgresStore({ pool: { async connect() {
            const client = await pool.connect();
            return { release: () => client.release(), async query(sql, args) {
                if (sql.includes('pg_advisory_xact_lock(hashtext($1))') && args?.[0] === gate) waiting.resolve();
                return client.query(sql, args);
            } };
        } } });
        const taskId = `task-${namespace}`;
        await store.admit({ id: taskId, namespace, rootId: taskId, flow: 'a', state: 'active', plan: { concurrency: 1, maxExecutions: 10 } }, [
            { id: `${taskId}:evaluation`, taskId, namespace, flow: 'a', kind: 'evaluation', state: 'pending', runAt: 0, priority: 0, required: 1 },
        ]);
        await held.query('BEGIN'); await held.query('SELECT pg_advisory_xact_lock(hashtext($1))', [gate]);
        claiming = store.claim(namespace, 'evaluation', { worker: 'waiting', flow: 'a' });
        await bounded(() => waiting.promise, { timeoutMs: 5000 });
        cancellation = store.cancel(namespace, taskId);
        await bounded(() => cancellation, { timeoutMs: 5000 });
        await held.query('COMMIT'); assert.equal(await claiming, null);
        assert.equal((await store.get(taskId, namespace)).jobs[0].state, 'cancelled');
    } finally { if (held) { await held.query('ROLLBACK').catch(() => {}); held.release(); } await Promise.allSettled([claiming, cancellation]); await store?.close(); await pool.end(); }
});

test('PostgreSQL cancellation waits for a locked invocation marker and fences its outcome', { skip: !enabled, timeout: 30000 }, async () => {
    const { Pool } = await import('pg'); const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL, statement_timeout: 10000 });
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let blockedId;
    const store = await createPostgresStore({ pool: interceptedPool(pool,
        (sql, args) => sql.includes('FROM dbc3_jobs WHERE id=$1 FOR UPDATE') && args[0] === blockedId, entered, release) });
    const runner = createRunner({ store, namespace: `locked-cancel-${crypto.randomUUID()}`,
        validators: { v: { revision: '1', validate: value => value } }, connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged' })) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: ['first', 'second'].map(id => ({ id, connector: 'sink', when: 'always', payload: 'raw-result' })) } } });
    let marker; let cancellation;
    try {
        const task = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
        const evaluation = await store.claim(runner.namespace, 'evaluation', { worker: 'planner', taskId: task.taskId });
        await store.commit(evaluation.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' },
            jobs: task.deliveries.map(job => ({ id: job.id, state: 'pending', payload: 1 })) });
        const delivery = await store.claim(runner.namespace, 'delivery', { worker: 'worker', taskId: task.taskId });
        blockedId = delivery.job.id; marker = store.commit(delivery.job, { job: { stage: 'invoked' } });
        await bounded(() => entered.promise, { timeoutMs: 5000 });
        let returned = false; cancellation = runner.cancel(task.taskId).then(value => { returned = true; return value; });
        await sleep(30); assert.equal(returned, false, 'Cancellation must wait for locked jobs.');
        release.resolve(); await marker; const cancelled = await cancellation;
        assert.equal(cancelled.deliveries.find(job => job.id === blockedId).status, 'unknown');
        assert.equal(cancelled.deliveries.find(job => job.id !== blockedId).status, 'cancelled');
        await assert.rejects(store.commit(delivery.job, { job: { state: 'acknowledged' } }), { code: 'STALE_LEASE' });
    } finally { release.resolve(); await Promise.allSettled([marker, cancellation]); await runner.close(); await store.close(); await pool.end(); }
});

test('PostgreSQL internal admission locks its owning group against concurrent settlement', { skip: !enabled, timeout: 30000 }, async () => {
    const { Pool } = await import('pg'); const pool = new Pool({ connectionString: process.env.DBC_POSTGRES_URL, statement_timeout: 10000 });
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let intercept = false;
    const store = await createPostgresStore({ pool: interceptedPool(pool,
        sql => intercept && sql.includes('FROM dbc3_aggregates WHERE id=$1 FOR UPDATE'), entered, release) });
    const runner = createRunner({ store, namespace: `locked-owner-${crypto.randomUUID()}`,
        evaluators: { e: { revision: '1', execute: async value => value } },
        flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    let admission; let settlement;
    try {
        await store.aggregate(runner.namespace, 'group', () => ({ aggregateId: 'group-id', state: 'complete', emitted: false, expiresAt: 0 }));
        intercept = true;
        admission = runner.submit({ flow: 'a', kind: 'event', event: 1, idempotencyKey: 'group-id' }, {
            source: 'aggregate:one', [ADMISSION_OWNER]: { kind: 'aggregate', key: 'group', aggregateId: 'group-id' },
        });
        await bounded(() => entered.promise, { timeoutMs: 5000 });
        let settled = false; settlement = store.aggregate(runner.namespace, 'group', current => ({ ...current, emitted: true })).then(value => { settled = true; return value; });
        await sleep(30); assert.equal(settled, false);
        release.resolve(); assert((await admission).taskId); await settlement;
        assert.equal((await runner.list()).items.length, 1);
    } finally { release.resolve(); await Promise.allSettled([admission, settlement]); await runner.close(); await store.close(); await pool.end(); }
});

test('an owned PostgreSQL pool survives idle connection termination and reconnects', { skip: !enabled, timeout: 20000 }, async () => {
    const moduleUrl = new URL('../src/storage/postgres.mjs', import.meta.url).href;
    const script = `
        import { Pool } from 'pg';
        import { setTimeout as sleep } from 'node:timers/promises';
        import { createPostgresStore } from ${JSON.stringify(moduleUrl)};
        const url = new URL(process.env.DBC_POSTGRES_URL); url.searchParams.set('application_name', process.env.DBC_PG_APPLICATION);
        const store = await createPostgresStore({ connectionString: url.href });
        const killer = new Pool({ connectionString: process.env.DBC_POSTGRES_URL });
        try {
            await killer.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=$1', [process.env.DBC_PG_APPLICATION]);
            await sleep(200); await store.inspect('idle-recovery'); console.log('idle-reconnected');
        } finally { await store.close(); await killer.end(); }
    `;
    const result = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script], {
        cwd: fileURLToPath(new URL('../', import.meta.url)), timeout: 15000, encoding: 'utf8',
        env: { ...process.env, DBC_PG_APPLICATION: `dbc-idle-${crypto.randomUUID()}` },
    });
    assert.equal(result.stdout.trim(), 'idle-reconnected');
});

test('failed owned PostgreSQL setup releases its pool without leaving an idle socket', { skip: !enabled, timeout: 15000 }, async () => {
    const moduleUrl = new URL('../src/storage/postgres.mjs', import.meta.url).href;
    const script = `
        import { Pool } from 'pg';
        import { createPostgresStore } from ${JSON.stringify(moduleUrl)};
        const admin = new Pool({ connectionString: process.env.DBC_POSTGRES_URL });
        const schema = 'dbc_setup_' + crypto.randomUUID().replaceAll('-', '');
        try {
            await admin.query('CREATE SCHEMA ' + schema);
            await admin.query('CREATE TABLE ' + schema + '.dbc3_tasks (id TEXT PRIMARY KEY)');
            const url = new URL(process.env.DBC_POSTGRES_URL); url.searchParams.set('options', '-c search_path=' + schema);
            let rejected = false;
            try { await createPostgresStore({ connectionString: url.href }); } catch { rejected = true; }
            if (!rejected) throw new Error('Invalid schema was accepted');
            console.log('setup-released');
        } finally { await admin.query('DROP SCHEMA IF EXISTS ' + schema + ' CASCADE'); await admin.end(); }
    `;
    const result = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script], {
        cwd: fileURLToPath(new URL('../', import.meta.url)), timeout: 7000, encoding: 'utf8', env: process.env,
    });
    assert.equal(result.stdout.trim(), 'setup-released');
});
