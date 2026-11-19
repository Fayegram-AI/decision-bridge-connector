import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as sleep } from 'node:timers/promises';
import { TABLES } from '../src/storage/repository.mjs';
import { qualificationRunner } from './qualification-fixture.mjs';

// Explicit opt-in service URL; this script creates only an isolated random namespace.
const directory = await mkdtemp(join(tmpdir(), 'dbc-qualification-'));
const backends = ['sqlite', ...(process.env.DBC_POSTGRES_URL ? ['postgres'] : [])];
const active = new Set();
const results = [];
function worker(backend, path, namespace, messages, crash = false) {
    const child = fork(new URL('./qualification-worker.mjs', import.meta.url), [backend, path, namespace, String(crash)], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
    active.add(child); let errors = '';
    child.stderr.on('data', chunk => { errors = (errors + chunk).slice(-10000); });
    const exited = new Promise((resolve, reject) => child.once('exit', (code, signal) => { active.delete(child); if (code && !crash) reject(Error(errors)); else resolve({ code, signal }); }));
    exited.catch(() => {});
    const ready = new Promise((resolve, reject) => {
        child.once('error', reject);
        child.on('message', message => { messages.push(message); if (message.type === 'ready') resolve(); });
        child.once('exit', () => reject(Error(`Worker exited before readiness: ${errors}`)));
    });
    return { child, ready, exited };
}
async function until(check, label, timeoutMs = 60000) {
    const end = Date.now() + timeoutMs;
    while (!await check()) { assert(Date.now() < end, `${label} timed out`); await sleep(20); }
}
async function seed(backend, path, namespace, task) {
    const row = { ...task, state: 'complete' };
    if (backend === 'postgres') {
        const { Client } = await import('pg'); const client = new Client({ connectionString: process.env.DBC_POSTGRES_URL }); await client.connect();
        try {
            await client.query(`INSERT INTO dbc3_tasks(id,namespace,state,value) SELECT $1 || '-' || lpad(n::text,6,'0'),$1,'complete',
                jsonb_set($2::jsonb,'{id}',to_jsonb($1 || '-' || lpad(n::text,6,'0')))::text FROM generate_series(1,100000) n`, [namespace, JSON.stringify(row)]);
            // Claim plans must be measured against retained jobs as well as tasks;
            // a fresh database legitimately scans a two-row job table sequentially.
            await client.query(`INSERT INTO dbc3_jobs(id,namespace,taskId,kind,flow,state,runAt,priority,value)
                SELECT $1 || '-' || lpad(n::text,6,'0') || ':retained',$1,$1 || '-' || lpad(n::text,6,'0'),
                'evaluation','load','acknowledged',0,0,
                jsonb_build_object('id',$1 || '-' || lpad(n::text,6,'0') || ':retained','namespace',$1,
                    'taskId',$1 || '-' || lpad(n::text,6,'0'),'kind','evaluation','flow','load','state','acknowledged',
                    'runAt',0,'priority',0)::text FROM generate_series(1,100000) n`, [namespace]);
            await client.query('ANALYZE dbc3_tasks'); await client.query('ANALYZE dbc3_jobs');
            const list = await client.query('EXPLAIN SELECT value FROM dbc3_tasks WHERE namespace=$1 AND id>$2 ORDER BY id LIMIT 100', [namespace, namespace + '-050000']);
            const claim = await client.query("EXPLAIN SELECT value FROM dbc3_jobs WHERE namespace=$1 AND kind='evaluation' AND state IN ('pending','retry_pending') AND runAt<=$2 ORDER BY priority,runAt,id LIMIT 128", [namespace, Date.now()]);
            return { list: list.rows.map(row => row['QUERY PLAN']), claim: claim.rows.map(row => row['QUERY PLAN']) };
        } finally { await client.end(); }
    }
    const db = new DatabaseSync(path);
    try {
        db.exec('BEGIN');
        const insert = db.prepare('INSERT INTO dbc3_tasks(id,namespace,state,value) VALUES (?,?,?,?)');
        const insertJob = db.prepare('INSERT INTO dbc3_jobs(id,namespace,taskId,kind,flow,state,runAt,priority,value) VALUES (?,?,?,?,?,?,?,?,?)');
        for (let n = 1; n <= 100000; n++) {
            const id = `${namespace}-${String(n).padStart(6, '0')}`;
            insert.run(id, namespace, 'complete', JSON.stringify({ ...row, id }));
            const job = { id: `${id}:retained`, namespace, taskId: id, kind: 'evaluation', flow: 'load', state: 'acknowledged', runAt: 0, priority: 0 };
            insertJob.run(job.id, namespace, id, job.kind, job.flow, job.state, job.runAt, job.priority, JSON.stringify(job));
        }
        db.exec('COMMIT; ANALYZE;');
        return { list: db.prepare('EXPLAIN QUERY PLAN SELECT value FROM dbc3_tasks WHERE namespace=? AND id>? ORDER BY id LIMIT 100').all(namespace, namespace + '-050000'),
            claim: db.prepare("EXPLAIN QUERY PLAN SELECT value FROM dbc3_jobs WHERE namespace=? AND kind='evaluation' AND state IN ('pending','retry_pending') AND runAt<=? ORDER BY priority,runAt,id LIMIT 128").all(namespace, Date.now()) };
    } finally { db.close(); }
}
try {
    for (const backend of backends) {
        const path = join(directory, `${backend}.sqlite`); const namespace = `qualification-${crypto.randomUUID()}`;
        const { runner, store } = await qualificationRunner({ backend, path, namespace });
        try {
            const initial = await runner.submit({ flow: 'load', kind: 'request', request: 'seed-template' });
            await runner.cancel(initial.taskId);
            const plans = await seed(backend, path, namespace, (await store.get(initial.taskId, namespace, { metadata: true })).task);
            assert.match(JSON.stringify(plans.list), /INDEX|Index/);
            assert.match(JSON.stringify(plans.claim), /INDEX|Index/);
            const pageStart = performance.now();
            assert.equal((await runner.list({ after: namespace + '-050000', limit: 100 })).items.length, 100);
            results.push({ backend, retainedRecords: 100000, pageMs: performance.now() - pageStart, plans });
            for (const count of [1, 2, 4]) {
                const messages = []; const workers = Array.from({ length: count }, () => worker(backend, path, namespace, messages));
                await Promise.all(workers.map(item => item.ready));
                const start = performance.now();
                for (let i = 0; i < 40; i++) await runner.submit({ flow: 'load', kind: 'request', request: `${count}-${i}` });
                try { await until(async () => (await runner.pending()) === 0, `${backend}/${count} drain`); }
                catch (error) {
                    const active = await runner.list({ state: 'active' });
                    const jobs = (await Promise.all(active.items.map(item => store.get(item.taskId, namespace))))
                        .flatMap(record => record.jobs.filter(job => job.state === 'active').map(job => ({
                            id: job.id, kind: job.kind, stage: job.stage, leaseUntil: job.leaseUntil, observedAt: Date.now(),
                        })));
                    console.error(JSON.stringify({ backend, count, state: await runner.inspect(), active, jobs, messages }, null, 4)); throw error;
                }
                for (const item of workers) item.child.send('stop'); await Promise.all(workers.map(item => item.exited));
                const deliveries = messages.filter(item => item.type === 'delivery');
                const evaluations = messages.filter(item => item.type === 'evaluation');
                assert.equal(evaluations.length, 40); assert.equal(deliveries.length, 40);
                assert.equal(new Set(deliveries.map(item => item.id)).size, 40);
                results.push({ backend, workers: count, tasks: 40, elapsedMs: performance.now() - start,
                    evaluations: evaluations.length, uniqueDeliveries: deliveries.length, workerRss: messages.filter(item => item.type === 'memory').map(item => item.rss) });
            }
            const messages = []; const doomed = worker(backend, path, namespace, messages, true); await doomed.ready;
            const task = await runner.submit({ flow: 'load', kind: 'request', request: 'crash-before-response' });
            await until(() => messages.some(item => item.type === 'evaluation'), 'crash dispatch');
            doomed.child.kill('SIGKILL'); await doomed.exited;
            await sleep(5100); await runner.recover();
            assert.equal((await runner.get(task.taskId)).evaluation.status, 'unknown');
            assert.equal(await runner.tick(), 0);
            results.push({ backend, crashRecovery: 'ambiguous inference held; no automatic replay' });
        } finally {
            await runner.close(); await store.close();
            if (backend === 'postgres') {
                const { Client } = await import('pg'); const client = new Client({ connectionString: process.env.DBC_POSTGRES_URL }); await client.connect();
                try { for (const table of TABLES) await client.query(`DELETE FROM dbc3_${table} WHERE namespace=$1`, [namespace]); }
                finally { await client.end(); }
            }
        }
    }
    console.log(JSON.stringify({ platform: process.platform, node: process.version, results }, null, 4));
} finally { for (const child of active) child.kill('SIGKILL'); await rm(directory, { recursive: true, force: true }); }
