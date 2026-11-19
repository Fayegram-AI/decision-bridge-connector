import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createMemoryStore, createRunner } from '../src/index.mjs';
import { bounded, delay } from '../src/util.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

test('a separate SQLite store recovers an unused rate reservation before a late claimant returns', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-claim-recovery-'));
    const filename = join(directory, 'state.sqlite');
    const owner = createSQLiteStore(filename, { leaseMs: 50 });
    const recovery = createSQLiteStore(filename, { leaseMs: 50 });
    const runner = createRunner({ store: owner, evaluators: { e: { revision: '1', execute: value => value } },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e', rate: 0.1 } } });
    try {
        const { taskId } = await runner.submit({ flow: 'a', kind: 'request', request: 42 });
        const options = { worker: 'slow', flow: 'a', taskId };
        const first = await owner.claim(runner.namespace, 'evaluation', options);
        assert.ok(first);
        await delay(Math.max(0, first.job.leaseUntil - Date.now() + 10));
        assert.equal(await recovery.recover(runner.namespace), 1);
        const next = await recovery.claim(runner.namespace, 'evaluation', { ...options, worker: 'recovery' });
        assert.ok(next, 'Recovery must clear the expired claim’s unused ten-second slot.');
        assert.equal(await owner.releaseClaim(first.job), false);
        assert.equal(await recovery.releaseClaim(next.job), true);
    } finally {
        await runner.close();
        await Promise.all([owner.close(), recovery.close()]);
        await rm(directory, { recursive: true, force: true });
    }
});

for (const mode of ['timeout', 'abort']) {
    test(`execution ${mode} releases a late claim without invoking the evaluator`, async () => {
        const entered = Promise.withResolvers();
        const release = Promise.withResolvers();
        const requeued = Promise.withResolvers();
        const store = createMemoryStore();
        let calls = 0;
        const runner = createRunner({ store: { ...store, async claim(...args) {
            const claim = await store.claim(...args);
            if (claim && args[2].taskId) {
                entered.resolve();
                await release.promise;
            }
            return claim;
        }, async releaseClaim(claim) {
            const result = await store.releaseClaim(claim);
            requeued.resolve();
            return result;
        } }, evaluators: { e: { revision: '1', execute(value) { calls++; return value; } } },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e', rate: 0.1 } } });
        const controller = new AbortController();
        try {
            const task = await runner.submit({ flow: 'a', kind: 'request', request: 1 });
            const wait = runner.execute(task.taskId, { signal: controller.signal, waitMs: mode === 'abort' ? 5000 : 150 });
            void wait.catch(() => {});
            await bounded(() => entered.promise, { timeoutMs: 1500 });
            const returned = bounded(() => wait, { timeoutMs: 1500 });
            if (mode === 'abort') {
                controller.abort();
                await assert.rejects(returned, { code: 'ABORTED' });
            } else assert.equal((await returned).status, 'running');
            release.resolve();
            await bounded(() => requeued.promise, { timeoutMs: 1500 });
            assert.equal((await store.get(task.taskId, runner.namespace)).jobs[0].state, 'pending');
            assert.equal(calls, 0);
            assert.equal(await runner.tick(), 1, 'The abandoned claim must release the rate slot.');
            assert.equal((await runner.execute(task.taskId)).status, 'completed');
            assert.equal(calls, 1);
        } finally {
            release.resolve();
            await runner.close({ cancel: true });
            await store.close();
        }
    });
}

for (const [name, factory] of stores) test(`${name}: releasing an unused lease restores a scheduled retry`, async () => {
    const store = await factory();
    const namespace = `release-${crypto.randomUUID()}`;
    const taskId = `${namespace}:task`;
    const runAt = Date.now() - 100;
    const options = { worker: 'test-worker', taskId, flow: 'a' };
    try {
        await store.admit({ id: taskId, namespace, flow: 'a', state: 'active', rootId: taskId,
            createdAt: runAt, plan: { maxExecutions: 1, concurrency: 1, rate: 0.1 } }, [
            { id: `${taskId}:evaluation`, namespace, taskId, flow: 'a', kind: 'evaluation', state: 'retry_pending',
                stage: 'prepared', createdAt: runAt, runAt, priority: 0, required: 1, token: null, leaseUntil: null },
        ]);
        const first = await store.claim(namespace, 'evaluation', options);
        assert.equal(await store.releaseClaim(first.job), true);
        const [job] = (await store.get(taskId, namespace)).jobs;
        assert.equal(job.state, 'retry_pending');
        assert.equal(job.runAt, runAt);
        assert.equal(job.token, null);
        const second = await store.claim(namespace, 'evaluation', options);
        assert.ok(second);
        assert.equal(await store.releaseClaim(first.job), false);
        await store.commit(second.job, { job: { stage: 'invoked' } });
        assert.equal(await store.releaseClaim(second.job), false);
    } finally { await store.close(); }
});

for (const [name, factory] of stores) test(`${name}: bounded claim pages reach runnable work beyond blocked placements`, { timeout: 30000 }, async () => {
    const store = await factory(); const namespace = `claim-${crypto.randomUUID()}`;
    try {
        for (let index = 0; index < 129; index++) {
            const taskId = `${namespace}:${String(index).padStart(3, '0')}`;
            await store.admit({ id: taskId, namespace, state: 'active', rootId: taskId, flow: 'a',
                plan: { maxExecutions: 1, concurrency: 10 } }, [{ id: `${taskId}:evaluation`, taskId, namespace,
                flow: 'a', kind: 'evaluation', state: 'pending', priority: index === 128 ? 1 : 0,
                runAt: index === 128 ? 1 : 10, createdAt: 0, placement: index === 128 ? 'local' : 'remote', required: 1 }]);
        }
        const options = { worker: 'local-worker', placements: ['local'] };
        assert.equal(await store.claim(namespace, 'evaluation', options), null);
        const claim = await store.claim(namespace, 'evaluation', options);
        assert.equal(claim.job.taskId, `${namespace}:128`);
        // The next scan wraps; the claimed job cannot be invoked a second time.
        assert.equal(await store.claim(namespace, 'evaluation', options), null);
        const remote = await store.claim(namespace, 'evaluation', { worker: 'remote-worker', placements: ['remote'] });
        assert.equal(remote.job.taskId, `${namespace}:000`);
        // Claim cursors do not affect explicit task execution or fencing.
        assert.equal(await store.claim(namespace, 'evaluation', { ...options, taskId: claim.job.taskId }), null);
        await store.commit(claim.job, { job: { state: 'acknowledged', leaseUntil: null, token: null } });
        await assert.rejects(store.commit(claim.job, { job: { state: 'failed' } }), { code: 'STALE_LEASE' });
    } finally { await store.close(); }
});

test('claim scans remain bounded and expired scheduling cursors are pruned', async () => {
    const repository = createMemoryRepository(); const pages = [];
    const store = createExecutionStore({ ...repository, transaction: callback => repository.transaction(tx => callback({ ...tx,
        async find(table, where, options) {
            const rows = await tx.find(table, where, options);
            if (table === 'jobs') pages.push({ limit: options?.limit, size: rows.length, locked: options?.lock === true, candidates: where.id?.in?.length });
            return rows;
        },
    })) });
    try {
        await repository.transaction(async tx => {
            for (let index = 0; index < 260; index++) await tx.put('jobs', { id: String(index).padStart(3, '0'),
                namespace: 'bounded', kind: 'evaluation', state: 'pending', priority: 0, runAt: 1, placement: 'elsewhere' });
        });
        for (let index = 0; index < 4; index++) assert.equal(await store.claim('bounded', 'evaluation', { worker: 'one' }), null);
        assert.deepEqual(pages.filter(page => !page.locked).map(page => page.size), [128, 128, 4, 0, 128]);
        assert.deepEqual(pages.filter(page => page.locked).map(page => page.size), [128, 128, 4, 128]);
        assert(pages.filter(page => page.locked).every(page => page.candidates <= 128));
        assert(pages.every(page => page.limit === 128));
        await repository.transaction(async tx => {
            const [cursor] = await tx.find('controls', { namespace: 'bounded', kind: 'claim-cursor' });
            assert(cursor); await tx.put('controls', { ...cursor, expiresAt: 0 });
        });
        await store.maintain('bounded');
        assert.equal(await repository.transaction(tx => tx.count('controls', { namespace: 'bounded', kind: 'claim-cursor' })), 0);
    } finally { await store.close(); }
});

test('slow candidate selection does not return an expired lease', async context => {
    context.mock.timers.enable({ apis: ['Date'], now: 1000 });
    const namespace = 'selection-clock'; const repository = createMemoryRepository();
    const store = createExecutionStore({ ...repository, transaction: callback => repository.transaction(tx => callback({ ...tx,
        async get(table, key, ...args) {
            const value = await tx.get(table, key, ...args);
            if (table === 'controls' && key === JSON.stringify([namespace, 'pause:flow:a'])) context.mock.timers.setTime(2000);
            return value;
        },
    })) }, { leaseMs: 50 });
    try {
        for (const flow of ['a', 'b']) {
            const taskId = `task-${flow}`;
            await store.admit({ id: taskId, namespace, rootId: taskId, flow, state: 'active', plan: { maxExecutions: 1, concurrency: 1 } }, [
                { id: `${taskId}:evaluation`, taskId, namespace, flow, kind: 'evaluation', state: 'pending', priority: 0, runAt: 0, required: 1 },
            ]);
        }
        await store.pause(namespace, 'flow', 'a', true);
        const claim = await store.claim(namespace, 'evaluation', { worker: 'clock' });
        assert.equal(claim.job.flow, 'b'); assert(claim.job.leaseUntil > Date.now());
        await store.renew(claim.job);
    } finally { await store.close(); }
});
