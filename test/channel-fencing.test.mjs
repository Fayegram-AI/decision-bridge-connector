import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createMemoryStore } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const configuration = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
const stores = [['memory', async () => ({ store: createMemoryStore(), cleanup: async () => {} })], ['sqlite', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-channel-fencing-'));
    return { store: createSQLiteStore(join(directory, 'state.sqlite')), cleanup: () => rm(directory, { recursive: true }) };
}], ...(process.env.DBC_POSTGRES_URL ? [['postgres', async () => ({
    store: await createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL }), cleanup: async () => {},
})]] : [])];

for (const [name, factory] of stores) test(`${name}: channel reservations prevent concurrent effects and fence superseded attempts`, { timeout: 30000 }, async () => {
    const { store, cleanup } = await factory(); const namespace = `channel-${crypto.randomUUID()}`;
    const first = createRunner({ ...configuration, store, namespace }); const second = createRunner({ ...configuration, store, namespace });
    const releases = []; const pending = [];
    try {
        const task = await first.run({ flow: 'a', kind: 'result', result: 1 });
        for (const [method, key, write] of [['reply', 'reply', 'recordReply'],
            ...(store.capabilities.durable ? [['acknowledge', 'sourceAcknowledgement', 'recordAcknowledgement']] : [])]) {
            const entered = Promise.withResolvers(); const release = Promise.withResolvers(); releases.push(release);
            let calls = 0;
            const active = first[method](task.taskId, async () => {
                calls++; entered.resolve(); await release.promise; return { status: 'acknowledged' };
            });
            active.catch(() => {}); pending.push(active); await entered.promise;
            await assert.rejects(second[method](task.taskId, () => { calls++; return { status: 'acknowledged' }; }), { code: 'CONFLICT' });
            assert.equal(calls, 1); release.resolve(); assert.equal((await active).status, 'acknowledged');

            // An expired operation may still finish externally after a replacement starts.
            await store[write](namespace, task.taskId, { status: 'in_flight', attemptId: 'expired-old', expiresAt: 0 });
            const nextEntered = Promise.withResolvers(); const nextRelease = Promise.withResolvers(); releases.push(nextRelease);
            const replacement = second[method](task.taskId, async () => {
                nextEntered.resolve(); await nextRelease.promise; return { status: 'acknowledged' };
            });
            replacement.catch(() => {}); pending.push(replacement); await nextEntered.promise;
            const before = await store.get(task.taskId, namespace);
            await assert.rejects(store[write](namespace, task.taskId, { status: 'acknowledged', attemptId: 'expired-old' }), { code: 'STALE_ATTEMPT' });
            const after = await store.get(task.taskId, namespace);
            assert.deepEqual(after.task[key], before.task[key]); assert.equal(after.task.leaseUntil, before.task.leaseUntil);
            await first.recover(); assert.equal((await first.get(task.taskId))[key].status, 'in_flight');
            nextRelease.resolve(); const disposition = await replacement;
            assert.equal(disposition.status, 'acknowledged');
            await assert.rejects(store[write](namespace, task.taskId, { status: 'unknown', attemptId: 'expired-old' }), { code: 'STALE_ATTEMPT' });
            assert.equal((await first.get(task.taskId))[key].attemptId, disposition.attemptId);
        }
    } finally {
        for (const release of releases) release.resolve(); await Promise.allSettled(pending);
        await first.close(); await second.close(); await store.close(); await cleanup();
    }
});

test('channel reservation and report reads consume the callback deadline', async t => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-channel-budget-'));
    const store = createSQLiteStore(join(directory, 'state.sqlite')); let clock = Date.now();
    t.mock.method(Date, 'now', () => clock);
    try {
        for (const [method, write, delayRead] of [['reply', 'recordReply', false], ['reply', 'recordReply', true],
            ['acknowledge', 'recordAcknowledgement', false]]) {
            let reserved = false; let calls = 0;
            const runner = createRunner({ ...configuration, namespace: `budget-${crypto.randomUUID()}`, store: { ...store,
                async [write](namespace, taskId, value) {
                    await store[write](namespace, taskId, value);
                    if (value.status === 'in_flight') { reserved = true; if (!delayRead) clock += 101; }
                },
                async get(...args) {
                    const result = await store.get(...args); if (reserved && delayRead) clock += 101; return result;
                },
            } });
            try {
                const task = await runner.run({ flow: 'a', kind: 'result', result: 1 });
                const outcome = await runner[method](task.taskId, () => { calls++; return { status: 'acknowledged' }; }, { timeoutMs: 100 });
                assert.equal(outcome.status, 'unknown'); assert.equal(outcome.error.code, 'EXPIRED'); assert.equal(calls, 0);
            } finally { await runner.close(); }
        }
    } finally { await store.close(); await rm(directory, { recursive: true }); }
});
