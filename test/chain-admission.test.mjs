import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded, delay } from '../src/util.mjs';

const stores = [['memory', options => createMemoryStore(options)], ['sqlite', options => createSQLiteStore(':memory:', options)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', options => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...options })]] : [])];

function configuration(store, chainRetry, limits = {}) {
    return { namespace: `admission-${crypto.randomUUID()}`, store,
        validators: { v: { revision: '1', validate: value => value } },
        evaluators: { e: { revision: '1', execute: async value => value } },
        connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged', receipt: { response: 42 } })) },
        flows: { parent: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'next', connector: 'sink', when: 'always', payload: 'raw-result', nextFlow: 'child', chainRetry },
        ] }, child: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' }, ...limits },
        other: { revision: '1', inputs: ['result'], validator: 'v' } } };
}

async function until(check) {
    return bounded(async signal => { while (!await check()) await delay(5, signal); }, { timeoutMs: 10000 });
}

for (const [name, factory] of stores) {
    test(`${name}: capacity recovery admits one child without resending the parent`, async () => {
        const store = await factory({ maxPending: 2 }); let sends = 0;
        const options = configuration(store, { timeoutMs: 10000, delayMs: 20, maxDelayMs: 80 });
        options.connectors.sink = createCallbackConnector(async () => { sends++; return { status: 'acknowledged', receipt: { response: 42 } }; });
        const runner = createRunner(options);
        try {
            const parent = await runner.submit({ flow: 'parent', kind: 'result', result: 1 });
            const blocker = await runner.submit({ flow: 'other', kind: 'result', result: 2 });
            await until(async () => { await runner.tick({ taskId: parent.taskId }); return (await runner.get(parent.taskId)).chains[0].status === 'retry_pending'; });
            const waiting = await runner.get(parent.taskId);
            assert.equal(waiting.requiredSettled, false); assert(waiting.chains[0].admissionAttempts >= 1);
            assert(waiting.chains[0].nextAttemptAt > waiting.createdAt); assert(waiting.chains[0].deadlineAt);
            await runner.execute(blocker.taskId);
            const completed = await runner.execute(parent.taskId);
            assert.equal(completed.status, 'completed'); assert.equal(sends, 1);
            const child = await runner.get(completed.chains[0].childTaskId);
            assert.equal(child.lineage.rootId, parent.taskId); assert.deepEqual(child.lineage.path, [parent.taskId]);
            await runner.execute(parent.taskId); assert.equal((await runner.list()).items.length, 3);
        } finally { await runner.close(); await store.close(); }
    });

    for (const [label, policy, status, code] of [
        ['attempt', { maxAttempts: 2 }, 'failed', 'CHAIN_ADMISSION_EXHAUSTED'],
        ['time', { timeoutMs: 150 }, 'expired', 'CHAIN_ADMISSION_EXPIRED'],
        ['combined', { maxAttempts: 1, timeoutMs: 10000 }, 'failed', 'CHAIN_ADMISSION_EXHAUSTED'],
    ]) test(`${name}: ${label} admission budget settles capacity pressure`, async () => {
        const store = await factory({ maxPending: 1 });
        const workerErrors = [];
        const runner = createRunner({ ...configuration(store, { ...policy, delayMs: 10, maxDelayMs: 20 }),
            onEvent(event) { if (event.type === 'worker.error') workerErrors.push(event.error); } });
        try {
            const result = await runner.run({ flow: 'parent', kind: 'result', result: 1 });
            assert.equal(result.chains[0].status, status, JSON.stringify({ chain: result.chains[0], workerErrors }));
            assert.equal(result.chains[0].error.code, code);
            if (policy.maxAttempts) assert.equal(result.chains[0].admissionAttempts, policy.maxAttempts);
            assert.equal((await runner.list()).items.length, 1);
        } finally { await runner.close(); await store.close(); }
    });

    for (const limit of [{ maxDepth: 1 }, { maxExecutions: 1 }]) test(`${name}: permanent ${Object.keys(limit)[0]} limit is not retried`, async () => {
        const store = await factory(); const runner = createRunner(configuration(store, { maxAttempts: 3, delayMs: 10, maxDelayMs: 20 }, limit));
        try {
            const result = await runner.run({ flow: 'parent', kind: 'result', result: 1 });
            assert.equal(result.chains[0].status, 'failed'); assert.equal(result.chains[0].admissionAttempts, 1);
            assert.equal(result.chains[0].error.code, 'RESOURCE_LIMIT');
        } finally { await runner.close(); await store.close(); }
    });

    test(`${name}: superseded child admissions are fenced before insertion`, async () => {
        const store = await factory(); const captured = Promise.withResolvers(); const release = Promise.withResolvers(); let rejected;
        const runner = createRunner(configuration({ ...store, async admit(task, ...args) {
            if (task.source === 'chain') {
                captured.resolve(task); await release.promise;
                try { return await store.admit(task, ...args); } catch (error) { rejected = error; throw error; }
            }
            return store.admit(task, ...args);
        } }, { maxAttempts: 3, delayMs: 10, maxDelayMs: 20 }));
        try {
            const parent = await runner.submit({ flow: 'parent', kind: 'result', result: 1 });
            await until(async () => { await runner.tick({ taskId: parent.taskId }); return (await runner.get(parent.taskId)).deliveries[0].status === 'acknowledged'; });
            await runner.tick({ taskId: parent.taskId }); await bounded(() => captured.promise, { timeoutMs: 10000 });
            const job = (await store.get(parent.taskId, runner.namespace)).jobs.find(item => item.kind === 'chain');
            await store.commit(job, { job: { leaseUntil: 0 } }); await runner.recover();
            release.resolve(); await until(async () => Boolean(rejected)); assert.equal(rejected.code, 'STALE_LEASE');
            assert.equal((await runner.list()).items.length, 1);
            assert.equal((await runner.execute(parent.taskId)).status, 'completed');
            assert.equal((await runner.list()).items.length, 2);
        } finally { release.resolve(); await runner.close(); await store.close(); }
    });

    test(`${name}: admission retry survives restart and competing workers`, async () => {
        const store = await factory({ maxPending: 2 });
        const options = configuration(store, { maxAttempts: 20, delayMs: 30, maxDelayMs: 50 });
        const original = createRunner(options); let resumed; let competitor;
        try {
            const parent = await original.submit({ flow: 'parent', kind: 'result', result: 1 });
            const blocker = await original.submit({ flow: 'other', kind: 'result', result: 1 });
            await until(async () => { await original.tick({ taskId: parent.taskId }); return (await original.get(parent.taskId)).chains[0].status === 'retry_pending'; });
            const attempts = (await original.get(parent.taskId)).chains[0].admissionAttempts;
            await original.close(); resumed = createRunner(options); competitor = createRunner(options);
            await resumed.execute(blocker.taskId);
            await until(async () => {
                await Promise.all([resumed.tick({ taskId: parent.taskId }), competitor.tick({ taskId: parent.taskId })]);
                return (await resumed.get(parent.taskId)).chains[0].status === 'acknowledged';
            });
            assert.equal((await resumed.list()).items.filter(item => item.flow === 'child').length, 1);
            assert((await resumed.get(parent.taskId)).chains[0].admissionAttempts > attempts);
        } finally { await original.close(); await resumed?.close(); await competitor?.close(); await store.close(); }
    });

    test(`${name}: delayed insertion cannot create a child after its admission deadline`, async () => {
        const store = await factory(); const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let rejected;
        const runner = createRunner(configuration({ ...store, async admit(task, ...args) {
            if (task.source !== 'chain') return store.admit(task, ...args);
            entered.resolve(); await release.promise;
            try { return await store.admit(task, ...args); } catch (error) { rejected = error; throw error; }
        } }, { timeoutMs: 1000, delayMs: 10, maxDelayMs: 20 }));
        try {
            const parent = await runner.submit({ flow: 'parent', kind: 'result', result: 1 });
            await until(async () => { await runner.tick({ taskId: parent.taskId }); return (await runner.get(parent.taskId)).deliveries[0].status === 'acknowledged'; });
            await runner.tick({ taskId: parent.taskId }); await bounded(() => entered.promise, { timeoutMs: 10000 });
            const result = await runner.execute(parent.taskId);
            assert.equal(result.chains[0].status, 'expired');
            release.resolve(); await until(async () => Boolean(rejected));
            assert.equal(rejected.code, 'ADMISSION_OWNER_SETTLED'); assert.equal((await runner.list()).items.length, 1);
        } finally { release.resolve(); await runner.close(); await store.close(); }
    });
}

test('chain policy and store support fail startup explicitly', async () => {
    const store = createMemoryStore();
    try {
        for (const policy of [undefined, {}, { delayMs: 1, maxDelayMs: 1 }, { maxAttempts: 1, delayMs: 2, maxDelayMs: 1 },
            { maxAttempts: 0, delayMs: 1, maxDelayMs: 1 }, { timeoutMs: 2147483648, delayMs: 1, maxDelayMs: 1 },
            { maxAttempts: 1, delayMs: 1, maxDelayMs: 1, typo: true }]) {
            const options = configuration(store, policy);
            if (policy === undefined) delete options.flows.parent.connections[0].chainRetry;
            const runner = createRunner(options);
            await assert.rejects(runner.ready, { code: 'CONFIGURATION_ERROR' }); await runner.close();
        }
        const runner = createRunner(configuration({ ...store, findChildAdmission: undefined }, { maxAttempts: 1, delayMs: 1, maxDelayMs: 1 }));
        await assert.rejects(runner.ready, /findChildAdmission/); await runner.close();
    } finally { await store.close(); }
});
