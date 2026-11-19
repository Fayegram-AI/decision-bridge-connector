import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createHttpConnector } from '../src/node/connectors.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded, delay } from '../src/util.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];
const configuration = (store, sink) => ({ store, namespace: `receipt-${crypto.randomUUID()}`,
    validators: { v: { revision: '1', validate: value => value } }, connectors: { sink },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
        { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', timeoutMs: 10000 },
    ] } } });
async function until(check) { return bounded(async signal => { while (!await check()) await delay(5, signal); }, { timeoutMs: 10000 }); }

test('runner preserves a native HTTP 2xx while optional response capture stalls', async () => {
    let calls = 0;
    const server = createServer((request, response) => {
        if (request.url === '/warmup') { request.resume(); response.writeHead(200, { 'content-type': 'application/json' }); response.end('{}'); return; }
        calls++; request.resume(); response.writeHead(200, { 'content-type': 'application/json' }); response.flushHeaders(); response.write('{');
    });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    const sink = createHttpConnector({ url: `http://127.0.0.1:${server.address().port}`, captureResponse: true, timeoutMs: 2000 });
    const options = configuration(undefined, sink); options.flows.a.connections[0].timeoutMs = 100;
    const runner = createRunner(options);
    try {
        // Measure interrupted capture after 2xx, independently of lazy HTTP-client startup.
        await (await fetch(`http://127.0.0.1:${server.address().port}/warmup`)).text();
        const result = await runner.run({ flow: 'a', kind: 'result', result: 42 });
        assert.equal(result.status, 'completed'); assert.equal(result.deliveries[0].status, 'acknowledged');
        assert.equal(result.deliveries[0].receipt.receipt.httpStatus, 200);
        assert.equal(result.deliveries[0].receipt.receipt.responseUnavailable, true);
        assert.equal(result.deliveries[0].attempts.length, 1); await runner.execute(result.taskId); assert.equal(calls, 1);
    } finally { await runner.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
});

for (const [name, factory] of stores) {
    for (const mode of ['recover', 'recover-with-response', 'cancel', 'checkpoint-response-lost']) test(`${name}: ${mode} preserves checkpointed acceptance`, async () => {
        const store = await factory(); const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let sends = 0; let callback;
        const sink = createCallbackConnector(async (_payload, context) => {
            sends++; callback = context.onAcceptance;
            const receipt = { status: 'acknowledged', receipt: { httpStatus: 200, ...(mode === 'recover-with-response' ? { response: 42 } : {}) } };
            await callback(receipt); receipt.receipt.httpStatus = 500;
            entered.resolve(); await release.promise; return { status: 'unknown' };
        });
        let inject = mode === 'checkpoint-response-lost';
        const runner = createRunner(configuration({ ...store, async commit(claim, update) {
            await store.commit(claim, update);
            if (inject && update.job?.acceptance) { inject = false; entered.resolve(); throw new Error('Checkpoint response lost'); }
        } }, sink));
        try {
            const task = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
            await until(async () => { await runner.tick({ taskId: task.taskId }); return (await store.get(task.taskId, runner.namespace)).jobs.some(job => job.acceptance); });
            await bounded(() => entered.promise, { timeoutMs: 10000 });
            if (mode.startsWith('recover')) {
                const claim = (await store.get(task.taskId, runner.namespace)).jobs.find(job => job.kind === 'delivery');
                await store.commit(claim, { job: { leaseUntil: 0 } }); await runner.recover();
            } else if (mode === 'cancel') await runner.cancel(task.taskId);
            release.resolve(); await runner.close();
            const report = await runner.get(task.taskId);
            assert.equal(report.deliveries[0].status, 'acknowledged'); assert.equal(report.deliveries[0].receipt.receipt.httpStatus, 200);
            assert.equal(report.deliveries[0].receipt.receipt.responseUnavailable, mode === 'recover-with-response' ? undefined : true); assert.equal(sends, 1);
            if (mode === 'recover-with-response') assert.equal(report.deliveries[0].receipt.receipt.response, 42);
            await assert.rejects(callback({ status: 'acknowledged' }), error => ['STALE_ATTEMPT', 'STALE_LEASE'].includes(error.code));
        } finally { release.resolve(); await runner.close(); await store.close(); }
    });

    test(`${name}: superseded callbacks cannot attach evidence to a replacement owner`, async () => {
        const store = await factory(); const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let callback;
        const runner = createRunner(configuration(store, createCallbackConnector(async (_payload, context) => {
            callback = context.onAcceptance; entered.resolve(); await release.promise; return { status: 'unknown' };
        })));
        try {
            const task = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
            await until(async () => { await runner.tick({ taskId: task.taskId }); return Boolean(callback); }); await entered.promise;
            const delivery = (await store.get(task.taskId, runner.namespace)).jobs.find(job => job.kind === 'delivery');
            await store.commit(delivery, { job: { token: 'replacement-owner' } });
            await assert.rejects(callback({ status: 'acknowledged' }), { code: 'STALE_LEASE' });
            assert.equal((await store.get(task.taskId, runner.namespace)).jobs.find(job => job.kind === 'delivery').acceptance, undefined);
            await runner.cancel(task.taskId); release.resolve(); await runner.execute(task.taskId);
            assert.equal((await runner.get(task.taskId)).deliveries[0].status, 'unknown');
        } finally { release.resolve(); await runner.close(); await store.close(); }
    });

    test(`${name}: unsuccessful receipt writes cannot fabricate durable acceptance`, async () => {
        const store = await factory(); const failed = Promise.withResolvers();
        const options = configuration({ ...store, async commit(claim, update) {
            if (claim.kind === 'delivery' && (update.job?.acceptance || update.job?.state === 'acknowledged')) throw new Error('Storage unavailable');
            return store.commit(claim, update);
        } }, createCallbackConnector(async (_value, context) => {
            await context.onAcceptance({ status: 'acknowledged', receipt: { httpStatus: 200 } }); return { status: 'acknowledged' };
        }));
        options.onEvent = event => { if (event.type === 'worker.error') failed.resolve(); };
        const runner = createRunner(options);
        try {
            const task = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
            await until(async () => { await runner.tick({ taskId: task.taskId }); return (await store.get(task.taskId, runner.namespace)).jobs.some(job => job.stage === 'invoked'); });
            await bounded(() => failed.promise, { timeoutMs: 10000 });
            const claim = (await store.get(task.taskId, runner.namespace)).jobs.find(job => job.kind === 'delivery');
            assert.equal(claim.acceptance, undefined); await store.commit(claim, { job: { leaseUntil: 0 } }); await runner.recover();
            const result = await runner.get(task.taskId); assert.equal(result.deliveries[0].status, 'unknown'); assert.equal(result.deliveries[0].receipt, null);
        } finally { await runner.close(); await store.close(); }
    });

    for (const admitted of [false, true]) test(`${name}: cancellation ${admitted ? 'links an admitted child' : 'fences a delayed child'}`, async () => {
        const store = await factory(); const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let child;
        const evaluated = Promise.withResolvers(); const delivered = Promise.withResolvers();
        const options = configuration({ ...store, async admit(task, ...args) {
            if (task.source !== 'chain') return store.admit(task, ...args);
            if (admitted) child = await store.admit(task, ...args);
            entered.resolve(); await release.promise;
            return child ?? store.admit(task, ...args);
        } }, createCallbackConnector(async () => ({ status: 'acknowledged', receipt: { response: 42 } })));
        options.evaluators = { e: { revision: '1', execute: value => value } };
        options.flows.a.connections[0].nextFlow = 'child';
        options.flows.a.connections[0].chainRetry = { maxAttempts: 3, delayMs: 10, maxDelayMs: 20 };
        options.flows.child = { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } };
        options.onEvent = event => {
            if (event.flow === 'a' && event.type === 'evaluation.settled') evaluated.resolve();
            if (event.flow === 'a' && event.type === 'delivery.settled') delivered.resolve();
        };
        const runner = createRunner(options);
        try {
            const parent = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
            await runner.tick({ taskId: parent.taskId }); await bounded(() => evaluated.promise, { timeoutMs: 10000 });
            await runner.tick({ taskId: parent.taskId }); await bounded(() => delivered.promise, { timeoutMs: 10000 });
            await runner.tick({ taskId: parent.taskId }); await bounded(() => entered.promise, { timeoutMs: 10000 });
            const [one, two] = await Promise.all([runner.cancel(parent.taskId), runner.cancel(parent.taskId)]);
            for (const result of [one, two]) {
                assert.equal(result.chains[0].status, admitted ? 'acknowledged' : 'cancelled');
                assert.equal(result.chains[0].childTaskId, child?.id ?? null);
            }
            release.resolve(); await runner.close();
            assert.equal((await runner.list()).items.length, admitted ? 2 : 1);
            if (admitted) assert.equal((await store.get(child.id, runner.namespace)).task.state, 'active');
        } finally { release.resolve(); await runner.close(); await store.close(); }
    });
}
