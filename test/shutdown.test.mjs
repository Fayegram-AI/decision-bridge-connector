import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createHost } from '../src/node/host.mjs';
import { createService } from '../src/node/service.mjs';

const gate = () => Promise.withResolvers();
const input = { flow: 'a', kind: 'result', result: 1 };
const flow = { revision: '1', inputs: ['result'], validator: 'v' };
const validator = { revision: '1', validate: value => value };

test('runner shutdown drains admission validation and forced shutdown cancels it', { timeout: 10000 }, async () => {
    for (const cancel of [false, true]) {
        const entered = gate(); const release = gate(); const store = createMemoryStore();
        const runner = createRunner({ store, validators: { v: { revision: '1', async validate(value) {
            entered.resolve(); await release.promise; return value;
        } } }, flows: { a: flow } });
        const admitted = runner.submit(input); const observed = admitted.catch(error => error);
        await entered.promise;
        let closed = false; const closing = runner.close({ cancel }).then(() => { closed = true; });
        try {
            await assert.rejects(runner.submit(input), { code: 'RUNNER_CLOSED' });
            if (cancel) {
                assert.equal((await observed).code, 'ABORTED'); await closing;
                assert.equal((await store.list('default')).items.length, 0);
            } else {
                await sleep(10); assert.equal(closed, false);
                release.resolve(); const task = await admitted; await closing;
                assert(await store.get(task.taskId, 'default'));
            }
        } finally { release.resolve(); await closing; await store.close(); }
    }
});

test('shutdown releases an undispatched claim and validates signals before claiming', { timeout: 10000 }, async () => {
    const store = createMemoryStore(); const entered = gate(); const release = gate(); let calls = 0; let claims = 0;
    const options = { validators: { v: validator }, connectors: { sink: createCallbackConnector(async () => { calls++; return { status: 'acknowledged' }; }) },
        flows: { a: { ...flow, connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] } } };
    const runner = createRunner({ ...options, store: { ...store, async claim(...args) {
        claims++; const claim = await store.claim(...args); entered.resolve(); await release.promise; return claim;
    } } });
    let tick; let closing;
    try {
        const task = await runner.submit(input);
        await assert.rejects(runner.tick({ signal: {} }), { code: 'CONFIGURATION_ERROR' });
        const controller = new AbortController(); controller.abort();
        await assert.rejects(runner.run(input, { signal: controller.signal }), { code: 'ABORTED' });
        assert.equal(claims, 0); assert.equal((await runner.list()).items.length, 1);
        tick = runner.tick(); await entered.promise;
        let closed = false; closing = runner.close().then(() => { closed = true; });
        await sleep(10); assert.equal(closed, false);
        release.resolve(); await tick; await closing;
        assert.equal(calls, 0);
        assert.equal((await store.get(task.taskId, 'default')).jobs.find(job => job.kind === 'evaluation').state, 'pending');
        const replacement = createRunner({ ...options, store });
        try { assert.equal((await replacement.execute(task.taskId)).status, 'completed'); assert.equal(calls, 1); }
        finally { await replacement.close(); }
    } finally { release.resolve(); await tick; await closing; await runner.close(); await store.close(); }
});

test('runner shutdown drains caller reply and source acknowledgement persistence', { timeout: 10000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-drain-')); const store = createSQLiteStore(join(directory, 'state.sqlite'));
    const runner = createRunner({ store, validators: { v: validator }, flows: { a: flow } });
    const replyStarted = gate(); const ackStarted = gate(); const release = gate();
    try {
        const task = await runner.run(input);
        await assert.rejects(runner.reply(task.taskId, () => {}, { signal: {} }), { code: 'CONFIGURATION_ERROR' });
        await assert.rejects(runner.acknowledge(task.taskId, () => {}, { timeoutMs: 0 }), { code: 'CONFIGURATION_ERROR' });
        assert.equal((await runner.get(task.taskId)).reply.status, 'not_requested');
        assert.equal((await runner.get(task.taskId)).sourceAcknowledgement.status, 'not_requested');
        const reply = runner.reply(task.taskId, async () => { replyStarted.resolve(); await release.promise; return { status: 'acknowledged' }; });
        const ack = runner.acknowledge(task.taskId, async () => { ackStarted.resolve(); await release.promise; return { status: 'acknowledged' }; });
        await Promise.all([replyStarted.promise, ackStarted.promise]);
        let closed = false; const closing = runner.close().then(() => { closed = true; });
        await sleep(10); assert.equal(closed, false);
        release.resolve(); await Promise.all([reply, ack, closing]);
        const saved = await store.get(task.taskId, 'default');
        assert.equal(saved.task.reply.status, 'acknowledged'); assert.equal(saved.task.sourceAcknowledgement.status, 'acknowledged');
    } finally { release.resolve(); await runner.close(); await store.close(); await rm(directory, { recursive: true }); }
});

test('forced shutdown interrupts policy planning and leaves saved evidence recoverable', { timeout: 10000 }, async () => {
    const store = createMemoryStore({ leaseMs: 200 }); const entered = gate(); const release = gate();
    const options = { store, validators: { v: validator }, policies: { p: { revision: '1', async execute() {
        entered.resolve(); await release.promise; return { status: 'accepted' };
    } } }, flows: { a: { ...flow, policies: { p: { type: 'module', name: 'p' } } } } };
    const runner = createRunner(options); let closing;
    try {
        const task = await runner.submit(input); await runner.tick(); await entered.promise;
        closing = runner.close({ cancel: true });
        assert.equal(await Promise.race([closing.then(() => true), sleep(2000, false, { ref: false })]), true);
        release.resolve(); await sleep(250);
        const replacement = createRunner(options);
        try { await replacement.recover(); assert.equal((await replacement.execute(task.taskId)).status, 'completed'); }
        finally { await replacement.close(); }
    } finally { release.resolve(); await closing; await runner.close(); await store.close(); }
});

test('forced shutdown leaves interrupted child admission recoverable without repeating parent delivery', { timeout: 10000 }, async () => {
    const store = createMemoryStore({ leaseMs: 200 }); const release = gate(); let validating = false; let sends = 0;
    const options = { store, validators: { v: validator }, evaluators: { e: { revision: '1',
        async validate(value) { validating = true; await release.promise; return value; }, execute: async value => value,
    } }, connectors: { sink: createCallbackConnector(async value => { sends++; return { status: 'acknowledged', receipt: { response: value } }; }) },
    flows: { a: { ...flow, connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', nextFlow: 'child', chainRetry: { maxAttempts: 3, delayMs: 10, maxDelayMs: 50 } }] },
        child: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } };
    const runner = createRunner(options);
    try {
        const task = await runner.submit(input);
        for (let i = 0; i < 100 && !validating; i++) { await runner.tick(); await sleep(10); }
        assert.equal(validating, true); await runner.close({ cancel: true });
        release.resolve(); await sleep(250);
        const replacement = createRunner(options);
        try {
            await replacement.recover(); const parent = await replacement.execute(task.taskId);
            assert.equal(parent.chains[0].status, 'acknowledged'); assert.equal(sends, 1);
            assert.equal((await replacement.execute(parent.chains[0].childTaskId)).evaluation.result, 1);
            assert.equal((await replacement.list()).items.length, 2);
        } finally { await replacement.close(); }
    } finally { release.resolve(); await runner.close({ cancel: true }); await store.close(); }
});

test('host shutdown closes iterable sources once and repeated close waits for cleanup', { timeout: 10000 }, async () => {
    const started = gate(); const read = gate(); const release = gate(); let closes = 0;
    const runner = createRunner({ validators: { v: validator }, flows: { a: flow } });
    const host = await createHost({ runner, role: 'ingress', sources: { source: {
        async *open() { started.resolve(); await read.promise; },
        async close() { closes++; read.resolve(); await release.promise; },
    } } });
    await started.promise;
    const first = host.close(); let closed = false; const second = host.close().then(() => { closed = true; });
    try { await sleep(10); assert.equal(closed, false); assert.equal(closes, 1); }
    finally { release.resolve(); await Promise.all([first, second]); }
});

test('host aggregation preserves explicitly null event input', async () => {
    const store = createMemoryStore(); const received = gate();
    const runner = createRunner({ store, validators: { v: validator }, flows: { a: flow } });
    const host = await createHost({ runner, role: 'ingress', aggregators: { example: { async add(value) { received.resolve(value); return {}; } } },
        sources: { source: { aggregation: { rule: 'example', correlationPath: '$' }, async *open() {
            yield { input: { flow: 'a', kind: 'event', event: null, idempotencyKey: 'null' } };
        } } } });
    try { assert.equal((await received.promise).event, null); }
    finally { await host.close(); await store.close(); }
});

test('service shutdown waits for a reload generation and leaves no source open', { timeout: 10000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-service-race-')); const key = `dbc-${crypto.randomUUID()}`;
    const state = { entered: gate(), release: gate(), active: 0 }; globalThis[key] = state;
    await writeFile(join(directory, 'source.mjs'), `const state = globalThis[${JSON.stringify(key)}];
export default { apiVersion: 1, revision: '1', schema: { type: 'object' }, create() {
    let opened = false;
    return { async start() { state.entered.resolve(); await state.release.promise; opened = true; state.active++; },
        async close() { if (opened) { state.active--; opened = false; } } };
} };`);
    const config = { schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } }, host: { role: 'ingress' },
        validators: { v: { type: 'schema', options: { schema: {} } } }, flows: { a: flow } };
    const service = await createService(config, { baseDir: directory }); let reload; let closing;
    try {
        reload = service.reload({ ...config, extensions: [{ kind: 'incoming', name: 'delayed', module: './source.mjs' }], incoming: { source: { type: 'delayed' } } });
        await state.entered.promise;
        let closed = false; closing = service.close().then(() => { closed = true; });
        await sleep(10); assert.equal(closed, false);
        state.release.resolve(); await Promise.all([reload, closing]);
        assert.equal(state.active, 0); await service.close();
        await assert.rejects(service.reload(config), { code: 'CONFLICT' });
    } finally { state.release.resolve(); await Promise.allSettled([reload, closing]); await service.close(); delete globalThis[key]; await rm(directory, { recursive: true }); }
});
