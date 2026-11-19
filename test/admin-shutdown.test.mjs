import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner, collectExport } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

const configuration = store => ({ ...(store ? { store } : {}), namespace: `admin-drain-${crypto.randomUUID()}`,
    evaluators: { e: { revision: '1', execute: async value => value } },
    validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v' } } });
const unknownEvaluator = { revision: '1', async execute() { throw new ConnectorError('LOST', 'Unknown outcome.', { ambiguous: true }); } };
const resolution = { status: 'succeeded', reason: 'Recovered evidence.', result: 2 };
const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) {
    test(`${name}: shutdown drains administrative writes and their report readback`, { timeout: 60000 }, async () => {
        const store = await factory();
        try {
            for (const action of ['retryDelivery', 'resolveDelivery', 'recordCompletion', 'cancel', 'pause', 'recover', 'maintain']) {
                const method = ['retryDelivery', 'resolveDelivery', 'recordCompletion'].includes(action) ? 'mutateJob' : action;
                const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let enabled = false; let operation; let closing;
                const options = configuration({ ...store, async [method](...args) {
                    const result = await store[method](...args);
                    if (enabled) { entered.resolve(); await release.promise; }
                    return result;
                } });
                const delivery = method === 'mutateJob';
                if (delivery) {
                    options.connectors = { sink: createCallbackConnector(async () => {
                        if (action === 'recordCompletion') return { status: 'acknowledged' };
                        throw new ConnectorError('REJECTED', 'Rejected.', { ambiguous: false, retryable: false });
                    }) };
                    options.flows.a.connections = [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }];
                }
                const runner = createRunner(options);
                try {
                    const task = delivery ? await runner.run({ flow: 'a', kind: 'result', result: 1 })
                        : action === 'cancel' ? await runner.submit({ flow: 'a', kind: 'result', result: 1 }) : null;
                    enabled = true;
                    const args = action === 'pause' ? ['flow', 'a'] : action === 'cancel' ? [task.taskId]
                        : action === 'resolveDelivery' ? [task.deliveries[0].id, { status: 'acknowledged' }]
                        : action === 'recordCompletion' ? [task.deliveries[0].id, { status: 'completed' }]
                        : delivery ? [task.deliveries[0].id] : [];
                    operation = runner[action](...args); operation.catch(() => {}); await entered.promise;
                    let closed = false; closing = runner.close().then(() => { closed = true; });
                    await sleep(10); assert.equal(closed, false, action);
                    release.resolve(); const result = await operation; await closing;
                    if (task) assert.equal(result.taskId, task.taskId);
                } finally { release.resolve(); await Promise.allSettled([operation, closing]); await runner.close(); }
            }
        } finally { await store.close(); }
    });

    test(`${name}: reconciliation validation drains or cancels before saved state changes`, { timeout: 30000 }, async () => {
        const store = await factory();
        try {
            for (const cancel of [false, true]) {
                const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let validationSignal; let operation; let closing;
                const runner = createRunner({ ...configuration(store), evaluators: { e: unknownEvaluator },
                    validators: { v: { revision: '1', async validate(value, { signal }) {
                        validationSignal = signal; entered.resolve(); await release.promise; return value;
                    } } } });
                try {
                    const task = await runner.run({ flow: 'a', kind: 'request', request: 1 }); assert.equal(task.status, 'unknown');
                    operation = runner.resolveEvaluation(task.taskId, resolution); operation.catch(() => {}); await entered.promise;
                    let closed = false; closing = runner.close({ cancel }).then(() => { closed = true; });
                    if (cancel) {
                        await assert.rejects(operation, { code: 'ABORTED' });
                        await bounded(() => closing, { timeoutMs: 2000 }); assert.equal(validationSignal.aborted, true);
                    } else {
                        await sleep(10); assert.equal(closed, false); release.resolve();
                        assert.equal((await operation).evaluation.result, 2); await closing;
                    }
                    const saved = await store.get(task.taskId, runner.namespace);
                    assert.equal(saved.task.result, cancel ? undefined : 2);
                    assert.equal(saved.jobs.find(job => job.kind === 'evaluation').state, cancel ? 'unknown' : 'pending');
                } finally { release.resolve(); await Promise.allSettled([operation, closing]); await runner.close(); }
            }
        } finally { await store.close(); }
    });
}

test('owned storage remains open through accepted evaluation reconciliation', async () => {
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let operation; let closing;
    const runner = createRunner({ ...configuration(), evaluators: { e: unknownEvaluator },
        validators: { v: { revision: '1', async validate(value) { entered.resolve(); await release.promise; return value; } } } });
    try {
        const task = await runner.run({ flow: 'a', kind: 'request', request: 1 });
        operation = runner.resolveEvaluation(task.taskId, resolution); operation.catch(() => {}); await entered.promise;
        let closed = false; closing = runner.close().then(() => { closed = true; });
        await sleep(10); assert.equal(closed, false); release.resolve();
        assert.equal((await operation).evaluation.result, 2); await closing;
    } finally { release.resolve(); await Promise.allSettled([operation, closing]); await runner.close(); }
});

test('derived and import preflight accepted before close can finish admission or cancel', async () => {
    const store = createMemoryStore();
    try {
        for (const action of ['reprocess', 'redeliver', 'reevaluate', 'importTask']) for (const cancel of [false, true]) {
            const runner = createRunner(configuration(store)); let operation; let closing;
            try {
                const original = await runner.run({ flow: 'a', kind: 'request', request: 1 }); const saved = await collectExport(runner, original.taskId);
                operation = runner[action](action === 'importTask' ? saved : original.taskId, { flow: 'a' }); operation.catch(() => {});
                closing = runner.close({ cancel });
                if (cancel) await assert.rejects(operation, { code: 'ABORTED' });
                else assert.notEqual((await operation).taskId, original.taskId);
                await closing; assert.equal((await store.list(runner.namespace)).items.length, cancel ? 1 : 2);
            } finally { await Promise.allSettled([operation, closing]); await runner.close(); }
        }
    } finally { await store.close(); }
});

test('preview validation and policy callbacks participate in graceful and forced shutdown', async () => {
    for (const phase of ['validation', 'policy']) for (const cancel of [false, true]) {
        const entered = Promise.withResolvers(); const release = Promise.withResolvers(); const store = createMemoryStore(); let operation; let closing;
        const wait = async value => { entered.resolve(); await release.promise; return value; };
        const options = configuration(store);
        if (phase === 'validation') options.validators.v.validate = wait;
        else { options.policies = { p: { revision: '1', execute: () => wait({ status: 'accepted' }) } };
            options.flows.a.policies = { p: { type: 'module', name: 'p' } }; }
        const runner = createRunner(options);
        try {
            operation = runner.preview({ flow: 'a', kind: 'result', result: 1 }); operation.catch(() => {}); await entered.promise;
            let closed = false; closing = runner.close({ cancel }).then(() => { closed = true; });
            if (cancel) { await assert.rejects(operation, { code: 'ABORTED' }); await bounded(() => closing, { timeoutMs: 2000 }); }
            else { await sleep(10); assert.equal(closed, false); release.resolve(); assert.equal((await operation).inference, false); await closing; }
            assert.deepEqual((await store.list(runner.namespace)).items, []);
        } finally { release.resolve(); await Promise.allSettled([operation, closing]); await runner.close(); await store.close(); }
    }
});

test('closed runners reject new administrative and preview work without using their borrowed store', async () => {
    const store = createMemoryStore(); const runner = createRunner(configuration(store));
    try {
        await runner.close();
        for (const [method, args] of [['retryDelivery', ['delivery']], ['resolveDelivery', ['delivery', {}]], ['recordCompletion', ['delivery', {}]],
            ['resolveEvaluation', ['task', resolution]], ['cancel', ['task']], ['pause', ['flow', 'a']], ['recover', []], ['maintain', []],
            ['preview', [{ flow: 'a', kind: 'result', result: 1 }]], ['reprocess', ['task']], ['redeliver', ['task', { flow: 'a' }]],
            ['reevaluate', ['task']], ['importTask', [{}, { flow: 'a' }]]]) await assert.rejects(runner[method](...args), { code: 'RUNNER_CLOSED' }, method);
        assert.deepEqual((await store.list(runner.namespace)).items, []);
    } finally { await store.close(); }
});
