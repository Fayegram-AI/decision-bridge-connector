import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bindInput } from '../src/node/incoming.mjs';
import { bounded, delay } from '../src/util.mjs';

const stores = [['memory', limits => createMemoryStore(limits)], ['sqlite', limits => createSQLiteStore(':memory:', limits)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', limits => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...limits })]] : [])];
for (const [name, factory] of stores) test(`${name}: configured byte limits bound admission, normalization and reconciliation`, async () => {
    const store = await factory({ maxBytes: 1024 }); const namespace = crypto.randomUUID(); const large = 'x'.repeat(2048);
    let normalized = false; let evaluations = 0;
    const runner = createRunner({ store, namespace,
        evaluators: { e: { revision: '1', validate: value => normalized ? large : value,
            execute() { evaluations++; throw new ConnectorError('TIMEOUT', 'Uncertain'); } } },
        validators: { v: { revision: '1', validate: value => normalized ? large : value } },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v' } } });
    try {
        for (const kind of ['request', 'result']) {
            await assert.rejects(runner.submit({ flow: 'a', kind, [kind]: large }), { code: 'RESOURCE_LIMIT' });
            await assert.rejects(runner.preview({ flow: 'a', kind, [kind]: large }), { code: 'RESOURCE_LIMIT' });
            normalized = true;
            await assert.rejects(runner.submit({ flow: 'a', kind, [kind]: 1 }), { code: 'RESOURCE_LIMIT' });
            normalized = false;
        }
        assert.equal((await runner.list()).items.length, 0); assert.equal(evaluations, 0);
        const task = await runner.run({ flow: 'a', kind: 'request', request: 1 });
        await assert.rejects(runner.resolveEvaluation(task.taskId, { status: 'succeeded', reason: 'Recovered', result: large }), { code: 'RESOURCE_LIMIT' });
        normalized = true;
        await assert.rejects(runner.resolveEvaluation(task.taskId, { status: 'succeeded', reason: 'Recovered', result: 1 }), { code: 'RESOURCE_LIMIT' });
        assert.equal((await runner.get(task.taskId)).evaluation.status, 'unknown'); assert.equal(evaluations, 1);
        normalized = false;
        await runner.resolveEvaluation(task.taskId, { status: 'succeeded', reason: 'Recovered', result: 1 });
        assert.equal((await runner.execute(task.taskId)).status, 'completed'); assert.equal(evaluations, 1);
    } finally { await runner.close(); await store.close(); }
});

test('raising the byte budget permits larger input and still enforces the configured boundary', async () => {
    const runner = createRunner({ limits: { maxBytes: 2 * 1024 * 1024 }, validators: { v: { revision: '1', validate: value => value } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    try {
        const result = 'x'.repeat(1100000);
        assert.equal((await runner.run({ flow: 'a', kind: 'result', result })).evaluation.result, result);
        await assert.rejects(runner.submit({ flow: 'a', kind: 'result', result: 'x'.repeat(2100000) }), { code: 'RESOURCE_LIMIT' });
    } finally { await runner.close(); }
});

test('null normalization is preserved for requests, saved results and recovered evidence', async () => {
    let ambiguous = false; const received = [];
    const runner = createRunner({ evaluators: { e: { revision: '1', validate: () => null, execute(value) {
        received.push(value); if (ambiguous) throw new ConnectorError('TIMEOUT', 'Uncertain'); return value;
    } } }, validators: { v: { revision: '1', validate: () => null } },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v' } } });
    try {
        const request = await runner.run({ flow: 'a', kind: 'request', request: 1 });
        assert.equal(request.evaluation.result, null); assert.deepEqual(received, [null]);
        const saved = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        assert.equal(saved.evaluation.result, null);
        ambiguous = true; const lost = await runner.run({ flow: 'a', kind: 'request', request: 1 });
        await runner.resolveEvaluation(lost.taskId, { status: 'succeeded', reason: 'Recovered', result: 1 });
        const resolved = await runner.execute(lost.taskId); assert.equal(resolved.status, 'completed'); assert.equal(resolved.evaluation.result, null);
        assert.equal(received.length, 2);
    } finally { await runner.close(); }
});

function configuration() {
    return { evaluators: { e: { revision: '1', execute: async value => value, validateSaved: value => value } },
        validators: { v: { revision: '1', validate: value => value } }, connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged' })) },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
        ] } } };
}

test('nested retry and dependency settings fail before admission and valid dependency arrays preserve ordering', async () => {
    for (const settings of [{ retry: null }, { retry: [] }, { retry: 'once' }, { retry: { maxAttemps: 1 } },
        { dependsOn: 'out' }, { dependsOn: null }, { dependsOn: [1] }, { dependsOn: [''] }]) {
        const options = configuration(); Object.assign(options.flows.a.connections[0], settings);
        const runner = createRunner(options);
        try {
            await assert.rejects(runner.ready, { code: 'CONFIGURATION_ERROR' });
            await assert.rejects(runner.submit({ flow: 'a', kind: 'result', result: 1 }), { code: 'CONFIGURATION_ERROR' });
            assert.equal((await runner.list()).items.length, 0);
        } finally { await runner.close(); }
    }
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let called = false;
    const options = configuration();
    options.flows.a.connections = ['parentAB', 'dependent'].map(id => ({ id, connector: 'sink', when: 'always', payload: 'raw-result',
        retry: { maxAttempts: 1 }, ...(id === 'dependent' ? { dependsOn: ['parentAB'] } : {}) }));
    options.connectors.sink = createCallbackConnector(async (_value, context) => {
        if (context.deliveryId.endsWith(':parentAB')) { entered.resolve(); await release.promise; }
        else called = true;
        return { status: 'acknowledged' };
    });
    const runner = createRunner(options);
    try {
        const task = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
        await bounded(async signal => {
            let started = false; entered.promise.then(() => { started = true; });
            while (!started) { await runner.tick({ taskId: task.taskId }); await delay(1, signal); }
        }, { timeoutMs: 5000 });
        await runner.tick({ taskId: task.taskId }); assert.equal(called, false);
        release.resolve(); assert.equal((await runner.execute(task.taskId)).status, 'completed'); assert.equal(called, true);
    } finally { release.resolve(); await runner.close(); }
});

test('unusable extensions, mismatched operations and invalid timer/settings contracts fail at startup', async () => {
    const invalid = [
        options => { delete options.evaluators.e.revision; },
        options => { options.validators.v.revision = 0; },
        options => { options.validators.v.validate = 'not callable'; },
        options => { options.connectors.sink = { revision: '1', send: 'not callable' }; },
        options => { options.evaluators.e.operation = 'extraction'; },
        options => { options.flows.a.validator = 'missing'; },
        options => { options.evaluators.e.validateSaved = true; },
        options => { options.policies = { p: { revision: '1', execute: true } }; options.flows.a.policies = { p: { type: 'module', name: 'p' } }; },
        options => { options.transforms = { t: { revision: '1', execute: true } }; options.flows.a.connections[0].transforms = [{ type: 'module', name: 't' }]; },
        options => { options.flows.a.timeoutMs = 2147483648; },
        options => { options.flows.a.connections[0].timeoutMs = 2147483648; },
        options => { options.flows.a.locked = { deadlineAt: 'not a date' }; },
        options => { options.flows.a.defaults = { correlationId: 1 }; },
        options => { options.flows.a.connections[0].outputSchema = false; },
    ];
    for (const change of invalid) {
        const options = configuration(); change(options); const runner = createRunner(options);
        try { await assert.rejects(runner.ready, { code: 'CONFIGURATION_ERROR' }); }
        finally { await runner.close(); }
    }
});

test('rate validation rejects overflowing schedules and preserves valid persisted intervals', async () => {
    for (const scope of ['flow', 'connection']) for (const rate of [Number.MIN_VALUE, 1e-300, 1e-15, 0, -1, Infinity, NaN, '1']) {
        const options = configuration();
        if (scope === 'flow') options.flows.a.rate = rate;
        else options.flows.a.connections[0].rate = rate;
        const runner = createRunner(options);
        try {
            await assert.rejects(runner.ready, { code: typeof rate === 'number' && !Number.isFinite(rate) ? 'VALIDATION_ERROR' : 'CONFIGURATION_ERROR' });
            assert.equal((await runner.list()).items.length, 0);
        } finally { await runner.close(); }
    }
    for (const rate of [0.25, 1e-9, Number.MAX_VALUE]) {
        const options = configuration(); options.flows.a.connections[0].rate = rate;
        const runner = createRunner(options);
        try {
            assert.equal((await runner.run({ flow: 'a', kind: 'result', result: 1 })).status, 'completed');
        } finally { await runner.close(); }
    }
});

test('caller metadata is validated before admission and locked settings retain precedence', async () => {
    const options = configuration(); options.flows.a.overrides = ['deadlineAt', 'correlationId'];
    options.flows.a.locked = { questionRevision: 'locked' };
    const runner = createRunner(options);
    try {
        for (const extra of [{ deadlineAt: 1 }, { deadlineAt: 'bad' }, { correlationId: '' }, { evidenceAt: 0 }])
            await assert.rejects(runner.submit({ flow: 'a', kind: 'result', result: 1, ...extra }), { code: 'CONFIGURATION_ERROR' });
        assert.equal((await runner.list()).items.length, 0);
        const task = await runner.run({ flow: 'a', kind: 'result', result: 1, correlationId: 'case', deadlineAt: '2099-01-01T00:00:00Z' });
        assert.equal(task.status, 'completed');
    } finally { await runner.close(); }
});

test('invalid submission envelopes are rejected before accessing fields and data getters never run', async () => {
    const runner = createRunner(configuration()); let reads = 0;
    try {
        for (const input of [null, 1, [], 'not a submission']) {
            await assert.rejects(runner.submit(input), { code: 'CONFIGURATION_ERROR' });
            assert.throws(() => bindInput(input, { flow: 'a' }), { code: 'CONFIGURATION_ERROR' });
        }
        await assert.rejects(runner.submit({ get flow() { reads++; return 'a'; }, kind: 'result', result: 1 }), { code: 'VALIDATION_ERROR' });
        assert.equal(reads, 0); assert.equal((await runner.list()).items.length, 0);
        assert.equal(bindInput(null, { flow: 'a', inputMode: 'event' }).event, null);
    } finally { await runner.close(); }
});
