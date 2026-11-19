import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { invoke } from '../src/node/http-service.mjs';

const validator = { revision: '1', validate: value => value };
const output = (id, connector, extra = {}) => ({ id, connector, when: 'evaluation.succeeded', payload: 'raw-result', ...extra });
const input = { flow: 'a', kind: 'result', result: 1 };

test('coded delivery failures and oversized receipts remain unknown without definite rejection', async () => {
    for (const mode of ['coded', 'receipt']) {
        let calls = 0;
        const runner = createRunner({ limits: { maxBytes: 1024 }, validators: { v: validator }, connectors: {
            sink: createCallbackConnector(async () => {
                calls++;
                if (mode === 'coded') throw Object.assign(new Error('Lost response'), { code: 'ECONNRESET' });
                return { status: 'acknowledged', receipt: 'x'.repeat(2048) };
            }),
        }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [output('out', 'sink')] } } });
        try {
            const result = await runner.run(input);
            assert.equal(result.deliveries[0].status, 'unknown'); assert.equal(calls, 1);
            await runner.tick(); assert.equal(calls, 1);
        } finally { await runner.close(); }
    }
});

test('public submission cannot bypass outputs or inject derived execution identity', async () => {
    let calls = 0;
    const runner = createRunner({ validators: { v: validator }, connectors: {
        sink: createCallbackConnector(async () => { calls++; return { status: 'acknowledged' }; }),
    }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [output('out', 'sink')] } } });
    try {
        for (const options of [{ policyOnly: true }, { evaluationId: 'forged' }, { lineage: { depth: 0, rootId: 'forged', path: [] } }])
            await assert.rejects(invoke(runner, 'submit', { input, options }), { code: 'CONFIGURATION_ERROR' });
        assert.equal((await runner.list()).items.length, 0);
        const original = await runner.run(input);
        await assert.rejects(runner.reprocess(original.taskId, { deliver: true }), { code: 'CONFIGURATION_ERROR' });
        const reprocessed = await runner.reprocess(original.taskId); await runner.execute(reprocessed.taskId);
        assert.equal(calls, 1);
        const delivered = await runner.redeliver(original.taskId, { flow: 'a' }); await runner.execute(delivered.taskId);
        assert.equal(calls, 2);
    } finally { await runner.close(); }
});

test('required failure notifications finish before an uncertain evaluation returns', async () => {
    let alerts = 0;
    const runner = createRunner({ evaluators: { e: { revision: '1', execute() { throw new ConnectorError('TIMEOUT', 'Uncertain'); } } },
        connectors: { sink: createCallbackConnector(async () => { await sleep(30); alerts++; return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e', connections: [output('normal', 'sink'),
            output('alert', 'sink', { when: 'failure', payload: 'failure' })] } } });
    try {
        const result = await runner.run({ flow: 'a', kind: 'request', request: 1 });
        assert.equal(result.status, 'unknown'); assert.equal(result.requiredSettled, true); assert.equal(alerts, 1);
        assert.equal(result.deliveries.find(job => job.connectionId === 'alert').status, 'acknowledged');
        assert.equal(result.deliveries.find(job => job.connectionId === 'normal').status, 'blocked');
    } finally { await runner.close(); }
});

// Exercise the same crash transitions through each domain store implementation.
const stores = [['memory', options => createMemoryStore(options)], ['sqlite', options => createSQLiteStore(':memory:', options)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', options => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...options })]] : [])];
for (const [name, factory] of stores) test(`${name}: recovery honors retry limits and schedules failure notifications once`, async () => {
    const store = await factory({ leaseMs: 30000 });
    let sends = 0; let alerts = 0; let evaluations = 0;
    const runner = createRunner({ store, namespace: `recovery-${crypto.randomUUID()}`, validators: { v: validator },
        evaluators: { e: { revision: '1', execute: async () => { evaluations++; return 1; } } }, connectors: {
            sink: createCallbackConnector(async () => { sends++; return { status: 'acknowledged' }; }, { idempotent: true }),
            alert: createCallbackConnector(async () => { alerts++; return { status: 'acknowledged' }; }),
        }, flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v', connections: [
            output('out', 'sink', { retry: { ambiguous: 'retry', maxAttempts: 2 } }),
            output('alert', 'alert', { when: 'failure', payload: 'failure' }),
        ] } } });
    try {
        const evaluated = await runner.submit({ flow: 'a', kind: 'request', request: 1 });
        const evaluation = await store.claim(runner.namespace, 'evaluation', { worker: 'lost', taskId: evaluated.taskId });
        await store.commit(evaluation.job, { job: { stage: 'invoked', leaseUntil: 0 } });
        const attempts = [];
        for (const count of [1, 2]) {
            const admitted = await runner.submit(input);
            const planned = await store.claim(runner.namespace, 'evaluation', { worker: 'setup', taskId: admitted.taskId });
            await store.commit(planned.job, { task: { plannedAt: Date.now() }, job: { state: 'acknowledged' }, jobs: [
                { id: `${admitted.taskId}:delivery:out`, state: 'pending', payload: 1 },
                { id: `${admitted.taskId}:delivery:alert`, state: 'skipped' },
            ] });
            const delivery = await store.claim(runner.namespace, 'delivery', { worker: 'lost', taskId: admitted.taskId });
            await store.commit(delivery.job, { job: { stage: 'invoked', leaseUntil: 0, attempts: Array.from({ length: count }, (_, i) => ({
                id: `attempt-${i}`, startedAt: Date.now(), status: i === count - 1 ? 'in_flight' : 'unknown',
            })) } });
            attempts.push(admitted);
        }
        // Explicitly expired invocation markers model a crashed worker without setup-time races.
        await runner.recover();
        assert.equal((await runner.get(evaluated.taskId)).requiredSettled, false);
        assert.equal((await runner.get(attempts[0].taskId)).deliveries.find(job => job.connectionId === 'out').status, 'retry_pending');
        assert.equal((await runner.get(attempts[1].taskId)).deliveries.find(job => job.connectionId === 'out').status, 'unknown');
        await assert.rejects(store.commit(evaluation.job, {}), { code: 'STALE_LEASE' });
        const recovered = await runner.execute(evaluated.taskId);
        assert.equal(recovered.status, 'unknown'); assert.equal(evaluations, 0);
        assert.equal((await runner.execute(attempts[0].taskId)).status, 'completed');
        assert.equal((await runner.execute(attempts[1].taskId)).status, 'unknown');
        await runner.recover(); await runner.tick();
        assert.equal(sends, 1); assert.equal(alerts, 2);
        assert.equal((await runner.get(attempts[1].taskId)).deliveries.find(job => job.connectionId === 'out').attempts.length, 2);
    } finally { await runner.close(); await store.close(); }
});

test('reconciliation refuses a changed explicit validator even with the original evaluator', async () => {
    const store = createMemoryStore(); let validations = 0;
    const options = { store, evaluators: { e: { revision: '1', execute() { throw new ConnectorError('TIMEOUT', 'Uncertain'); }, validateSaved: value => value } },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v' } } };
    const original = createRunner({ ...options, validators: { v: validator } });
    const changed = createRunner({ ...options, validators: { v: { revision: '2', validate(value) { validations++; return value; } } } });
    try {
        const task = await original.run({ flow: 'a', kind: 'request', request: 1 });
        await assert.rejects(changed.resolveEvaluation(task.taskId, { status: 'succeeded', result: 1, reason: 'Recovered receipt' }), { code: 'REVISION_UNAVAILABLE' });
        assert.equal(validations, 0);
        await original.resolveEvaluation(task.taskId, { status: 'succeeded', result: 1, reason: 'Recovered receipt' });
        assert.equal((await original.execute(task.taskId)).status, 'completed');
    } finally { await original.close(); await changed.close(); await store.close(); }
});

test('reply and source acknowledgement preserve uncertain and failed receipt statuses', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-receipts-')); const store = createSQLiteStore(join(directory, 'state.sqlite'));
    const runner = createRunner({ store, validators: { v: validator }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    try {
        const task = await runner.run(input);
        for (const status of ['unknown', 'failed']) {
            assert.equal((await runner.reply(task.taskId, () => ({ status }))).status, status);
            assert.equal((await runner.acknowledge(task.taskId, () => ({ status }))).status, status);
        }
        assert.equal((await runner.reply(task.taskId, () => ({ status: 'invalid' }))).status, 'unknown');
        assert.equal((await runner.acknowledge(task.taskId, () => ({ status: 'submitted' }))).status, 'unknown');
        assert.equal((await runner.acknowledge(task.taskId, () => 1)).status, 'acknowledged');
        assert.equal((await runner.get(task.taskId)).status, 'completed');
    } finally { await runner.close(); await store.close(); await rm(directory, { recursive: true }); }
});

test('reevaluation accepts saved false, zero and null requests', async () => {
    const received = [];
    const runner = createRunner({ evaluators: { e: { revision: '1', execute: async value => { received.push(value); return value; } } },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e' } } });
    try {
        for (const request of [false, 0, null]) {
            const original = await runner.run({ flow: 'a', kind: 'request', request });
            const repeated = await runner.reevaluate(original.taskId);
            assert.equal((await runner.execute(repeated.taskId)).status, 'completed');
        }
        assert.deepEqual(received, [false, false, 0, 0, null, null]);
    } finally { await runner.close(); }
});
