import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { prepare } from '../src/engine/admission.mjs';
import { compileFlows } from '../src/engine/flows.mjs';
import { setTimeout as sleep } from 'node:timers/promises';

const result = probability => ({ answers: { route: { type: 'choice', choice: 'yes', probabilities: { yes: probability, no: 1 - probability } } },
    meta: { providerKind: 'decision', probabilitySource: 'native' } });
const policy = minProbability => ({ revision: '1', accept: { question: 'route', type: 'choice', minProbability }, otherwise: 'review' });
function fixture(extra = {}) {
    const calls = []; let evaluations = 0;
    const connector = createCallbackConnector(async payload => { calls.push(payload); return { status: 'acknowledged' }; });
    const options = { evaluators: { test: { revision: '1', execute: async () => { evaluations++; return result(0.8); } } },
        validators: { test: { revision: '1', validate(value) { assert(value.answers); return value; } } },
        connectors: { sink: connector }, flows: { example: { revision: '1', inputs: ['request', 'result'], evaluator: 'test', validator: 'test',
            policies: { advisory: policy(0.7), action: policy(0.95) }, connections: [
                { id: 'raw', connector: 'sink', when: 'evaluation.succeeded', payload: 'raw-result' },
                { id: 'advice', connector: 'sink', policy: 'advisory', when: 'decision.accepted', payload: 'final-decision' },
                { id: 'act', connector: 'sink', policy: 'action', when: 'decision.accepted', payload: 'final-decision' },
                { id: 'review', connector: 'sink', policy: 'action', when: 'decision.review', payload: 'final-decision-with-result' },
            ] } }, ...extra };
    return { options, calls, evaluations: () => evaluations };
}

for (const [name, factory] of [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])]) {
    test(`${name}: result admission, independent policies, deduplication and isolated flows`, async () => {
        const store = await factory(); const setup = fixture({ store, namespace: `engine-${crypto.randomUUID()}` });
        setup.options.flows.other = { revision: '1', inputs: ['result'], validator: 'test', connections: [{ id: 'other', connector: 'sink', when: 'always', payload: 'raw-result' }] };
        const runner = createRunner(setup.options);
        try {
            const input = { flow: 'example', kind: 'result', result: result(0.8), idempotencyKey: 'one' };
            const first = await runner.run(input);
            assert.equal(first.status, 'completed'); assert.equal(setup.evaluations(), 0); assert.equal(setup.calls.length, 3);
            assert.equal(first.policies.advisory.decision.status, 'accepted'); assert.equal(first.policies.action.decision.status, 'review');
            assert.equal(first.deliveries.find(x => x.connectionId === 'act').status, 'skipped');
            const duplicate = await runner.run(input); assert.equal(first.taskId, duplicate.taskId); assert.equal(setup.calls.length, 3);
            await assert.rejects(runner.submit({ ...input, result: result(0.9) }), { code: 'IDEMPOTENCY_CONFLICT' });
            await assert.rejects(runner.submit({ ...input, evaluator: 'other' }), { code: 'CONFIGURATION_ERROR' });
        } finally { await runner.close(); await store.close(); }
    });
    test(`${name}: two workers share admission and delivery ownership`, async () => {
        const store = await factory(); const setup = fixture({ store, namespace: `workers-${crypto.randomUUID()}` });
        const a = createRunner(setup.options); const b = createRunner(setup.options);
        try {
            const input = { flow: 'example', kind: 'request', request: {}, idempotencyKey: 'work' };
            const [first, second] = await Promise.all([a.run(input), b.run(input)]);
            assert.equal(first.taskId, second.taskId); assert.equal(setup.evaluations(), 1); assert.equal(setup.calls.length, 3);
        } finally { await a.close(); await b.close(); await store.close(); }
    });
}

test('optional slow delivery does not delay required completion or consume its timeout', async () => {
    const setup = fixture();
    setup.options.connectors.slow = createCallbackConnector(() => new Promise(() => {}));
    setup.options.flows.example.connections.unshift({ id: 'slow', connector: 'slow', when: 'always', payload: 'raw-result', required: false, timeoutMs: 10000 });
    const runner = createRunner(setup.options);
    try {
        const report = await runner.run({ flow: 'example', kind: 'request', request: {} });
        assert.equal(report.status, 'completed'); assert.equal(report.deliveries.find(x => x.connectionId === 'slow').status, 'active'); assert.equal(setup.calls.length, 3);
    } finally { await runner.close({ cancel: true }); }
});

test('expired lease fences stale workers and preserves ambiguous inference', async () => {
    const store = createMemoryStore({ leaseMs: 20 }); const setup = fixture({ store });
    const prepared = await prepare({ flow: 'example', kind: 'request', request: {} },
        { flows: await compileFlows(setup.options), options: setup.options, source: 'test', namespace: 'test' });
    await store.admit(prepared.task, prepared.jobs, prepared.dedupKey);
    const claim = await store.claim('test', 'evaluation', { worker: 'lost' });
    await store.commit(claim.job, { job: { stage: 'invoked' } });
    await sleep(30); await store.recover('test');
    await assert.rejects(store.commit(claim.job, { task: { result: result(1) } }), { code: 'STALE_LEASE' });
    assert.equal((await store.get(prepared.task.id, 'test')).jobs.find(x => x.kind === 'evaluation').state, 'unknown');
    await store.close();
});

test('saved result recovery skips inference after a worker crash', async () => {
    const store = createMemoryStore({ leaseMs: 1000 }); const setup = fixture({ store });
    const runner = createRunner(setup.options);
    try {
        const admitted = await runner.submit({ flow: 'example', kind: 'request', request: {} });
        const claim = await store.claim('default', 'evaluation', { worker: 'lost' });
        await store.commit(claim.job, { task: { result: result(0.8) }, job: { stage: 'result_saved' } });
        await sleep(1050); await runner.recover();
        const report = await runner.execute(admitted.taskId);
        assert.equal(report.status, 'completed'); assert.equal(setup.evaluations(), 0);
    } finally { await runner.close(); await store.close(); }
});
