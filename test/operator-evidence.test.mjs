import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';

const stores = [['memory', options => createMemoryStore(options)], ['sqlite', options => createSQLiteStore(':memory:', options)],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', options => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL, ...options })]] : [])];

for (const [name, factory] of stores) test(`${name}: operator evidence is bounded before durable delivery mutation`, async () => {
    const store = await factory({ maxBytes: 1024 }); let calls = 0;
    const runner = createRunner({ store, namespace: `operator-evidence-${crypto.randomUUID()}`,
        validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async () => { calls++; return { status: 'unknown' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
        ] } } });
    try {
        const original = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        const deliveryId = original.deliveries[0].id;
        await assert.rejects(runner.resolveDelivery(deliveryId, { status: 'acknowledged', receipt: 'x'.repeat(1024) }), { code: 'RESOURCE_LIMIT' });
        await assert.rejects(runner.resolveDelivery(deliveryId, { status: 'acknowledged', receipt: { invalid: undefined } }), { code: 'VALIDATION_ERROR' });
        assert.deepEqual(await runner.get(original.taskId), original);
        const accepted = await runner.resolveDelivery(deliveryId, { status: 'acknowledged', receipt: false });
        assert.equal(accepted.deliveries[0].receipt.receipt, false);
        await assert.rejects(runner.recordCompletion(deliveryId, { status: 'completed', evidence: 'x'.repeat(1024) }), { code: 'RESOURCE_LIMIT' });
        await assert.rejects(runner.recordCompletion(deliveryId, { status: 'completed', evidence: { invalid: undefined } }), { code: 'VALIDATION_ERROR' });
        assert.deepEqual(await runner.get(original.taskId), accepted);
        const completed = await runner.recordCompletion(deliveryId, { status: 'completed', evidence: null });
        assert.deepEqual(completed.deliveries[0].completion, { status: 'completed', evidence: null });
        assert.equal(calls, 1);
    } finally { await runner.close(); await store.close(); }
});

for (const [name, factory] of stores) test(`${name}: invalid recovery evidence leaves an uncertain evaluation untouched`, async () => {
    const store = await factory({ maxBytes: 256 }); let calls = 0; let validations = 0;
    const runner = createRunner({ store, namespace: `evaluation-evidence-${crypto.randomUUID()}`,
        evaluators: { e: { revision: '1', execute() { calls++; throw new ConnectorError('TIMEOUT', 'Response missing.'); },
            validateSaved(value) { validations++; return value; } } },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e' } } });
    try {
        const original = await runner.run({ flow: 'a', kind: 'request', request: {} });
        const taskId = original.taskId;
        assert.equal(original.status, 'unknown');
        for (const invalid of [{ status: 'acknowledged', reason: 'Delivery accepted' }, { status: 'failed', reason: '  ' },
            { status: 'succeeded', reason: 'Missing response' }]) {
            await assert.rejects(store.resolveEvaluation(runner.namespace, taskId, invalid), { code: 'CONFIGURATION_ERROR' });
        }
        const accessor = { get status() { assert.fail('Resolution getter was invoked'); }, reason: 'Unavailable' };
        await assert.rejects(runner.resolveEvaluation(taskId, accessor), { code: 'VALIDATION_ERROR' });
        await assert.rejects(runner.resolveEvaluation(taskId, { status: 'succeeded', result: null, reason: 'x'.repeat(65536) }),
            { code: 'RESOURCE_LIMIT' });
        await runner.tick({ taskId });
        assert.deepEqual(await runner.get(taskId), original);
        assert.equal(validations, 0); assert.equal(calls, 1);
        const reason = 'Verified provider response. '.repeat(20);
        await runner.resolveEvaluation(taskId, { status: 'succeeded', reason, result: null });
        const recovered = await runner.execute(taskId);
        assert.equal(recovered.status, 'completed'); assert.equal(recovered.evaluation.result, null);
        assert.equal((await store.getJob(runner.namespace, `${taskId}:evaluation`)).resolution.reason, reason);
        await assert.rejects(runner.resolveEvaluation(taskId, { status: 'succeeded', reason: 'Duplicate', result: 1 }), { code: 'CONFLICT' });
        assert.equal(validations, 1); assert.equal(calls, 1);
    } finally { await runner.close(); await store.close(); }
});

test('a legacy requeued evaluation without outcome evidence requires fresh reconciliation', async () => {
    const repository = createMemoryRepository(); const store = createExecutionStore(repository); let calls = 0;
    const runner = createRunner({ store, evaluators: { e: { revision: '1', execute() { calls++; return 'unexpected'; } } },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e' } } });
    try {
        const { taskId } = await runner.submit({ flow: 'a', kind: 'request', request: 1 });
        await repository.transaction(async tx => {
            const job = await tx.get('jobs', `${taskId}:evaluation`);
            await tx.put('jobs', { ...job, stage: 'reconciled', resolution: { status: 'acknowledged', reason: 'Delivery accepted' } });
        });
        const guarded = await runner.execute(taskId);
        assert.equal(guarded.status, 'unknown'); assert.equal(guarded.errors[0].code, 'INVALID_RESOLUTION');
        await runner.resolveEvaluation(taskId, { status: 'failed', reason: 'Provider confirmed rejection' });
        assert.equal((await runner.execute(taskId)).status, 'failed');
        assert.equal(calls, 0);
    } finally { await runner.close(); await store.close(); }
});
