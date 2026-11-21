import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

test('saved evaluation uses a normalized null request on replay, and caller context only when there was no evaluation', async () => {
    const received = [];
    const evaluator = { revision: '1', validate: () => null, execute: () => 42,
        validateSaved(value, request) { received.push(request); return value; } };
    const runner = createRunner({ evaluators: { e: evaluator }, flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e' } } });
    try {
        const validation = { request: 'caller context' };
        const original = await runner.run({ flow: 'a', kind: 'request', request: 'before normalization', validation });
        await runner.reprocess(original.taskId);
        await runner.redeliver(original.taskId, { flow: 'a' });
        await runner.importTask(runner.exportTask(original.taskId), { flow: 'a' });
        await runner.submit({ flow: 'a', kind: 'result', result: 42, validation });
        await runner.submit({ flow: 'a', kind: 'result', result: 42, validation: null });
        await runner.submit({ flow: 'a', kind: 'result', result: 42, validation: { request: null } });
        assert.deepEqual(received, [null, null, null, 'caller context', undefined, null]);
    } finally { await runner.close(); }
});

test('saved-result callbacks can edit a private request copy without changing retained evidence', async () => {
    const request = { profile: { model: 'original-selector' } };
    const normalize = (result, saved) => { saved.profile.model = 'canonical'; return result; };
    const runner = createRunner({ evaluators: { e: { revision: '1', execute: () => 42, validateSaved: normalize } },
        validators: { v: { revision: '1', validate: (value, { validation }) => normalize(value, validation.request) } },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e' },
            historical: { revision: '1', inputs: ['result'], validator: 'v' } } });
    try {
        const original = await runner.run({ flow: 'a', kind: 'request', request });
        for (const flow of ['a', 'historical']) {
            const direct = await runner.run({ flow, kind: 'result', result: 42, validation: { request } });
            assert.deepEqual((await runner.store.get(direct.taskId, runner.namespace)).task.input.validation.request, request);
            const replay = await runner.reprocess(original.taskId, { flow });
            const imported = await runner.importTask(runner.exportTask(replay.taskId), { flow });
            for (const derived of [replay, imported]) {
                assert.deepEqual((await runner.store.get(derived.taskId, runner.namespace)).task.request, request);
            }
        }
    } finally { await runner.close(); }
});

for (const [name, factory] of stores) test(`${name}: recovered evaluation retains metadata and binds the evaluated request`, async () => {
    const store = await factory(); let evaluations = 0; let validations = 0;
    const request = { model: 'normalized' };
    const runner = createRunner({ store, namespace: `resolution-context-${crypto.randomUUID()}`,
        evaluators: { e: { revision: '1', validate: () => request, execute() { evaluations++; throw new ConnectorError('TIMEOUT', 'Uncertain'); } } },
        validators: { v: { revision: '1', validate(value, { validation }) {
            validations++; assert.deepEqual(validation, { attestation: { source: 'verified' }, request }); return value;
        } } }, flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v' } } });
    try {
        const original = await runner.run({ flow: 'a', kind: 'request', request: { model: 'original' },
            validation: { attestation: { source: 'verified' }, request: { model: 'untrusted' } } });
        assert.equal(original.status, 'unknown');
        await runner.resolveEvaluation(original.taskId, { status: 'succeeded', result: 42, reason: 'Recovered provider result' });
        assert.equal((await runner.execute(original.taskId)).status, 'completed');
        const derived = await runner.reprocess(original.taskId);
        assert.equal((await runner.execute(derived.taskId)).status, 'completed');
        assert.equal(evaluations, 1); assert.equal(validations, 2);
    } finally { await runner.close(); await store.close(); }
});

for (const [name, factory] of stores) test(`${name}: saved-result operations retain complete validation context`, { timeout: 30000 }, async () => {
    const store = await factory();
    try {
        for (const context of [{ request: { model: 'original' }, attestation: { source: 'verified' } }, 'opaque-validation-tag', null, undefined]) {
            let sends = 0; let validated = 0;
            const runner = createRunner({ store, namespace: `saved-context-${crypto.randomUUID()}`,
                validators: { v: { revision: '1', validate(value, { validation }) {
                    validated++; assert.deepEqual(validation, context ?? {}); return value;
                } } }, connectors: { sink: createCallbackConnector(async () => { sends++; return { status: 'acknowledged' }; }) },
                flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                    { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
                ] } } });
            try {
                const original = await runner.run({ flow: 'a', kind: 'result', result: 42,
                    ...(context === undefined ? {} : { validation: context }) });
                const reprocessed = await runner.reprocess(original.taskId); await runner.execute(reprocessed.taskId);
                assert.equal(sends, 1);
                const redelivered = await runner.redeliver(original.taskId, { flow: 'a' }); await runner.execute(redelivered.taskId);
                const imported = await runner.importTask(await runner.exportTask(original.taskId), { flow: 'a' });
                assert.equal((await runner.execute(imported.taskId)).status, 'completed');
                assert.equal(sends, 3); assert.equal(validated, 4);
                assert.equal(imported.evaluation.result, 42);
            } finally { await runner.close(); }
        }
    } finally { await store.close(); }
});
