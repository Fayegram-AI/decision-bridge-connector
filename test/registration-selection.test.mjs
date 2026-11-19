import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createCallbackConnector, createRegistry, ConnectorError } from '../src/index.mjs';
import { createRunner, createConfiguredRunner } from './helpers/runner.mjs';

import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) test(`${name}: saved-result operations use the selected validator without name coercion`, { timeout: 60000 }, async () => {
    const store = await factory(); let expected = 0; let unrelated = 0;
    const runner = createRunner({ store, namespace: `selector-${crypto.randomUUID()}`,
        evaluators: { e: { revision: '1', execute() { throw new ConnectorError('TIMEOUT', 'Provider outcome unknown.'); },
            validateSaved(value) { expected++; if (!value.verified) throw new ConnectorError('INVALID_SAVED_OUTPUT', 'Invalid saved evidence.'); return value; } } },
        validators: { undefined: { revision: 'unrelated', validate(value) { unrelated++; return value; } } },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e' },
            literal: { revision: '1', inputs: ['result'], validator: 'undefined' } } });
    const bad = { flow: 'a', kind: 'result', result: { verified: false } };
    try {
        await assert.rejects(runner.preview(bad), { code: 'INVALID_SAVED_OUTPUT' });
        await assert.rejects(runner.submit(bad), { code: 'INVALID_SAVED_OUTPUT' });
        assert.equal((await runner.list()).items.length, 0);
        const original = await runner.run({ ...bad, result: { verified: true } });
        const reprocessed = await runner.reprocess(original.taskId); await runner.execute(reprocessed.taskId);
        const redelivered = await runner.redeliver(original.taskId, { flow: 'a' }); await runner.execute(redelivered.taskId);
        const imported = await runner.importTask(await runner.exportTask(original.taskId), { flow: 'a' }); await runner.execute(imported.taskId);
        const unknown = await runner.run({ flow: 'a', kind: 'request', request: 1 });
        await assert.rejects(runner.resolveEvaluation(unknown.taskId, { status: 'succeeded', reason: 'Recovered', result: bad.result }),
            { code: 'INVALID_SAVED_OUTPUT' });
        assert.equal((await runner.get(unknown.taskId)).evaluation.status, 'unknown');
        await runner.resolveEvaluation(unknown.taskId, { status: 'succeeded', reason: 'Recovered', result: { verified: true } });
        assert.equal((await runner.execute(unknown.taskId)).status, 'completed');
        assert.equal(expected, 8); assert.equal(unrelated, 0);
        assert.equal((await runner.run({ ...bad, flow: 'literal' })).status, 'completed');
        assert.equal(unrelated, 1, 'The literal string remains a valid explicit name.');
    } finally { await runner.close(); await store.close(); }
});

test('admission requires its pinned validator revision instead of falling back to another validator', async () => {
    for (const selected of ['validator', 'evaluator']) {
        let validations = 0;
        const options = { evaluators: { e: { revision: '1', execute: value => value, validateSaved(value) { validations++; return value; } } },
            validators: { v: { revision: '1', validate(value) { validations++; return value; } } },
            flows: { a: { revision: '1', inputs: ['result'], evaluator: 'e', ...(selected === 'validator' ? { validator: 'v' } : {}) } } };
        const runner = createRunner(options);
        const entries = selected === 'validator' ? options.validators : options.evaluators; const key = selected === 'validator' ? 'v' : 'e';
        const original = entries[key];
        try {
            await runner.ready; delete entries[key];
            await assert.rejects(runner.submit({ flow: 'a', kind: 'result', result: 1 }), { code: 'REVISION_UNAVAILABLE' });
            entries[key] = { ...original, revision: '2' };
            await assert.rejects(runner.preview({ flow: 'a', kind: 'result', result: 1 }), { code: 'REVISION_UNAVAILABLE' });
            assert.equal(validations, 0); assert.equal((await runner.list()).items.length, 0);
            entries[key] = original;
            assert.equal((await runner.run({ flow: 'a', kind: 'result', result: 1 })).status, 'completed');
        } finally { await runner.close(); }
    }
});

test('connection policies require explicit string selectors and retain valid literal names', async () => {
    let sends = 0;
    const names = ['undefined', 'null', '0', 'false', 'constructor'];
    const options = connection => ({ validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async () => { sends++; return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', operation: 'extraction', inputs: ['result'], validator: 'v',
            policies: Object.fromEntries(names.map(name => [name, { revision: '1', accept: { path: 'ok', equals: true } }])),
            connections: [{ id: 'out', connector: 'sink', when: 'decision.accepted', payload: 'final-decision', ...connection }] } } });
    for (const selector of [undefined, null, 0, false, 'missing']) {
        const runner = createRunner(options(selector === undefined ? {} : { policy: selector }));
        try { await assert.rejects(runner.ready, { code: 'CONFIGURATION_ERROR' }); } finally { await runner.close(); }
    }
    const unused = createRunner(options({ when: 'always', payload: 'raw-result', policy: 'missing' }));
    try { await assert.rejects(unused.ready, { code: 'CONFIGURATION_ERROR' }); } finally { await unused.close(); }
    assert.equal(sends, 0);
    for (const policy of names) {
        const runner = createRunner(options({ policy }));
        try { assert.equal((await runner.run({ flow: 'a', kind: 'result', result: { ok: true } })).deliveries[0].status, 'acknowledged'); }
        finally { await runner.close(); }
    }
    assert.equal(sends, names.length);
});

test('configured extension types cannot resolve absent or non-string selectors', async () => {
    let factories = 0;
    const registry = () => {
        const value = createRegistry();
        for (const name of ['undefined', 'null', '0', 'constructor', 'toString']) value.register('validator', name,
            { apiVersion: 1, revision: '1', schema: { type: 'object' }, create() { factories++; return { validate: value => value }; } });
        return value;
    };
    for (const selector of [undefined, null, 0, false, {}]) assert.throws(() => registry().get('validator', selector), { code: 'CONFIGURATION_ERROR' });
    assert.throws(() => registry().get('invalid-kind', 'undefined'), { code: 'CONFIGURATION_ERROR' });
    const config = descriptor => ({ schemaVersion: 3, validators: { v: descriptor }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    for (const descriptor of [{}, { type: null }, { type: 0 }])
        await assert.rejects(createConfiguredRunner(config(descriptor), { registry: registry() }), { code: 'CONFIGURATION_ERROR' });
    assert.equal(factories, 0);
    for (const type of ['undefined', 'null', '0', 'constructor', 'toString']) {
        const runner = await createConfiguredRunner(config({ type }), { registry: registry() });
        try { assert.equal((await runner.run({ flow: 'a', kind: 'result', result: 1 })).status, 'completed'); }
        finally { await runner.close(); }
    }
    assert.equal(factories, 5);
});
