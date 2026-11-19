import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';
import Ajv from 'ajv';
import { createCallbackConnector } from '../src/index.mjs';
import { createRunner } from '../src/engine/runner.mjs';

test('published incoming schema bounds source identities and aggregation selectors', async () => {
    const validate = new Ajv({ strict: true }).compile(JSON.parse(await readFile(new URL('../schemas/config.schema.json', import.meta.url))));
    const configuration = (name, aggregation) => ({ schemaVersion: 3, flows: { a: { revision: '1' } },
        incoming: { [name]: { type: 'spool', aggregation } } });
    const binding = { rule: 'pair', member: 'one', correlationPath: '$.case', eventIdPath: '/id' };
    assert(validate(configuration('one', binding)), JSON.stringify(validate.errors));
    for (const name of ['', 'bad\nname', 'x'.repeat(257)]) assert.equal(validate(configuration(name, binding)), false);
    for (const bindingValue of [{ ...binding, rule: '' }, { ...binding, member: 'bad\nname' },
        { ...binding, eventIdPath: 'x'.repeat(4097) }, { ...binding, correlationPath: null }])
        assert.equal(validate(configuration('one', bindingValue)), false);
});

test('published identity and placement schemas match strict runtime fields', async () => {
    const validate = new Ajv({ strict: true }).compile(JSON.parse(await readFile(new URL('../schemas/config.schema.json', import.meta.url))));
    const base = { schemaVersion: 3, flows: { a: { revision: '1' } } };
    for (const value of ['', null, 1, 'bad\nname', 'x'.repeat(257)]) {
        for (const key of ['namespace', 'workerId']) assert.equal(validate({ ...base, [key]: value }), false);
        assert.equal(validate({ ...base, placements: [value] }), false);
        assert.equal(validate({ ...base, flows: { a: { revision: '1', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', placement: value },
        ] } } }), false);
    }
    for (const placements of [[], ['local'], ['local', 'remote']]) assert(validate({ ...base, placements }));
    assert.equal(validate({ ...base, placements: 'not-local' }), false);
    assert.equal(validate({ ...base, placements: ['local', 'local'] }), false);
});

test('published retry and dependency schemas reject malformed settings without removing valid defaults', async () => {
    const validate = new Ajv({ strict: true }).compile(JSON.parse(await readFile(new URL('../schemas/config.schema.json', import.meta.url))));
    const configuration = settings => ({ schemaVersion: 3, flows: { a: { revision: '1', connections: [
        { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', ...settings },
    ] } } });
    for (const settings of [{}, { retry: {} }, { retry: { maxAttempts: 1, delayMs: 1, maxDelayMs: 9007199254740991, ambiguous: 'hold' } },
        { dependsOn: [] }, { dependsOn: ['parentAB'] }]) assert(validate(configuration(settings)), JSON.stringify(validate.errors));
    for (const settings of [{ retry: null }, { retry: [] }, { retry: { maxAttemps: 1 } }, { retry: { maxAttempts: 101 } },
        { retry: { delayMs: 0 } }, { retry: { delayMs: 1.5 } }, { retry: { ambiguous: 'maybe' } },
        { dependsOn: 'parentAB' }, { dependsOn: null }, { dependsOn: [''] }, { dependsOn: [1] }])
        assert.equal(validate(configuration(settings)), false, JSON.stringify(settings));
});

test('published schemas validate complete example configurations and actual reports', async () => {
    const ajv = new Ajv({ strict: true, allErrors: true });
    const config = ajv.compile(JSON.parse(await readFile(new URL('../schemas/config.schema.json', import.meta.url))));
    const connection = { id: 'next', connector: 'sink', when: 'always', payload: 'raw-result', nextFlow: 'child' };
    const chaining = { schemaVersion: 3, flows: { parent: { revision: '1', connections: [connection] } } };
    assert.equal(config(chaining), false);
    connection.chainRetry = { maxAttempts: 1, delayMs: 10, maxDelayMs: 100 };
    assert(config(chaining), JSON.stringify(config.errors));
    delete connection.chainRetry.maxAttempts; assert.equal(config(chaining), false);
    connection.chainRetry.timeoutMs = 1000; assert(config(chaining));
    for (const name of await readdir(new URL('../examples', import.meta.url))) if (name.endsWith('.config.json')) {
        assert(config(JSON.parse(await readFile(new URL(`../examples/${name}`, import.meta.url)))), `${name}: ${JSON.stringify(config.errors)}`);
    }
    const validate = ajv.compile(JSON.parse(await readFile(new URL('../schemas/record.schema.json', import.meta.url))));
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
        evaluators: { e: { revision: '1', execute: value => value } },
        connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged', receipt: { response: 42 } })) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [{ ...connection,
            chainRetry: { maxAttempts: 1, delayMs: 10, maxDelayMs: 100 } }] },
        child: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    try { assert(validate(await runner.run({ flow: 'a', kind: 'result', result: {} })), JSON.stringify(validate.errors)); }
    finally { await runner.close(); }
});
