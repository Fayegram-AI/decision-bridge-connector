import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import Ajv from 'ajv';
import { createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner, createConfiguredRunner } from './helpers/runner.mjs';


test('malformed downstream completion metadata remains unknown after delivery invocation', async () => {
    for (const completion of ['completed', null, {}, { status: 'invalid' }, { status: 'failed', detail: 'Downstream failed.' }]) {
        let calls = 0;
        const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
            connectors: { sink: createCallbackConnector(async () => { calls++; return { status: 'acknowledged', completion }; }) },
            flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
            ] } } });
        try {
            const result = await runner.run({ flow: 'a', kind: 'result', result: 1 });
            const delivery = result.deliveries[0]; const valid = completion?.status === 'failed';
            assert.equal(delivery.status, valid ? 'acknowledged' : 'unknown');
            assert.equal(delivery.attempts.length, 1);
            if (valid) assert.deepEqual(delivery.completion, completion);
            else { assert.equal(delivery.error.code, 'INVALID_RECEIPT'); assert.equal(delivery.completion.status, 'untracked'); }
            await runner.tick(); assert.equal(calls, 1);
        } finally { await runner.close(); }
    }
});

test('output validation rejection, false results and timeouts prevent recipient invocation', async () => {
    for (const mode of ['false', 'invalid', 'async-false', 'async-reject', 'timeout', 'accept']) {
        let sent = 0; let validated = false;
        const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
            connectors: { sink: createCallbackConnector(async () => { assert(validated); sent++; return { status: 'acknowledged' }; }) },
            validateOutput() {
                if (mode === 'false') return false;
                if (mode === 'invalid') return 0;
                if (mode === 'timeout') return new Promise(() => {});
                return sleep(10).then(() => {
                    validated = true;
                    if (mode === 'async-reject') throw new ConnectorError('OUTPUT_SCHEMA', 'Rejected');
                    return mode === 'accept';
                });
            }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', outputSchema: {}, timeoutMs: mode === 'timeout' ? 100 : 10000 },
            ] } } });
        try {
            const result = await runner.run({ flow: 'a', kind: 'result', result: 1 });
            assert.equal(result.deliveries[0].status, mode === 'accept' ? 'acknowledged' : 'failed');
            assert.equal(sent, mode === 'accept' ? 1 : 0);
            if (mode !== 'accept') assert.equal(result.deliveries[0].error.code, mode === 'timeout' ? 'TIMEOUT' : 'OUTPUT_SCHEMA');
        } finally { await runner.close(); }
    }
});

test('configured boolean output schemas are enforced and supported by published configuration schema', async () => {
    const { readFile } = await import('node:fs/promises');
    const validateConfig = new Ajv({ strict: true }).compile(JSON.parse(await readFile(new URL('../schemas/config.schema.json', import.meta.url))));
    for (const outputSchema of [false, true]) {
        const directory = await mkdtemp(join(tmpdir(), 'dbc-output-schema-'));
        const config = { schemaVersion: 3, placements: ['local'], validators: { v: { type: 'schema', options: { schema: {} } } },
            connectors: { sink: { type: 'file', options: { directory } } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', placement: 'local', outputSchema },
            ] } } };
        assert(validateConfig(config), JSON.stringify(validateConfig.errors));
        const runner = await createConfiguredRunner(config);
        try {
            const result = await runner.run({ flow: 'a', kind: 'result', result: 1 });
            assert.equal(result.deliveries[0].status, outputSchema ? 'acknowledged' : 'failed');
            assert.equal((await readdir(directory)).length, outputSchema ? 1 : 0);
        } finally { await runner.close(); await rm(directory, { recursive: true }); }
    }
});

test('configured async schemas reject invalid data and repeated schema IDs remain isolated', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-async-schema-'));
    const schema = { $id: 'urn:dbc:async-input', $async: true, type: 'boolean' };
    const outputSchema = { $id: 'urn:dbc:async-output', $async: true, const: false };
    const config = { schemaVersion: 3, placements: ['local'], validators: {
        v: { type: 'schema', options: { schema } }, duplicate: { type: 'schema', options: { schema } },
    }, connectors: { sink: { type: 'file', options: { directory } } }, flows: { a: {
        revision: '1', inputs: ['result'], validator: 'v', connections: ['one', 'two'].map(id => ({
            id, connector: 'sink', when: 'always', payload: 'raw-result', placement: 'local', outputSchema,
        })),
    } } };
    try {
        for (let i = 0; i < 2; i++) {
            const runner = await createConfiguredRunner(config);
            try {
                await assert.rejects(runner.submit({ flow: 'a', kind: 'result', result: 1 }), { code: 'INVALID_RESULT' });
                const rejected = await runner.run({ flow: 'a', kind: 'result', result: true });
                assert(rejected.deliveries.every(delivery => delivery.status === 'failed' && delivery.error.code === 'OUTPUT_SCHEMA'));
                const accepted = await runner.run({ flow: 'a', kind: 'result', result: false });
                assert.equal(accepted.status, 'completed'); assert(accepted.deliveries.every(delivery => delivery.status === 'acknowledged'));
            } finally { await runner.close(); }
        }
        assert.equal((await readdir(directory)).length, 4);
    } finally { await rm(directory, { recursive: true }); }
});

test('async extension settings are validated before the factory can create resources', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-extension-schema-'));
    await writeFile(join(directory, 'guarded.mjs'), `import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
export default { apiVersion: 1, revision: '1', schema: { $async: true, type: 'object', required: ['allow'],
    properties: { allow: { const: true } }, additionalProperties: false },
    create(_options, { baseDir }) { writeFileSync(join(baseDir, 'constructed'), 'created');
        return { revision: '1', send: async () => ({ status: 'acknowledged' }) }; } };`);
    const config = { schemaVersion: 3, extensions: [{ kind: 'outgoing', name: 'guarded', module: './guarded.mjs' }],
        validators: { v: { type: 'schema', options: { schema: {} } } },
        connectors: { sink: { type: 'guarded', options: { allow: false } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
    try {
        await assert.rejects(createConfiguredRunner(config, { baseDir: directory }), { code: 'CONFIGURATION_ERROR' });
        assert.deepEqual(await readdir(directory), ['guarded.mjs']);
        const runner = await createConfiguredRunner({ ...config, connectors: { sink: { type: 'guarded', options: { allow: true } } } }, { baseDir: directory });
        await runner.close(); assert((await readdir(directory)).includes('constructed'));
    } finally { await rm(directory, { recursive: true }); }
});
