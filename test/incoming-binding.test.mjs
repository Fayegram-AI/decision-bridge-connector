import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createRunner, createConfiguredRunner } from './helpers/runner.mjs';

import { createHost } from '../src/node/host.mjs';
import { createSpoolSource } from '../src/node/spool.mjs';
import { createIncoming } from '../src/node/incoming.mjs';
import { createRedisSource } from '../src/integrations/redis-source.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';

const configuration = { schemaVersion: 3, validators: { v: { type: 'schema', options: { schema: {} } } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };

test('built-in incoming constructors reject malformed binding shapes before consuming input', async () => {
    let commands = 0;
    for (const settings of [{ flow: undefined }, { flow: '' }, { flow: 1 }, { flow: 'a', inputMode: 'unknown' }]) {
        assert.throws(() => createSpoolSource({ directory: '.', ...settings }), { code: 'CONFIGURATION_ERROR' });
        assert.throws(() => createRedisSource({ stream: 'stream', group: 'group', client: { sendCommand() { commands++; } }, ...settings }),
            { code: 'CONFIGURATION_ERROR' });
        for (const type of ['file', 'stdin', 'process', 'http']) await assert.rejects(createIncoming(type, settings), { code: 'CONFIGURATION_ERROR' });
    }
    assert.equal(commands, 0);
});

test('configured source and aggregation bindings reject unavailable or incompatible flows before consuming files', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-binding-config-'));
    try {
        const original = '{"kind":"result","result":42}'; await writeFile(join(directory, 'valid.json'), original);
        for (const settings of [{ flow: 'missing' }, { flow: 'a', inputMode: 'event' }])
            await assert.rejects(createConfiguredRunner({ ...configuration,
                incoming: { spool: { type: 'spool', options: { directory, ...settings } } } }), { code: 'CONFIGURATION_ERROR' });
        await assert.rejects(createConfiguredRunner({ ...configuration,
            aggregations: { invalid: { revision: '1', flow: 'a', sources: ['one'], windowMs: 1000 } } }), { code: 'CONFIGURATION_ERROR' });
        assert.deepEqual(await readdir(directory), ['valid.json']); assert.equal(await readFile(join(directory, 'valid.json'), 'utf8'), original);
    } finally { await rm(directory, { recursive: true }); }
});

test('SDK host preflights built-in bindings before workers, Redis commands or spool claims', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-binding-host-')); let commands = 0; let evaluations = 0;
    const runner = createRunner({ evaluators: { e: { revision: '1', execute() { evaluations++; return 42; } } },
        validators: { v: { revision: '1', validate: value => value } },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v' } } });
    try {
        await runner.submit({ flow: 'a', kind: 'request', request: 1 });
        await writeFile(join(directory, 'valid.json'), '{"kind":"result","result":42}');
        for (const source of [createSpoolSource({ directory, flow: 'missing' }),
            createSpoolSource({ directory, flow: 'a', inputMode: 'event' }),
            createRedisSource({ stream: 'stream', group: 'group', flow: 'missing', client: { sendCommand() { commands++; } } })])
            await assert.rejects(createHost({ runner, sources: { invalid: source } }), { code: 'CONFIGURATION_ERROR' });
        assert.equal(commands, 0); assert.equal(evaluations, 0); assert.deepEqual(await readdir(directory), ['valid.json']);
    } finally { await runner.close(); await rm(directory, { recursive: true }); }
});

test('aggregation member envelopes retain independent input kinds and custom sources need no binding metadata', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-member-binding-'));
    const store = createSQLiteStore(join(directory, 'state.sqlite'));
    const runner = createRunner({ store, validators: { v: { revision: '1', validate: value => value } }, flows: configuration.flows });
    const source = { ...createSpoolSource({ directory: '.', flow: 'member-envelope', inputMode: 'event' }),
        aggregation: { rule: 'combined', correlationPath: '$' }, async *open() {} };
    let host;
    try {
        host = await createHost({ runner, role: 'ingress', sources: { member: source, custom: { async *open() {} } },
            aggregators: { combined: { async add() {}, async flush() {} } } });
        assert.equal((await host.health()).ready, true);
    } finally { await host?.close(); await runner.close(); await store.close(); await rm(directory, { recursive: true }); }
});
