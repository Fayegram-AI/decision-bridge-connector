import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import { createDurableAggregator, createRegistry } from '../src/index.mjs';
import { createRunner, createConfiguredRunner } from './helpers/runner.mjs';

import { createHost } from '../src/node/host.mjs';
import { createSpoolSource } from '../src/node/spool.mjs';
import { createIncoming } from '../src/node/incoming.mjs';
import { createRedisSource } from '../src/integrations/redis-source.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const flows = { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } };
const evaluators = { e: { revision: '1', execute: value => value } };
const rule = { id: 'pair', revision: '1', flow: 'a', sources: ['one', 'two'], windowMs: 60000 };
const binding = { rule: 'pair', member: 'one', correlationPath: '$.case', eventIdPath: '$.id' };

test('a volatile host rejects an acknowledging message before admitting or executing it', async () => {
    const rejected = Promise.withResolvers();
    let evaluations = 0;
    const runner = createRunner({ flows, evaluators: { e: { revision: '1', execute(value) { evaluations++; return value; } } } });
    const source = { async *open() {
        yield { input: { flow: 'a', kind: 'event', event: 1 },
            acknowledge() { assert.fail('Rejected message must never be acknowledged.'); },
            reject(error) { rejected.resolve(error); } };
    } };
    const host = await createHost({ runner, sources: { source }, pollMs: 5 });
    try {
        assert.equal((await rejected.promise).code, 'CONFIGURATION_ERROR');
        assert.deepEqual((await runner.list()).items, []);
        assert.equal(await runner.tick(), 0);
        assert.equal(evaluations, 0);
    } finally { await host.close(); }
});

test('finite incoming sources retain the flow and input mode validated at construction', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-source-settings-')); const controller = new AbortController();
    try {
        const path = join(directory, 'input.json'); await writeFile(path, '42');
        const settings = { path, flow: 'original', inputMode: 'result', format: 'json' };
        const source = await createIncoming('file', settings);
        settings.flow = 'changed'; settings.inputMode = 'event'; settings.path = join(directory, 'missing.json');
        const iterator = source.open({ signal: controller.signal });
        try {
            const { input } = (await iterator.next()).value;
            assert.equal(input.flow, 'original'); assert.equal(input.kind, 'result'); assert.equal(input.result, 42);
            assert.equal(Object.hasOwn(input, 'event'), false);
        } finally { await iterator.return(); }
    } finally { controller.abort(); await rm(directory, { recursive: true }); }
});

test('configured incoming names and aggregation bindings reject before extension factories or file claims', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-source-preflight-')); let factories = 0;
    const registry = createRegistry().register('evaluator', 'probe', { apiVersion: 1, revision: '1', schema: { type: 'object' },
        create() { factories++; return evaluators.e; } });
    try {
        const original = '{"id":"one","case":"case"}'; await writeFile(join(directory, 'valid.json'), original);
        const { id: _id, ...configuredRule } = rule;
        const config = { schemaVersion: 3, flows, evaluators: { e: { type: 'probe' } }, aggregations: { pair: configuredRule } };
        for (const [name, aggregation] of [['x'.repeat(257), binding], ['', binding], ['bad\nname', binding],
            ['one', { ...binding, member: 'typo' }], ['one', { ...binding, rule: 'missing' }],
            ['one', { ...binding, correlationPath: 'a..b' }], ['one', { ...binding, eventIdPath: '/bad~2escape' }],
            ['one', { ...binding, correlationPath: '__proto__.case' }], ['one', { ...binding, correlationPath: undefined }],
            ['one', { ...binding, extra: true }]]) {
            const aggregationValue = Object.fromEntries(Object.entries(aggregation).filter(([, value]) => value !== undefined));
            await assert.rejects(createConfiguredRunner({ ...config, incoming: { [name]: { type: 'spool',
                options: { directory, flow: 'member', inputMode: 'event' }, aggregation: aggregationValue } } }, { registry }),
            { code: 'CONFIGURATION_ERROR' });
        }
        assert.equal(factories, 0); assert.deepEqual(await readdir(directory), ['valid.json']);
        assert.equal(await readFile(join(directory, 'valid.json'), 'utf8'), original);
    } finally { await rm(directory, { recursive: true }); }
});

test('SDK host preflights all names, aggregation contracts and durable source requirements before worker effects', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-sdk-source-preflight-'));
    let evaluations = 0; let opened = 0; let commands = 0;
    const runner = createRunner({ evaluators: { e: { revision: '1', execute(value) { evaluations++; return value; } } }, flows });
    const aggregate = createDurableAggregator({ runner, rule });
    const source = { aggregation: binding, async *open() { opened++; } };
    try {
        await writeFile(join(directory, 'valid.json'), '{}');
        await runner.submit({ flow: 'a', kind: 'event', event: {} });
        const cases = [
            { sources: { ['x'.repeat(257)]: source } },
            { sources: { one: { ...source, aggregation: { ...binding, member: 'typo' } } } },
            { sources: { one: { ...source, aggregation: { ...binding, eventIdPath: 'constructor.id' } } } },
            { sources: { one: { ...source, aggregation: { ...binding, rule: 'missing' } } } },
            { sources: { one: { ...source, start() { opened++; } } } },
            { sources: { one: source }, aggregators: { pair: {} } },
            { sources: { one: createSpoolSource({ directory, flow: 'a', inputMode: 'event' }) } },
            { sources: { one: createRedisSource({ stream: 'events', group: 'workers', flow: 'a', inputMode: 'event',
                client: { sendCommand() { commands++; } } }) } },
            // Validate every source before starting the first valid one.
            { sources: { first: { async start() { opened++; } }, invalid: {} } },
        ];
        for (const options of cases) await assert.rejects(createHost({ runner, aggregators: { pair: aggregate }, ...options }),
            { code: 'CONFIGURATION_ERROR' });
        assert.equal(opened, 0); assert.equal(evaluations, 0); assert.equal(commands, 0);
        assert.deepEqual(await readdir(directory), ['valid.json']);
    } finally { await runner.close(); await rm(directory, { recursive: true }); }
});

test('worker-only hosts ignore unconsumed sources and custom sources retain per-message acknowledgement contracts', async () => {
    const runner = createRunner({ evaluators, flows }); let opened = 0; let host;
    try {
        const task = await runner.submit({ flow: 'a', kind: 'event', event: 42 });
        host = await createHost({ runner, role: 'worker', sources: { ['x'.repeat(257)]: { start() { opened++; } } }, pollMs: 5 });
        for (let attempt = 0; attempt < 100 && (await runner.get(task.taskId)).status !== 'completed'; attempt++) await sleep(5);
        assert.equal((await runner.get(task.taskId)).status, 'completed'); assert.equal(opened, 0);
    } finally { await host?.close(); await runner.close(); }
    const second = createRunner({ evaluators, flows }); let custom;
    try {
        custom = await createHost({ runner: second, role: 'ingress', sources: { custom: { async *open() {} } } });
        assert.equal((await custom.health()).ready, true);
    } finally { await custom?.close(); await second.close(); }
});

const stores = [['sqlite', directory => createSQLiteStore(join(directory, 'state.sqlite'))],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];
for (const [name, factory] of stores) test(`${name}: explicit semantic member IDs deduplicate distinct spool transports without dropping evidence`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-semantic-spool-')); const store = await factory(directory);
    const runner = createRunner({ store, namespace: `semantic-${crypto.randomUUID()}`, evaluators, flows });
    const aggregate = createDurableAggregator({ runner, rule }); let host;
    const event = { id: 'logical-one', case: 'case', value: 42 };
    try {
        await writeFile(join(directory, 'first.json'), JSON.stringify(event));
        await writeFile(join(directory, 'second.json'), JSON.stringify(event));
        host = await createHost({ runner, role: 'ingress', aggregators: { pair: aggregate }, sources: {
            one: { ...createSpoolSource({ directory, flow: 'member', inputMode: 'event', pollMs: 5 }), aggregation: binding },
        } });
        for (let attempt = 0; attempt < 200; attempt++) {
            if ((await readdir(join(directory, '.processed')).catch(() => [])).length === 2) break;
            await sleep(5);
        }
        assert.equal((await readdir(join(directory, '.processed'))).length, 2);
        assert.deepEqual(await readdir(join(directory, '.quarantine')), []);
        const completed = await aggregate.add({ source: 'two', eventId: 'logical-two', correlationKey: 'case', event: { value: 7 } });
        assert.equal(completed.emitted, true); assert.equal(completed.events.one.eventId, event.id);
        assert.deepEqual(completed.events.one.event, event);
        assert.equal((await runner.list()).items.length, 1); assert.deepEqual((await host.health()).errors, []);
    } finally { await host?.close(); await runner.close(); await store.close(); await rm(directory, { recursive: true }); }
});

test('aggregation without an event ID selector retains the transport key and captures routing before iteration', async () => {
    const runner = createRunner({ evaluators, flows }); const received = Promise.withResolvers();
    const aggregation = { rule: 'pair', member: 'one', correlationPath: '$.case' };
    const host = await createHost({ runner, role: 'ingress', aggregators: { pair: { async add(value) { received.resolve(value); return {}; } } },
        sources: { source: { aggregation, async *open() {
            aggregation.member = 'mutated'; aggregation.correlationPath = '$.missing';
            yield { input: { kind: 'event', event: { case: 'case' }, idempotencyKey: 'transport-id' } };
        } } } });
    try {
        const value = await received.promise;
        assert.equal(value.eventId, 'transport-id'); assert.equal(value.source, 'one'); assert.equal(value.correlationKey, 'case');
    } finally { await host.close(); await runner.close(); }
});
