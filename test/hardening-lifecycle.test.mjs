import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRunner, createDurableAggregator, ConnectorError, publicError } from '../src/index.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createService } from '../src/node/service.mjs';
import { createHost } from '../src/node/host.mjs';
import { createLiveSource } from '../src/integrations/live.mjs';
import { shouldQuarantine } from '../src/node/source-recovery.mjs';
import { bounded, delay } from '../src/util.mjs';

const options = () => ({ evaluators: { e: { revision: '1', execute: value => value } }, validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['event', 'result'], evaluator: 'e', validator: 'v', template: { $path: '$' } } } });

test('replacement aggregation remembers superseded event IDs through emission and object recreation', async () => {
    const runner = createRunner(options());
    const rule = { id: 'replace', revision: '1', sources: ['one', 'two'], duplicate: 'replace', windowMs: 60000, flow: 'a' };
    const aggregate = () => createDurableAggregator({ runner, rule });
    const event = (eventId, value, source = 'one') => ({ source, eventId, event: value, correlationKey: 'case' });
    try {
        await aggregate().add(event('old', 1)); await aggregate().add(event('new', 2));
        assert.equal((await aggregate().add(event('old', 1))).events.one.event, 2);
        const complete = await aggregate().add(event('last', 3, 'two'));
        assert.equal(complete.emitted, true);
        assert.equal((await aggregate().add(event('old', 1))).events.one.event, 2);
        assert.equal((await runner.list()).items.length, 1);
    } finally { await runner.close(); }
});

test('root ceilings constrain derived operations even when their destination allows more work', async () => {
    const configuration = options();
    configuration.flows.a.maxExecutions = 2; configuration.flows.a.maxDepth = 2;
    configuration.flows.wide = { ...configuration.flows.a, maxExecutions: 100, maxDepth: 100 };
    const runner = createRunner(configuration);
    try {
        const original = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        const child = await runner.reprocess(original.taskId, { flow: 'wide' });
        assert.equal(child.lineage.maxExecutions, 2); assert.equal(child.lineage.maxDepth, 2);
        await assert.rejects(runner.redeliver(original.taskId, { flow: 'wide' }), { code: 'RESOURCE_LIMIT' });
        await assert.rejects(runner.reprocess(child.taskId, { flow: 'wide' }), { code: 'RESOURCE_LIMIT' });
        const imported = await runner.importTask(runner.exportTask(original.taskId), { flow: 'wide' });
        assert.equal(imported.lineage.rootId, imported.taskId); assert.equal(imported.lineage.depth, 0);
        assert.equal(imported.lineage.maxExecutions, 100);
    } finally { await runner.close(); }
});

test('permanent mapping failures quarantine while storage capacity remains recoverable', async () => {
    const configuration = options(); configuration.flows.a.template = { $path: '$.missing', required: true };
    const runner = createRunner({ ...configuration, limits: { maxPending: 1 } });
    try {
        let rejected;
        try { await runner.submit({ flow: 'a', kind: 'event', event: {} }); } catch (error) { rejected = publicError(error); }
        assert.equal(rejected.inputInvalid, true); assert.equal(shouldQuarantine(rejected), true);
        await runner.submit({ flow: 'a', kind: 'result', result: 1 });
        await assert.rejects(runner.submit({ flow: 'a', kind: 'result', result: 2 }), error => {
            assert.equal(shouldQuarantine(publicError(error)), false); assert.equal(error.retryable, true); return error.code === 'RESOURCE_LIMIT';
        });
        assert.equal(shouldQuarantine({ code: 'VALIDATION_ERROR', ambiguous: true, inputInvalid: true }), false);
    } finally { await runner.close(); }
});

test('equivalent worker generations and retired ingress roles do not retain current work', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-reap-equivalent-'));
    const config = (role, revision = '1') => ({ schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
        host: { role, pollMs: 5 }, validators: { v: { type: 'schema', options: { schema: {} } } },
        flows: { a: { revision, inputs: ['result'], validator: 'v' } } });
    let service;
    try {
        service = await createService({ ...config('worker'), placements: ['unused'] }, { baseDir: directory, maxGenerations: 2 });
        await service.runner.pause('flow', 'a', true);
        const pending = await service.runner.submit({ flow: 'a', kind: 'result', result: 1 });
        await service.reload(config('worker'));
        assert.equal((await service.health()).retainedGenerations, 0);
        assert.equal((await service.runner.get(pending.taskId)).status, 'running');
        await service.reload(config('ingress'));
        assert.equal((await service.health()).retainedGenerations, 1, 'The old worker is still needed by an ingress-only replacement.');
        await service.runner.cancel(pending.taskId); assert.equal((await service.health()).retainedGenerations, 0);
        const queued = await service.runner.submit({ flow: 'a', kind: 'result', result: 2 });
        await service.reload(config('ingress', '2'));
        assert.equal((await service.health()).retainedGenerations, 0);
        assert.equal((await service.runner.get(queued.taskId)).status, 'running');
    } finally { await service?.close(); await rm(directory, { recursive: true, force: true }); }
});

test('host readiness follows Live exhaustion and source acknowledgements remain per-envelope attempts', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-source-health-'));
    const store = createSQLiteStore(join(directory, 'state.sqlite'));
    const runner = createRunner({ ...options(), store });
    const source = createLiveSource({ flow: 'a', sessionId: 'exhaustion', reconnect: { maxAttempts: 1, timeoutMs: 100 },
        connect() { throw new ConnectorError('UNAVAILABLE', 'Offline.', { ambiguous: false }); } });
    const host = await createHost({ runner, role: 'ingress', sources: { live: source } });
    try {
        await bounded(async signal => { while (source.status().state !== 'exhausted') await delay(1, signal); }, { timeoutMs: 1000 });
        const health = await host.health(); assert.equal(health.ready, false); assert.equal(health.failures['source:live'].code, 'SOURCE_UNREADY');
        const input = { flow: 'a', kind: 'result', result: 1, idempotencyKey: 'redelivery' };
        const task = await runner.submit(input); let acknowledgements = 0;
        await runner.acknowledge(task.taskId, () => { acknowledgements++; });
        const replay = await runner.submit(input); assert.equal(replay.taskId, task.taskId);
        await runner.acknowledge(replay.taskId, () => { acknowledgements++; });
        assert.equal(acknowledgements, 2);
    } finally { await host.close(); await store.close(); await rm(directory, { recursive: true, force: true }); }
});

test('the first aggregate event establishes its window despite elapsed clock ticks during insertion', async t => {
    const runner = createRunner(options()); let clock = Date.now(); t.mock.method(Date, 'now', () => clock += 5);
    try {
        const aggregate = createDurableAggregator({ runner, rule: { id: 'first', revision: '1', sources: ['one'], windowMs: 1, flow: 'a' } });
        const group = await aggregate.add({ source: 'one', eventId: 'first', event: 1, correlationKey: 'case' });
        assert.equal(group.state, 'complete'); assert.equal(group.emitted, true);
        assert.equal((await runner.list()).items.length, 1);
    } finally { await runner.close(); }
});

test('generation retirement preserves a worker for an actual required placement', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-placement-retention-'));
    const config = placements => ({ schemaVersion: 3, placements, store: { type: 'sqlite', options: { path: 'state.sqlite' } }, host: { role: 'worker' },
        validators: { v: { type: 'schema', options: { schema: {} } } }, connectors: { sink: { type: 'http', options: { url: 'http://127.0.0.1:1' } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [{ id: 'out', connector: 'sink', placement: 'dedicated', when: 'always', payload: 'raw-result' }] } } });
    const service = await createService(config(['dedicated']), { baseDir: directory });
    try {
        await service.runner.pause('flow', 'a', true);
        const task = await service.runner.submit({ flow: 'a', kind: 'result', result: 1 }); await service.reload(config([]));
        assert.equal((await service.health()).retainedGenerations, 1);
        await service.runner.cancel(task.taskId); assert.equal((await service.health()).retainedGenerations, 0);
    } finally { await service.close(); await rm(directory, { recursive: true, force: true }); }
});
