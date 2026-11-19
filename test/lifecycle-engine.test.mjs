import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createMemoryStore, createCallbackConnector, ConnectorError } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createHost } from '../src/node/host.mjs';
import { createService } from '../src/node/service.mjs';
import { checkConnector, checkStore } from '../src/testing.mjs';

const validator = { revision: '1', validate: value => value };
const output = (id, connector, extra = {}) => ({ id, connector, when: 'evaluation.succeeded', payload: 'raw-result', ...extra });

test('chained responses have persisted lineage and finite execution depth', async () => {
    const runner = createRunner({ validators: { v: validator }, evaluators: { e: { revision: '1', execute: async value => value } },
        connectors: { response: createCallbackConnector(async value => ({ status: 'acknowledged', receipt: { response: value + 1 } })) },
        flows: { a: { revision: '1', inputs: ['result', 'event'], validator: 'v', evaluator: 'e', template: { $path: '$' },
            maxDepth: 2, connections: [output('next', 'response', { nextFlow: 'a', chainRetry: { maxAttempts: 3, delayMs: 10, maxDelayMs: 50 } })] } } });
    try {
        const parent = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        assert.equal(parent.chains[0].status, 'acknowledged');
        const child = await runner.execute(parent.chains[0].childTaskId);
        assert.equal(child.evaluation.result, 2); assert.equal(child.lineage.rootId, parent.taskId);
        assert.equal(child.status, 'partial'); assert.equal(child.chains[0].error.code, 'RESOURCE_LIMIT');
        assert.equal((await runner.list()).items.length, 2);
    } finally { await runner.close(); }
});

test('delivery failures activate failure outputs once without recursive delivery', async () => {
    const received = [];
    const runner = createRunner({ validators: { v: validator }, connectors: {
        broken: createCallbackConnector(async () => { throw new ConnectorError('REJECTED', 'Rejected.', { ambiguous: false, retryable: false }); }),
        failure: createCallbackConnector(async value => { received.push(value); return { status: 'acknowledged' }; }),
    }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [output('bad', 'broken'),
        output('alert', 'failure', { when: 'failure', payload: 'failure', required: false })] } } });
    try {
        const first = await runner.run({ flow: 'a', kind: 'result', result: {} }, { waitFor: 'settled' });
        assert.equal(first.status, 'partial'); assert.equal(received.length, 1);
        await runner.tick(); assert.equal(received.length, 1);
        assert.equal(received[0].errors[0].stage, 'delivery');
    } finally { await runner.close(); }
});

test('ordering survives retention and active idempotency survives dedupe expiry', async () => {
    const store = createMemoryStore({ retentionMs: 10, dedupeMs: 10 });
    const runner = createRunner({ store, validators: { v: validator }, connectors: {
        sink: createCallbackConnector(async () => { throw new ConnectorError('REJECTED', 'No.', { ambiguous: false, retryable: false }); }),
    }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [output('out', 'sink', { ordering: { key: 'shared' } })] } } });
    try {
        const failed = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        const queued = await runner.submit({ flow: 'a', kind: 'result', result: 2, idempotencyKey: 'second' });
        await sleep(20); await runner.maintain();
        assert(await runner.get(failed.taskId));
        assert.equal((await runner.submit({ flow: 'a', kind: 'result', result: 2, idempotencyKey: 'second' })).taskId, queued.taskId);
        const pending = await runner.execute(queued.taskId, { waitMs: 50 });
        assert.equal(pending.deliveries[0].attempts.length, 0);
    } finally { await runner.close(); await store.close(); }
});

test('host shutdown bounds an incoming iterator that ignores cancellation', async () => {
    const runner = createRunner({ validators: { v: validator }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    const host = await createHost({ runner, role: 'ingress', drainMs: 30,
        sources: { stuck: { async *open() { await new Promise(() => {}); } } } });
    const start = Date.now();
    await assert.rejects(host.close(), error => error instanceof AggregateError && error.errors.some(failure => failure.code === 'TIMEOUT'));
    assert(Date.now() - start < 500);
});

test('reload retains old revision workers while new requests use new contracts', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-reload-'));
    const configuration = revision => ({ schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
        validators: { v: { type: 'schema', options: { schema: {} } } },
        flows: { a: { revision, inputs: ['result'], validator: 'v' } } });
    const service = await createService(configuration('1'), { baseDir: directory, pollMs: 10 });
    try {
        const old = await service.runner.submit({ flow: 'a', kind: 'result', result: 1 });
        await service.reload(configuration('2'));
        const next = await service.runner.run({ flow: 'a', kind: 'result', result: 2 });
        assert.equal(next.flowRevision, '2');
        for (let i = 0; i < 50 && (await service.runner.get(old.taskId)).status === 'running'; i++) await sleep(10);
        assert.equal((await service.runner.get(old.taskId)).status, 'completed');
        assert.equal((await service.health()).retainedGenerations, 0);
    } finally { await service.close(); await rm(directory, { recursive: true }); }
});

test('extension conformance helpers exercise receipts and concurrent store visibility', async () => {
    assert.equal((await checkConnector(createCallbackConnector(async () => ({ status: 'acknowledged' })))).passed, true);
    const store = createMemoryStore();
    try { assert.equal((await checkStore(store)).passed, true); } finally { await store.close(); }
});

test('expired or unplannable deliveries settle their unstarted response chains', async () => {
    for (const expiry of [true, false]) {
        const runner = createRunner({ validators: { v: validator }, evaluators: { e: { revision: '1', execute: async value => value } },
            connectors: { sink: createCallbackConnector(async () => { throw Error('Must not dispatch'); }) },
            flows: { a: { revision: '1', inputs: ['result', 'event'], evaluator: 'e', validator: 'v', template: { $path: '$' },
                deliveryTtlMs: expiry ? 1 : 60000,
                connections: [output('next', 'sink', { nextFlow: 'a', chainRetry: { maxAttempts: 3, delayMs: 10, maxDelayMs: 50 }, ...(expiry ? {} : { projection: { $path: 'missing' } }) })] } } });
        try {
            const task = await runner.submit({ flow: 'a', kind: 'result', result: {} }); await sleep(10);
            const report = await runner.execute(task.taskId, { waitFor: 'settled', waitMs: 1000 });
            assert.equal(report.status, 'partial'); assert.equal(report.chains[0].status, 'skipped'); assert.equal(report.settled, true);
        } finally { await runner.close(); }
    }
});
