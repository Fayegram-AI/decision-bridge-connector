import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { mkdtemp, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createMemoryStore, createCallbackConnector, createDurableAggregator } from '../src/index.mjs';
import { createRunner, createConfiguredRunner } from './helpers/runner.mjs';
import { compileFlows } from '../src/engine/flows.mjs';

import { createService } from '../src/node/service.mjs';

function configuration(store, revision = '1') {
    return { store, validators: { v: { revision: '1', validate: value => value } },
        evaluators: { e: { revision: '1', execute: async value => value } },
        connectors: { sink: createCallbackConnector(async value => ({ status: 'acknowledged', receipt: { response: value } })) },
        flows: {
            parent: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                { id: 'next', connector: 'sink', when: 'always', payload: 'raw-result', nextFlow: 'child', chainRetry: { maxAttempts: 3, delayMs: 10, maxDelayMs: 50 } },
            ] },
            child: { revision, inputs: ['event'], evaluator: 'e', template: { $path: '$' } },
        } };
}

test('downstream changes isolate parent claims and cycle fingerprints are deterministic', async () => {
    const store = createMemoryStore(); const oldOptions = configuration(store); const newOptions = configuration(store, '2');
    const original = createRunner(oldOptions); const next = createRunner(newOptions);
    try {
        const admitted = await original.submit({ flow: 'parent', kind: 'result', result: 1 });
        assert.notEqual((await original.ready).parent.runtimeRevision, (await next.ready).parent.runtimeRevision);
        assert.equal(await next.tick(), 0);
        const executed = await original.execute(admitted.taskId);
        const child = await original.execute(executed.chains[0].childTaskId);
        assert.equal(child.flowRevision, '1'); assert.equal(child.evaluation.result, 1);
        const cyclic = configuration(store);
        cyclic.flows.parent.inputs.push('event'); cyclic.flows.parent.evaluator = 'e'; cyclic.flows.parent.template = { $path: '$' };
        cyclic.flows.child.connections = [{ id: 'back', connector: 'sink', when: 'always', payload: 'raw-result', nextFlow: 'parent', chainRetry: { maxAttempts: 3, delayMs: 10, maxDelayMs: 50 } }];
        const first = await compileFlows(cyclic);
        const second = await compileFlows({ ...cyclic, flows: { child: cyclic.flows.child, parent: cyclic.flows.parent } });
        assert.equal(first.parent.runtimeRevision, second.parent.runtimeRevision);
        assert.equal(first.child.runtimeRevision, second.child.runtimeRevision);
        assert.notEqual(first.parent.runtimeRevision, first.child.runtimeRevision);
    } finally { await original.close(); await next.close(); await store.close(); }
});

test('aggregation retains its owning generation and only that generation can flush its evidence', async () => {
    const store = createMemoryStore(); const original = createRunner(configuration(store)); const next = createRunner(configuration(store, '2'));
    const rule = { id: 'pair', revision: '1', sources: ['one', 'two'], windowMs: 80, incomplete: 'emit', flow: 'child' };
    const before = createDurableAggregator({ runner: original, rule }); const after = createDurableAggregator({ runner: next, rule });
    try {
        await before.add({ source: 'one', eventId: 'one', correlationKey: 'case', event: 1 });
        assert.equal(await original.pending(), 1); assert.equal(await next.pending(), 0);
        await sleep(100);
        assert.deepEqual(await after.flush(), []); assert.equal((await next.list()).items.length, 0);
        await before.flush();
        const [task] = (await original.list()).items;
        assert.equal(task.flowRevision, '1'); assert.equal(task.evaluation.status, 'pending');
        assert.equal(await next.tick(), 0);
        await original.execute(task.taskId); assert.equal(await original.pending(), 0);
        assert.equal((await original.list()).items.length, 1);
    } finally { await original.close(); await next.close(); await store.close(); }
});

test('configured memory storage enforces top-level resource limits', async () => {
    const runner = await createConfiguredRunner({ schemaVersion: 3, limits: { maxPending: 1 },
        validators: { v: { type: 'schema', options: { schema: {} } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    try {
        await runner.submit({ flow: 'a', kind: 'result', result: 1 });
        await assert.rejects(runner.submit({ flow: 'a', kind: 'result', result: 2 }), { code: 'RESOURCE_LIMIT' });
    } finally { await runner.close(); }
});

test('service startup and reload honor configured host roles with explicit options taking precedence', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-host-config-'));
    const config = role => ({ schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } }, host: { role, pollMs: 5 },
        validators: { v: { type: 'schema', options: { schema: {} } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    const service = await createService(config('ingress'), { baseDir: directory });
    let override;
    try {
        assert.equal((await service.health()).role, 'ingress');
        const task = await service.runner.submit({ flow: 'a', kind: 'result', result: 1 });
        await sleep(30); assert.equal((await service.runner.get(task.taskId)).status, 'running');
        await service.reload(config('worker')); assert.equal((await service.health()).role, 'worker');
        for (let i = 0; i < 100 && (await service.runner.get(task.taskId)).status !== 'completed'; i++) await sleep(10);
        assert.equal((await service.runner.get(task.taskId)).status, 'completed');
        override = await createService(config('ingress'), { baseDir: directory, role: 'worker' });
        assert.equal((await override.health()).role, 'worker');
    } finally { await override?.close(); await service.close(); await rm(directory, { recursive: true }); }
});

test('service rejects invalid generation limits before acquiring storage or hosts', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-generation-limits-'));
    const config = { schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } }, host: { role: 'ingress' },
        validators: { v: { type: 'schema', options: { schema: {} } } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
    try {
        for (const maxGenerations of [0, -1, 1.5, NaN, Infinity, -Infinity, Number.MAX_SAFE_INTEGER + 1, '2', null, false])
            await assert.rejects(createService(config, { baseDir: directory, maxGenerations }),
                error => error.code === 'CONFIGURATION_ERROR' && error.message.includes('maxGenerations'));
        await assert.rejects(stat(join(directory, 'state.sqlite')), { code: 'ENOENT' });
        const service = await createService(config, { baseDir: directory, maxGenerations: 1 });
        try { assert.equal((await service.health()).ready, true); } finally { await service.close(); }
    } finally { await rm(directory, { recursive: true }); }
});

test('finite service generation limits retain pending work and allow reload after reaping', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-generation-cap-'));
    const config = revision => ({ schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } }, host: { role: 'worker' },
        validators: { v: { type: 'schema', options: { schema: {} } } }, flows: { a: { revision, inputs: ['result'], validator: 'v' } } });
    const service = await createService(config('1'), { baseDir: directory, maxGenerations: 2 });
    try {
        await service.runner.pause('flow', 'a', true);
        const old = await service.runner.submit({ flow: 'a', kind: 'result', result: 1 });
        await service.reload(config('2'));
        assert.equal((await service.health()).retainedGenerations, 1);
        const current = service.runner;
        await assert.rejects(service.reload(config('3')), { code: 'RESOURCE_LIMIT' });
        assert.equal(service.runner, current); assert.equal((await service.health()).ready, true);
        await service.runner.cancel(old.taskId);
        assert.equal((await service.health()).retainedGenerations, 0);
        await service.reload(config('3'));
        assert.notEqual(service.runner, current); assert.equal((await service.health()).ready, true);
    } finally { await service.close(); await rm(directory, { recursive: true }); }
});
