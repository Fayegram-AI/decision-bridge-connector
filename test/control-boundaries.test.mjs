import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector, createRegistry } from '../src/index.mjs';
import { createRunner, createConfiguredRunner } from './helpers/runner.mjs';

import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];
const flows = { a: { revision: '1', inputs: ['result'], validator: 'v' } };
const validators = { v: { revision: '1', validate: value => value } };

test('runner identity and placement settings reject malformed values before resource construction', async () => {
    const invalid = [
        ...[null, 0, '', 'bad\nname', 'x'.repeat(257)].flatMap(value => [{ namespace: value }, { workerId: value }] ),
        ...[null, false, 1, 'not-local', {}, [''], [1], ['local', 'local']].map(placements => ({ placements })),
    ];
    let resources = 0;
    const registry = () => createRegistry().register('outgoing', 'probe', { apiVersion: 1, revision: '1', schema: {},
        create() { resources++; return { close() {} }; } });
    for (const identity of invalid) {
        let storeRead = false;
        assert.throws(() => createRunner({ ...identity, flows, validators,
            get store() { storeRead = true; return createMemoryStore(); } }), { code: 'CONFIGURATION_ERROR' });
        assert.equal(storeRead, false);
        await assert.rejects(createConfiguredRunner({ schemaVersion: 3, ...identity, flows,
            validators: { v: { type: 'schema', options: { schema: {} } } }, connectors: { probe: { type: 'probe' } } },
        { registry: registry() }), { code: 'CONFIGURATION_ERROR' });
        assert.equal(resources, 0);
    }
    for (const placement of [null, '', 1, {}, []]) {
        const runner = createRunner({ validators, connectors: { sink: createCallbackConnector(async () => ({ status: 'acknowledged' })) },
            flows: { a: { ...flows.a, connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', placement }] } } });
        try { await assert.rejects(runner.ready, { code: 'CONFIGURATION_ERROR' }); } finally { await runner.close(); }
    }
});

for (const [name, factory] of stores) test(`${name}: worker labels remain fixed and claims require exact placement membership`, async () => {
    const store = await factory(); const placements = ['not-local']; let sends = 0;
    const options = { store, namespace: `placement-${crypto.randomUUID()}`, placements, validators,
        connectors: { sink: { name: 'local', revision: '1', capabilities: { direction: 'outgoing', runtime: 'node', local: true },
            send() { sends++; return { status: 'acknowledged' }; } } },
        flows: { a: { ...flows.a, connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', placement: 'local' }] } } };
    const original = createRunner(options); let local;
    try {
        const task = await original.submit({ flow: 'a', kind: 'result', result: 42 });
        await bounded(async () => {
            while ((await original.get(task.taskId)).deliveries[0].status === 'blocked') { await original.tick(); await sleep(1); }
        }, { timeoutMs: 5000 });
        placements.push('local'); options.placements = ['local'];
        assert.equal(await original.tick({ taskId: task.taskId }), 0); assert.equal(sends, 0);
        for (const invalid of ['not-local', null, [1], ['local', 'local']]) await assert.rejects(
            store.claim(original.namespace, 'delivery', { worker: 'invalid', placements: invalid, taskId: task.taskId }), { code: 'CONFIGURATION_ERROR' });
        assert.equal((await original.get(task.taskId)).deliveries[0].attempts.length, 0);
        local = createRunner({ ...options, placements: ['local'] });
        assert.equal((await local.execute(task.taskId)).status, 'completed'); assert.equal(sends, 1);
    } finally { await original.close(); await local?.close(); await store.close(); }
});

for (const [name, factory] of stores) test(`${name}: pause rejects invalid targets and retains flow and destination scope`, async () => {
    const store = await factory(); let sends = 0;
    const runner = createRunner({ store, namespace: `pause-${crypto.randomUUID()}`, validators,
        connectors: { sink: createCallbackConnector(async () => { sends++; return { status: 'acknowledged' }; }) },
        flows: { a: { ...flows.a, connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] }, b: flows.a } });
    try {
        for (const type of ['flow', 'destination']) for (const target of [undefined, null, '', 1, {}, 'bad\nname', 'x'.repeat(257)])
            await assert.rejects(runner.pause(type, target), { code: 'CONFIGURATION_ERROR' });
        await runner.pause('destination', 'undefined');
        await runner.pause('flow', 'a');
        const a = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
        assert.equal(await runner.tick({ taskId: a.taskId }), 0);
        assert.equal((await runner.run({ flow: 'b', kind: 'result', result: 2 })).status, 'completed');
        const destination = (await runner.ready).a.connections[0].destinationKey;
        await runner.pause('destination', destination); await runner.pause('flow', 'a', false);
        await bounded(async () => {
            while ((await runner.get(a.taskId)).deliveries[0].status === 'blocked') { await runner.tick({ taskId: a.taskId }); await sleep(1); }
        }, { timeoutMs: 5000 });
        assert.equal(await runner.tick({ taskId: a.taskId }), 0); assert.equal(sends, 0);
        await runner.pause('destination', destination, false);
        assert.equal((await runner.execute(a.taskId)).status, 'completed'); assert.equal(sends, 1);
    } finally { await runner.close(); await store.close(); }
});

for (const [name, factory] of stores) test(`${name}: destination pause cannot stop destinationless job kinds`, async () => {
    const store = await factory(); const namespace = `pause-kinds-${crypto.randomUUID()}`;
    try {
        for (const [index, kind] of ['evaluation', 'failure', 'chain', 'delivery'].entries()) {
            const taskId = `task-${namespace}-${index}`;
            await store.admit({ id: taskId, namespace, flow: 'a', state: 'active', rootId: taskId,
                plan: { concurrency: 10, maxExecutions: 1 } }, [{ id: `${taskId}:job`, taskId, namespace, flow: 'a', kind,
                state: 'pending', createdAt: 0, runAt: 0, priority: 0, required: 1, expiresAt: Date.now() + 60000,
                attempts: [], plan: { dependsOn: [] }, ...(kind === 'delivery' ? { destination: 'undefined' } : {}) }]);
        }
        await store.pause(namespace, 'destination', 'undefined', true);
        for (const kind of ['evaluation', 'failure', 'chain']) assert(await store.claim(namespace, kind, { worker: 'unrelated' }), kind);
        assert.equal(await store.claim(namespace, 'delivery', { worker: 'recipient' }), null);
        await store.pause(namespace, 'destination', 'undefined', false);
        assert(await store.claim(namespace, 'delivery', { worker: 'recipient' }));
    } finally { await store.close(); }
});
