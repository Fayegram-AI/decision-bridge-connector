import test from 'node:test';
import assert from 'node:assert/strict';
import { createRegistry } from '../src/index.mjs';
import { createConfiguredRunner } from './helpers/runner.mjs';
import { createConfiguredRedisConnector } from '../src/node/redis-connector.mjs';
import { createHost } from '../src/node/host.mjs';
import { createResourceOwner } from '../src/node/resources.mjs';

const configuration = { schemaVersion: 3, validators: { v: { type: 'schema', options: { schema: {} } } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };

test('configured aggregation bindings reject inherited target flow names at startup', async () => {
    for (const flow of ['constructor', 'toString', 'hasOwnProperty']) await assert.rejects(createConfiguredRunner({
        ...configuration, aggregations: { invalid: { revision: '1', sources: ['one'], flow, windowMs: 1000 } },
    }), { code: 'CONFIGURATION_ERROR' });
});

test('configured startup closes a factory resource that arrives after timeout', async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let closes = 0;
    const registry = createRegistry().register('outgoing', 'late', { apiVersion: 1, revision: '1', schema: {},
        create(options, { signal }) { entered.resolve(signal); return release.promise; } });
    const startup = createConfiguredRunner({ ...configuration, connectors: { late: { type: 'late' } } }, { registry });
    const rejected = assert.rejects(startup, { code: 'TIMEOUT' });
    try {
        const signal = await entered.promise; t.mock.timers.tick(60001); await rejected;
        assert.equal(signal.aborted, true);
        release.resolve(Object.freeze({ close() { closes++; } }));
        await new Promise(resolve => setImmediate(resolve));
        assert.equal(closes, 1);
    } finally { release.resolve({ close() {} }); await startup.catch(() => {}); }
});

test('cleanup wait limits do not settle or restart the shared resource close promise', async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const release = Promise.withResolvers(); let closes = 0; let settled = false;
    const owner = createResourceOwner();
    const resource = await owner.create(() => ({ close() { closes++; return release.promise; } }));
    const closed = owner.close(resource); closed.then(() => { settled = true; });
    try {
        const cleanup = assert.rejects(owner.closeAll(), AggregateError); t.mock.timers.tick(10001); await cleanup;
        assert.equal(settled, false); assert.equal(closes, 1);
        assert.equal(owner.close(resource), closed);
        release.resolve(); await closed; assert.equal(settled, true); assert.equal(closes, 1);
    } finally { release.resolve(); await closed; }
});

test('configured source methods keep their receiver and shutdown closes the source once', async () => {
    const opened = Promise.withResolvers(); const read = Promise.withResolvers(); const release = Promise.withResolvers(); let closes = 0;
    class Source {
        #name = 'class-source';
        get name() { return this.#name; }
        get revision() { return '1'; }
        get capabilities() { return { direction: 'incoming', runtime: 'portable', acknowledgement: 'none' }; }
        async *open() { assert.equal(this.#name, 'class-source'); opened.resolve(); await read.promise; }
        async close(options) {
            assert.equal(this.#name, 'class-source'); assert(options.signal instanceof AbortSignal);
            assert.equal(options.timeoutMs, 1000); closes++; read.resolve(); await release.promise;
        }
    }
    const registry = createRegistry().register('incoming', 'owned-source', { apiVersion: 1, revision: '1', schema: {}, create: () => new Source() });
    const runner = await createConfiguredRunner({ ...configuration, incoming: { source: { type: 'owned-source' } } }, { registry });
    let host;
    try {
        assert.equal(runner.sources.source.name, 'class-source');
        host = await createHost({ runner, role: 'ingress', drainMs: 1000 }); await opened.promise;
        const stop = host.stopIngress(); const closing = host.close();
        await new Promise(resolve => setImmediate(resolve)); assert.equal(closes, 1);
        release.resolve(); await Promise.all([stop, closing]); await runner.close();
        assert.equal(closes, 1);
    } finally { release.resolve(); read.resolve(); await host?.close(); await runner.close(); }
});

test('configured classes retain capability, operation and provenance getters with their receiver', async () => {
    let closes = 0; let sends = 0;
    class Connector {
        #name = 'local-class';
        get name() { return this.#name; }
        get capabilities() { return { direction: 'outgoing', runtime: 'node', acknowledgement: 'accepted', local: true, idempotent: true }; }
        send() { assert.equal(this.#name, 'local-class'); sends++; return { status: 'acknowledged' }; }
        close() { assert.equal(this.#name, 'local-class'); closes++; }
    }
    class Evaluator {
        #operation = 'extraction';
        get operation() { return this.#operation; }
        execute() { assert.equal(this.#operation, 'extraction'); return 42; }
    }
    class Validator {
        #provenance = 'verified';
        get provenance() { return this.#provenance; }
        validate(value) { assert.equal(this.#provenance, 'verified'); return value; }
    }
    const registry = () => createRegistry()
        .register('outgoing', 'class-sink', { apiVersion: 1, revision: '1', schema: {}, create: () => new Connector() })
        .register('evaluator', 'class-evaluator', { apiVersion: 1, revision: '1', schema: {}, create: () => new Evaluator() })
        .register('validator', 'class-validator', { apiVersion: 1, revision: '1', schema: {}, create: () => new Validator() });
    const config = { ...configuration, placements: ['local'],
        connectors: { sink: { type: 'class-sink' } }, evaluators: { e: { type: 'class-evaluator' } }, validators: { v: { type: 'class-validator' } },
        flows: { a: { revision: '1', operation: 'extraction', inputs: ['request', 'result'], evaluator: 'e', validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', retry: { ambiguous: 'retry' } },
        ] } } };
    await assert.rejects(createConfiguredRunner(config, { registry: registry() }), /placement/);
    assert.equal(closes, 1); assert.equal(sends, 0);
    config.flows.a.connections[0].placement = 'local';
    await assert.rejects(createConfiguredRunner({ ...config, flows: { a: { ...config.flows.a, operation: 'decision' } } },
        { registry: registry() }), /operation/);
    assert.equal(closes, 2); assert.equal(sends, 0);
    const runner = await createConfiguredRunner(config, { registry: registry() });
    try {
        const request = await runner.run({ flow: 'a', kind: 'request', request: {} });
        assert.equal(request.status, 'completed'); assert.equal(request.evaluation.result, 42);
        const result = await runner.run({ flow: 'a', kind: 'result', result: 7 });
        assert.equal(result.status, 'completed'); assert.equal(sends, 2);
        assert.equal((await runner.store.get(result.taskId, runner.namespace)).task.validation.provenance, 'verified');
    } finally { await runner.close(); }
    assert.equal(closes, 3);
});

test('configured Redis construction cleans failures and transfers successful client ownership', async () => {
    for (const mode of ['connect-failure', 'invalid-settings', 'cancelled', 'success']) {
        const controller = new AbortController(); const entered = Promise.withResolvers(); const release = Promise.withResolvers();
        const failure = new Error('Connection failed.'); let destroys = 0; let closes = 0;
        const client = { isOpen: false, on() {},
            async connect() {
                this.isOpen = true; entered.resolve();
                if (mode === 'connect-failure') throw failure;
                if (mode === 'cancelled') await release.promise;
            },
            destroy() { destroys++; this.isOpen = false; release.resolve(); },
            async close() { closes++; this.isOpen = false; },
            async sendCommand() { return 'entry-id'; },
        };
        const pending = createConfiguredRedisConnector({ url: 'redis://test', ...(mode === 'invalid-settings' ? {} : { stream: 'events' }) },
            { signal: controller.signal }, () => client);
        if (mode === 'cancelled') { await entered.promise; controller.abort(); }
        if (mode === 'success') {
            const connector = await pending; controller.abort(); assert.equal(destroys, 0);
            await connector.close(); assert.equal(closes, 1); assert.equal(client.isOpen, false);
        } else {
            await assert.rejects(pending, error => mode === 'connect-failure' ? error === failure
                : error.code === (mode === 'cancelled' ? 'ABORTED' : 'CONFIGURATION_ERROR'));
            assert.equal(destroys, 1); assert.equal(client.isOpen, false);
        }
    }
});
