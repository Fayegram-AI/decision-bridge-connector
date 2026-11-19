import test from 'node:test';
import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createService } from '../src/node/index.mjs';
import { bounded } from '../src/util.mjs';

const base = { schemaVersion: 3, host: { role: 'ingress', drainMs: 3000 },
    store: { type: 'sqlite', options: { path: 'state.sqlite' } },
    validators: { v: { type: 'schema', options: { schema: {} } } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
const http = port => ({ type: 'http', options: { flow: 'a', port } });
async function bind() {
    const server = createServer().listen(0, '127.0.0.1');
    await once(server, 'listening');
    return server;
}
const close = server => new Promise(resolve => server.close(resolve));
const url = (port, path) => `http://127.0.0.1:${port}${path}`;
// A cutover closes the old TCP connection. Probe each listener with a fresh connection.
const submit = port => fetch(url(port, '/tasks'), { method: 'POST', signal: AbortSignal.timeout(3000),
    headers: { connection: 'close' }, body: JSON.stringify({ kind: 'result', result: 42 }) });

test('failed replacement gates HTTP admission and restores a suspended same-address listener', async () => {
    const probe = await bind(); const { port } = probe.address();
    const mcpProbe = await bind(); const mcpPort = mcpProbe.address().port;
    await Promise.all([close(probe), close(mcpProbe)]);
    const directory = await mkdtemp(join(tmpdir(), 'dbc-reload-activate-'));
    const key = crypto.randomUUID();
    const state = globalThis[key] = { started: Promise.withResolvers(), fail: Promise.withResolvers() };
    const config = { ...base, incoming: { http: http(port) } };
    let service;
    try {
        await writeFile(join(directory, 'failure.mjs'), `
const state = globalThis[${JSON.stringify(key)}];
export default { apiVersion: 1, revision: '1', schema: { type: 'object' }, create() {
    return { async start({ runner, whenActive }) {
        whenActive.catch(() => {});
        try { await runner.submit({ flow: 'a', kind: 'result', result: 1 }); }
        catch (error) { state.code = error.code; }
        state.started.resolve();
        await state.fail.promise;
        throw new Error('Replacement source failed.');
    } };
} };
`);
        service = await createService(config, { baseDir: directory });
        const previous = service.runner;
        const reload = service.reload({ ...config, flows: { a: { ...config.flows.a, revision: '2' } },
            extensions: [{ kind: 'incoming', name: 'failure', module: 'failure.mjs' }],
            incoming: { ...config.incoming, mcp: { type: 'mcp', options: { transport: 'http', flow: 'a', port: mcpPort } },
                failure: { type: 'failure' } } });
        void reload.catch(() => {});
        await bounded(() => state.started.promise, { timeoutMs: 4000 });
        assert.equal(state.code, 'HOST_NOT_ACTIVE');
        const staged = await submit(port);
        assert.equal(staged.status, 503);
        assert.equal((await staged.json()).error.code, 'HOST_NOT_ACTIVE');
        const stagedMcp = await fetch(url(mcpPort, '/'), { signal: AbortSignal.timeout(3000) });
        assert.equal(stagedMcp.status, 503);
        await stagedMcp.arrayBuffer();
        state.fail.resolve();
        await assert.rejects(reload, /Replacement source failed/);
        assert.equal(service.runner, previous);
        assert.equal((await service.health()).ready, true);
        assert.deepEqual((await previous.list()).items, []);
        const restored = await submit(port);
        assert.equal(restored.status, 202);
        assert.equal((await restored.json()).value.flowRevision, '1');
        await service.reload({ ...config, flows: { a: { ...config.flows.a, revision: '2' } } });
        const activated = await submit(port);
        assert.equal(activated.status, 202);
        assert.equal((await activated.json()).value.flowRevision, '2');
    } finally {
        state.fail.resolve();
        await service?.close();
        delete globalThis[key];
        await rm(directory, { recursive: true, force: true });
    }
});

test('an occupied replacement port leaves the previous listener accepting work', async () => {
    const probe = await bind(); const { port } = probe.address(); await close(probe);
    const blocker = await bind();
    const directory = await mkdtemp(join(tmpdir(), 'dbc-reload-occupied-'));
    const config = { ...base, incoming: { http: http(port) } };
    let service;
    try {
        service = await createService(config, { baseDir: directory });
        await assert.rejects(service.reload({ ...config, incoming: { http: http(blocker.address().port) } }), { code: 'EADDRINUSE' });
        assert.equal((await service.health()).ready, true);
        const admitted = await submit(port);
        assert.equal(admitted.status, 202);
        await admitted.arrayBuffer();
    } finally { await service?.close(); await close(blocker); await rm(directory, { recursive: true, force: true }); }
});

test('a programmatic aggregation write pins its generation until the saved group drains', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-reload-aggregate-'));
    const key = crypto.randomUUID();
    const state = globalThis[key] = { entered: Promise.withResolvers(), resume: Promise.withResolvers(), finished: Promise.withResolvers() };
    const sqlite = new URL('../src/storage/sqlite.mjs', import.meta.url).href;
    let service;
    try {
        await writeFile(join(directory, 'fixture.mjs'), `
import { createSQLiteStore } from ${JSON.stringify(sqlite)};
import { join } from 'node:path';
const state = globalThis[${JSON.stringify(key)}];
let generation = 0;
export const store = { apiVersion: 1, revision: '1', schema: { type: 'object' }, create(_settings, { baseDir }) {
    const saved = createSQLiteStore(join(baseDir, 'state.sqlite'));
    if (++generation !== 1) return saved;
    let first = true;
    return { ...saved, async aggregate(...args) {
        if (!first) return saved.aggregate(...args);
        first = false; state.entered.resolve();
        await state.resume.promise;
        try { return await saved.aggregate(...args); } finally { state.finished.resolve(); }
    }, async close() { await state.finished.promise; await saved.close(); } };
} };
export const evaluator = { apiVersion: 1, revision: '1', schema: { type: 'object' },
    create() { return { execute: value => value }; } };
`);
        const config = { ...base, host: { role: 'all', pollMs: 5 }, store: { type: 'fixture' },
            extensions: [{ kind: 'store', name: 'fixture', module: 'fixture.mjs', export: 'store' },
                { kind: 'evaluator', name: 'echo', module: 'fixture.mjs', export: 'evaluator' }],
            evaluators: { e: { type: 'echo' } }, flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } },
            aggregations: { g: { revision: '1', flow: 'a', sources: ['one', 'two'], windowMs: 60000 } } };
        service = await createService(config, { baseDir: directory });
        const old = service.runner;
        const add = old.aggregators.g.add({ source: 'one', eventId: 'one', correlationKey: 'key', event: 1 });
        void add.catch(() => {});
        await bounded(() => state.entered.promise, { timeoutMs: 3000 });
        await service.reload({ ...config, aggregations: {} });
        assert.equal((await bounded(() => service.health(), { timeoutMs: 3000 })).retainedGenerations, 1);
        state.resume.resolve();
        const group = await add;
        assert.equal(group.state, 'pending');
        assert.equal((await service.health()).retainedGenerations, 1);
        await service.runner.store.aggregate(service.runner.namespace, group.key, current => ({ ...current, expiresAt: 0 }));
        await old.aggregators.g.flush();
        assert.equal((await service.health()).retainedGenerations, 0);
    } finally {
        state.resume.resolve(); state.finished.resolve();
        await service?.close();
        delete globalThis[key];
        await rm(directory, { recursive: true, force: true });
    }
});
