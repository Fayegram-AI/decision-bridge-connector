import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { bounded, delay } from '../src/util.mjs';
import { createService } from '../src/node/service.mjs';

const base = {
    schemaVersion: 3,
    store: { type: 'sqlite', options: { path: 'state.sqlite' } },
    validators: { v: { type: 'schema', options: { schema: {} } } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } },
};

test('invalid replacement listener settings are rejected while the old listener still serves', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-reload-http-'));
    const socket = createServer();
    socket.listen(0, 'localhost');
    await once(socket, 'listening');
    const { address, port } = socket.address();
    await new Promise(resolve => socket.close(resolve));
    const incoming = { type: 'http', options: { flow: 'a', host: 'localhost', port, reply: 'admitted' } };
    const config = { ...base, host: { role: 'ingress' }, incoming: { http: incoming } };
    let service;
    try {
        service = await createService(config, { baseDir: directory });
        const url = `http://${address.includes(':') ? `[${address}]` : address}:${port}/health`;
        const check = async () => {
            const response = await fetch(url, { signal: AbortSignal.timeout(3000) });
            assert.equal(response.status, 200);
            await response.arrayBuffer();
        };
        await check();
        for (const invalid of [
            { http: { ...incoming, options: { ...incoming.options, maxBytes: 0 } } },
            { http: { ...incoming, options: { ...incoming.options, host: {}, authToken: 'test-only' } } },
            { http: incoming, other: { ...incoming, options: { ...incoming.options, port: String(port) } } },
            { http: incoming, other: { ...incoming, options: { ...incoming.options, host: address } } },
            { http: { ...incoming, options: { ...incoming.options, host: '127.0.0.1' } },
                other: { ...incoming, options: { ...incoming.options, host: '::ffff:127.0.0.1', authToken: 'test-only' } } },
            { http: incoming, mcp: { type: 'mcp', options: { flow: 'a', port, transport: 'http',
                host: address.includes(':') ? '::' : '0.0.0.0', authToken: 'test-only' } } },
        ]) {
            await assert.rejects(service.reload({ ...config, incoming: invalid }), { code: 'CONFIGURATION_ERROR' });
            assert.equal((await service.health()).ready, true);
            await check();
        }
    } finally { await service?.close(); await rm(directory, { recursive: true, force: true }); }
});

for (const mode of ['retry', 'legacy store retry', 'reconcile']) {
    test(`reload retains a worker for delivery ${mode} and reaps it after recovery`, async () => {
        const directory = await mkdtemp(join(tmpdir(), 'dbc-reload-recovery-'));
        let requests = 0; let service;
        const sink = createServer((_request, response) => {
            response.writeHead(++requests === 1 ? 400 : 200).end('42');
        });
        sink.listen(0, '127.0.0.1');
        try {
            await once(sink, 'listening');
            const nextFlow = mode === 'reconcile' ? { nextFlow: 'child', chainRetry: { maxAttempts: 1, delayMs: 1, maxDelayMs: 1 } } : {};
            const a = { ...base.flows.a, connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result',
                retry: { maxAttempts: mode === 'reconcile' ? 1 : 2 }, ...nextFlow }] };
            const config = { ...base, host: { pollMs: 5 }, flows: { a, ...(mode === 'reconcile' ? {
                child: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } },
            } : {}) }, connectors: { sink: { type: 'http', options: { url: `http://127.0.0.1:${sink.address().port}` } } } };
            if (mode === 'reconcile') {
                await writeFile(join(directory, 'echo.mjs'), `export default {
                    apiVersion: 1, revision: '1', schema: { type: 'object' },
                    create() { return { execute: value => value }; },
                };`);
                config.extensions = [{ kind: 'evaluator', name: 'echo', module: 'echo.mjs' }];
                config.evaluators = { e: { type: 'echo' } };
            } else if (mode === 'legacy store retry') {
                const sqlite = new URL('../src/storage/sqlite.mjs', import.meta.url).href;
                await writeFile(join(directory, 'legacy.mjs'), `
import { createSQLiteStore } from ${JSON.stringify(sqlite)};
import { join } from 'node:path';
export default { apiVersion: 1, revision: '1', schema: { type: 'object' }, create(_options, { baseDir }) {
    const store = createSQLiteStore(join(baseDir, 'state.sqlite'));
    return { ...store, pendingJobs(...args) {
        if (args.length !== 2) throw new Error('Unsupported pendingJobs arguments.');
        return store.pendingJobs(...args);
    } };
} };
`);
                config.extensions = [{ kind: 'store', name: 'legacy', module: 'legacy.mjs' }];
                config.store = { type: 'legacy' };
            }
            service = await createService(config, { baseDir: directory });
            const { taskId } = await service.runner.run({ flow: 'a', kind: 'result', result: 1 }, { waitFor: 'settled' });
            const saved = () => service.runner.store.get(taskId, service.runner.namespace);
            const delivery = (await saved()).jobs.find(job => job.kind === 'delivery');
            assert.equal(delivery.state, 'failed');
            await service.reload({ ...config, flows: { ...config.flows, a: { ...a, revision: '2' } } });
            assert.equal((await service.health()).retainedGenerations, 1);
            let response = 42;
            for (let depth = 0; depth < 100; depth++) response = { value: response };
            if (mode !== 'reconcile') await service.runner.retryDelivery(delivery.id);
            else await service.runner.resolveDelivery(delivery.id, { status: 'acknowledged', receipt: { response } });
            const jobs = await bounded(async signal => {
                while (true) {
                    const { jobs } = await saved();
                    if (jobs.every(job => job.state === 'acknowledged')) return jobs;
                    await delay(5, signal);
                }
            }, { timeoutMs: 3000 });
            assert.equal(requests, mode === 'reconcile' ? 1 : 2);
            if (mode === 'reconcile') {
                const { childTaskId } = jobs.find(job => job.kind === 'chain');
                assert.ok(childTaskId);
                const child = await service.runner.store.get(childTaskId, service.runner.namespace);
                assert.deepEqual(child.task.input.event, response);
            }
            assert.equal((await service.health()).retainedGenerations, 0);
        } finally { await service?.close(); await new Promise(resolve => sink.close(resolve)); await rm(directory, { recursive: true, force: true }); }
    });
}

for (const mode of ['validation', 'commit']) {
    test(`reload fences incoming ${mode} before retiring compatible workers`, async () => {
        const directory = await mkdtemp(join(tmpdir(), 'dbc-reload-admission-'));
        const key = `dbc-reload-${crypto.randomUUID()}`;
        const state = globalThis[key] = { mode, entered: Promise.withResolvers(), release: Promise.withResolvers(),
            finished: Promise.withResolvers(), rejected: Promise.withResolvers() };
        let service;
        try {
            const sqlite = new URL('../src/storage/sqlite.mjs', import.meta.url).href;
            await writeFile(join(directory, 'extensions.mjs'), `
import { createSQLiteStore } from ${JSON.stringify(sqlite)};
import { join } from 'node:path';
const state = globalThis[${JSON.stringify(key)}];
export const incoming = { apiVersion: 1, revision: '1', schema: { type: 'object' }, create() {
    return { async *open() { yield { input: { flow: 'a', kind: 'result', result: 42 },
        reject(error) { state.rejected.resolve(error); } }; } };
} };
export const validator = { apiVersion: 1, revision: '1', schema: { type: 'object' }, create() {
    return { async validate(value, { signal }) {
        if (state.mode === 'validation') {
            state.entered.resolve(signal);
            await state.release.promise;
            state.finished.resolve();
        }
        return value;
    } };
} };
export const store = { apiVersion: 1, revision: '1', schema: { type: 'object' }, create(_options, { baseDir }) {
    const saved = createSQLiteStore(join(baseDir, 'state.sqlite'));
    return { ...saved, async admit(task, ...args) {
        if (state.mode === 'commit' && task.source === 'input') {
            state.entered.resolve();
            await state.release.promise;
        }
        return saved.admit(task, ...args);
    } };
} };
`);
            const config = { ...base, store: { type: 'gated-store' }, host: { role: 'all', pollMs: 5, drainMs: 100 },
                extensions: [['incoming', 'incoming'], ['validator', 'validator'], ['store', 'gated-store']].map(([kind, name]) =>
                    ({ kind, name, module: 'extensions.mjs', export: kind })),
                validators: { v: { type: 'validator' } }, incoming: { input: { type: 'incoming' } } };
            service = await createService(config, { baseDir: directory });
            const signal = await bounded(() => state.entered.promise, { timeoutMs: 3000 });
            await service.reload({ ...config, incoming: {}, flows: { a: { ...config.flows.a, revision: '2' } } });
            if (mode === 'validation') {
                assert.equal(signal.aborted, true);
                assert.equal((await bounded(() => state.rejected.promise, { timeoutMs: 3000 })).code, 'ABORTED');
                state.release.resolve();
                await state.finished.promise;
                assert.deepEqual((await service.runner.list()).items, []);
            } else {
                assert.equal((await service.health()).retainedGenerations, 1);
                assert.deepEqual((await service.runner.list()).items, []);
                state.release.resolve();
                const task = await bounded(async signal => {
                    while (true) {
                        const [item] = (await service.runner.list()).items;
                        if (item?.status === 'completed') return item;
                        await delay(5, signal);
                    }
                }, { timeoutMs: 3000 });
                assert.equal(task.flowRevision, '1');
            }
            await bounded(async signal => {
                while ((await service.health()).retainedGenerations > 0) await delay(5, signal);
            }, { timeoutMs: 3000 });
        } finally {
            state.release.resolve();
            await service?.close();
            delete globalThis[key];
            await rm(directory, { recursive: true, force: true });
        }
    });
}
