import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createService } from '../src/node/service.mjs';

const configuration = store => ({ schemaVersion: 3, namespace: `reload-store-${crypto.randomUUID()}`, store,
    host: { role: 'ingress' }, validators: { v: { type: 'schema', options: { schema: {} } } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });

test('service reload accepts reordered storage declarations and rejects resolved environment retargets', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-storage-reload-')); const env = { STATE_PATH: 'first.sqlite' };
    const config = configuration({ type: 'sqlite', options: { path: { $env: 'STATE_PATH' } } });
    const service = await createService(config, { baseDir: directory, env });
    try {
        const task = await service.runner.submit({ flow: 'a', kind: 'result', result: 1 });
        await service.reload({ ...config, store: { options: { path: { $env: 'STATE_PATH' } }, type: 'sqlite' } });
        assert(await service.runner.get(task.taskId));
        const retained = service.runner; env.STATE_PATH = 'second.sqlite';
        await assert.rejects(service.reload(config), { code: 'CONFIGURATION_ERROR' });
        assert.equal(service.runner, retained); assert(await service.runner.get(task.taskId));
        assert.equal((await service.health()).ready, true);
    } finally { await service.close(); await rm(directory, { recursive: true, force: true }); }
});

test('service reload rejects a changed custom store factory revision', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-factory-reload-')); const module = join(directory, 'store.mjs');
    const source = revision => `import { createSQLiteStore } from ${JSON.stringify(new URL('../src/storage/sqlite.mjs', import.meta.url).href)};
import { resolve } from 'node:path';
export default { apiVersion: 1, revision: '${revision}', schema: { type: 'object' },
    create(settings, { baseDir }) { return createSQLiteStore(resolve(baseDir, settings.path)); } };
`;
    let service;
    try {
        await writeFile(module, source('1'));
        const config = { ...configuration({ type: 'custom', options: { path: 'state.sqlite' } }),
            extensions: [{ kind: 'store', name: 'custom', module: 'store.mjs' }] };
        service = await createService(config, { baseDir: directory });
        const task = await service.runner.submit({ flow: 'a', kind: 'result', result: 1 }); const retained = service.runner;
        await writeFile(module, source('2'));
        await assert.rejects(service.reload(config), { code: 'CONFIGURATION_ERROR' });
        assert.equal(service.runner, retained); assert(await service.runner.get(task.taskId));
        assert.equal((await service.health()).ready, true);
    } finally { await service?.close(); await rm(directory, { recursive: true, force: true }); }
});

test('service reload rejects lower stored-value limits before cutover and accepts increases', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-value-limit-reload-'));
    const config = { ...configuration({ type: 'sqlite', options: { path: 'state.sqlite' } }), limits: { maxBytes: 8192 } };
    const service = await createService(config, { baseDir: directory });
    try {
        const value = 'x'.repeat(5000);
        const task = await service.runner.submit({ flow: 'a', kind: 'result', result: value });
        const retained = service.runner;
        await assert.rejects(service.reload({ ...config, limits: { maxBytes: 1024 } }),
            error => error.code === 'CONFIGURATION_ERROR' && error.message.includes('maxBytes'));
        assert.equal(service.runner, retained);
        assert.equal((await service.health()).ready, true);
        const original = await service.runner.execute(task.taskId, { report: { includeData: true, maxInlineBytes: 16384 } });
        assert.equal(original.status, 'completed');
        assert.equal(original.data.find(part => part.entity === 'task' && part.path.join('.') === 'result').value, value);

        await service.reload({ ...config, limits: { maxBytes: 16384, maxPending: 2 } });
        assert.equal(service.runner.store.limits.maxPending, 2);
        const larger = 'y'.repeat(9000);
        const current = await service.runner.run({ flow: 'a', kind: 'result', result: larger },
            { report: { includeData: true, maxInlineBytes: 32768 } });
        assert.equal(current.status, 'completed');
        assert.equal(current.data.find(part => part.entity === 'task' && part.path.join('.') === 'result').value, larger);
    } finally { await service.close(); await rm(directory, { recursive: true, force: true }); }
});

test('service reload detects PostgreSQL driver environment changes while preserving its current runner', { skip: !process.env.DBC_POSTGRES_URL, timeout: 30000 }, async () => {
    const previous = process.env.PGAPPNAME; let service;
    try {
        process.env.PGAPPNAME = `dbc-reload-first-${crypto.randomUUID()}`;
        const config = configuration({ type: 'postgres', options: { connectionString: process.env.DBC_POSTGRES_URL } });
        service = await createService(config);
        const task = await service.runner.submit({ flow: 'a', kind: 'result', result: 1 }); const retained = service.runner;
        process.env.PGAPPNAME = `dbc-reload-next-${crypto.randomUUID()}`;
        await assert.rejects(service.reload(config), { code: 'CONFIGURATION_ERROR' });
        assert.equal(service.runner, retained); assert(await service.runner.get(task.taskId));
        assert.equal((await service.health()).ready, true);
    } finally {
        if (previous === undefined) delete process.env.PGAPPNAME; else process.env.PGAPPNAME = previous;
        await service?.close();
    }
});
