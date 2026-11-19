import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore } from '../src/index.mjs';
import { createRunner, collectExport } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { bounded } from '../src/util.mjs';

const stores = [['memory', () => createMemoryStore()], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

// A real digest may yield while application code still owns the parsed export.
for (const [name, factory] of stores) test(`${name}: import captures each frame before asynchronous integrity verification`, async t => {
    const store = await factory();
    const runner = createRunner({ store, namespace: `import-integrity-${crypto.randomUUID()}`,
        validators: { v: { revision: '1', validate: value => value } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let importing;
    try {
        const original = await runner.run({ flow: 'a', kind: 'result', result: { value: 'verified' },
            validation: { attestation: 'original' }, evidenceAt: '2026-01-01T00:00:00Z' });
        const chunks = await collectExport(runner, original.taskId);
        const exported = chunks.map(chunk => JSON.parse(new TextDecoder().decode(chunk)));
        const checksum = exported.at(-1).checksum;
        const lease = await runner.openImport({ flow: 'a' });
        const digest = crypto.subtle.digest.bind(crypto.subtle); let first = true;
        t.mock.method(crypto.subtle, 'digest', async (...args) => {
            const result = await digest(...args);
            if (first) { first = false; entered.resolve(); await release.promise; }
            return result;
        });
        importing = runner.appendImport(lease.id, exported[0]); importing.catch(() => {});
        await bounded(() => entered.promise, { timeoutMs: 5000 });
        exported[0].schemaVersion = 999; exported[0].seq = 999;
        release.resolve(); await importing;
        for (const frame of exported.slice(1)) await runner.appendImport(lease.id, frame);
        const imported = await runner.finishImport(lease.id); await runner.closeImport(lease.id);
        assert.deepEqual((await runner.get(imported.taskId)).evaluation.result, original.evaluation.result);
        const saved = await store.get(imported.taskId, runner.namespace);
        assert.deepEqual(saved.task.input.validation, { attestation: 'original' });
        assert.equal(saved.task.evidenceAt, '2026-01-01T00:00:00Z');
        assert.equal(saved.task.input.idempotencyKey, `import:${checksum}`);
        const replay = await runner.importTask(chunks, { flow: 'a' });
        assert.equal(replay.taskId, imported.taskId);
        assert.equal((await runner.list()).items.length, 2);
    } finally { release.resolve(); await importing?.catch(() => {}); await runner.close(); await store.close(); }
});
