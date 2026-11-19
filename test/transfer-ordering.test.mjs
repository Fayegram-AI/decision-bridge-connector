import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import { createRunner } from '../src/engine/runner.mjs';

const compare = new Intl.Collator('en-u-kn-true').compare;
const settings = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
const input = { flow: 'a', kind: 'result', result: 'payload_59', validation: 'payload_121' };
const encode = frame => new TextEncoder().encode(JSON.stringify(frame) + '\n');
const hashes = frames => frames.filter(frame => frame.type === 'chunk' && frame.index === 0).map(frame => frame.sha256);

/** Reproduce a database whose string range predicates and ordering use numeric collation. */
function numericRepository() {
    const raw = createMemoryRepository();
    return { ...raw, transaction: callback => raw.transaction(tx => callback({ ...tx,
        async find(table, where = {}, query = {}) {
            if (table !== 'records' || where.kind !== 'export-blob' || query.order?.[0] !== 'rootId')
                return tx.find(table, where, query);
            const { rootId, ...rest } = where;
            const rows = await tx.find(table, rest, { limit: 1000 });
            return rows.filter(row => rootId?.gt === undefined || compare(row.rootId, rootId.gt) > 0)
                .sort((a, b) => compare(a.rootId, b.rootId)).slice(0, query.limit ?? 100);
        },
    })) };
}

const stores = [['numeric repository', () => createExecutionStore(numericRepository())],
    ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];

for (const [name, factory] of stores) test(`${name}: export hash ordering is portable across repository collations`, async () => {
    const store = await factory();
    const runner = createRunner({ ...settings, store, namespace: `export-order-${crypto.randomUUID()}` });
    try {
        const original = await runner.run(input);
        const frames = [];
        for await (const chunk of runner.exportTask(original.taskId)) frames.push(JSON.parse(new TextDecoder().decode(chunk)));
        const ordered = hashes(frames);
        assert(ordered.length >= 2);
        assert.deepEqual(ordered, [...ordered].sort());
        const imported = await runner.importTask(frames.map(encode), { flow: 'a' });
        const saved = await store.get(imported.taskId, runner.namespace);
        assert.equal(saved.task.result, input.result);
        assert.equal(saved.task.input.validation, input.validation);
    } finally { await runner.close(); await store.close(); }
});

test('existing export cursors retain saved frames while value ordering metadata is upgraded', async () => {
    const raw = createMemoryRepository(); const store = createExecutionStore(raw);
    const runner = createRunner({ ...settings, store });
    let lease;
    try {
        const original = await runner.run(input); lease = await runner.openSnapshot(original.taskId);
        const frames = []; let after; let beforeChunk;
        do {
            beforeChunk = after;
            const page = await runner.readExportFrames(lease.id, { after });
            frames.push(...page.frames); after = page.next;
        } while (frames.at(-1).type !== 'chunk');
        const prefix = frames.at(-1);
        // Earlier schema-v3 sessions have saved frames and a hash cursor, but no ordinals.
        await raw.transaction(async tx => {
            const owner = await tx.get('owners', lease.id);
            const { blobOrderVersion: _version, ...exportState } = owner.exportState;
            await tx.put('owners', { ...owner, exportState });
            for (const row of await tx.find('records', { ownerId: lease.id, kind: 'export-blob' }, { limit: 1000 })) {
                const { ordinal: _ordinal, ...previous } = row;
                await tx.put('records', previous);
            }
        });
        assert.deepEqual((await runner.readExportFrames(lease.id, { after: beforeChunk })).frames, [prefix]);
        do {
            const page = await runner.readExportFrames(lease.id, { after, limit: 8 });
            frames.push(...page.frames); after = page.next;
        } while (after);
        assert.deepEqual(hashes(frames), [...hashes(frames)].sort());
        const imported = await runner.importTask(frames.map(encode), { flow: 'a' });
        assert.equal((await store.get(imported.taskId, runner.namespace)).task.result, input.result);
    } finally { if (lease) await runner.closeSnapshot(lease.id); await runner.close(); await store.close(); }
});
