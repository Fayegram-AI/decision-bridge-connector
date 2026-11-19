import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner } from '../src/engine/runner.mjs';
import { createCallbackConnector } from '../src/index.mjs';
import { createExecutionStore } from '../src/storage/store.mjs';
import { createMemoryRepository } from '../src/storage/memory.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const setup = store => ({ store, namespace: `v3-${crypto.randomUUID()}`, evaluators: { e: { revision: '1', execute: value => value } },
    validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e', validator: 'v' } } });
const stores = [['memory', async () => createExecutionStore(createMemoryRepository())], ['sqlite', () => createSQLiteStore(':memory:')],
    ...(process.env.DBC_POSTGRES_URL ? [['postgres', () => createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL })]] : [])];
async function parts(runner, snapshotId) {
    const items = []; let after;
    do { const page = await runner.listSnapshot(snapshotId, { collection: 'data', after }); items.push(...page.items); after = page.next; } while (after);
    return items;
}

for (const [name, factory] of stores) {
    test(`${name}: large values admit without composed-record limits and summaries remain bounded`, async () => {
        const store = await factory(); const runner = createRunner(setup(store));
        try {
            const request = { text: 'x'.repeat(520000) };
            const preview = await runner.preview({ flow: 'a', kind: 'request', request });
            await runner.closeSnapshot(preview.snapshot.id);
            const current = await runner.run({ flow: 'a', kind: 'request', request });
            assert.equal(current.status, 'completed'); assert.equal(current.schemaVersion, 3);
            assert.equal(current.evaluation.hasResult, true); assert(!Object.hasOwn(current.evaluation, 'result'));
            assert(JSON.stringify(current).length < 2000);
            const bounded = await runner.get(current.taskId, { includeData: true, maxInlineBytes: 1024 });
            assert.equal(bounded.status, 'completed'); assert.equal(bounded.dataComplete, false); assert(bounded.omittedCount >= 3);
            assert(JSON.stringify(bounded).length < 12000);
            const lease = await runner.openSnapshot(current.taskId); const entries = await parts(runner, lease.id);
            const part = entries.find(item => item.path.join('.') === 'result');
            assert.deepEqual(await runner.readValue(lease.id, part.id), request);
            const page = await runner.readSnapshot(lease.id, part.id);
            assert.equal(atob(page.data).length, 65536); assert.equal(page.nextOffset, 65536);
            await runner.closeSnapshot(lease.id);
            await assert.rejects(runner.readValue(lease.id, part.id), { code: 'TRANSFER_EXPIRED' });
        } finally { await runner.close(); await store.close(); }
    });
    test(`${name}: application $data properties remain opaque through storage and transfer`, async () => {
        const store = await factory(); const runner = createRunner(setup(store));
        const value = { $data: { id: 'not-a-storage-reference', sha256: '0'.repeat(64), bytes: 123, kind: 'json' }, nested: { $data: 42 } };
        try {
            const task = await runner.run({ flow: 'a', kind: 'result', result: value }); assert.equal(task.status, 'completed');
            const lease = await runner.openSnapshot(task.taskId); const part = (await parts(runner, lease.id)).find(item => item.path.join('.') === 'result');
            assert.deepEqual(await runner.readValue(lease.id, part.id), value); await runner.closeSnapshot(lease.id);
            const imported = await runner.importTask(runner.exportTask(task.taskId), { flow: 'a' });
            const copied = await runner.get(imported.taskId, { includeData: true, maxInlineBytes: 16384 });
            assert.deepEqual(copied.data.find(item => item.path.join('.') === 'result').value, value);
        } finally { await runner.close(); await store.close(); }
    });
    test(`${name}: policy names can use $data without colliding with reference metadata`, async () => {
        const store = await factory(); const options = setup(store);
        options.policies = { p: { revision: '1', execute: () => ({ status: 'accepted' }) } };
        options.flows.a.policies = { $data: { type: 'module', name: 'p' } };
        const runner = createRunner(options);
        try {
            const task = await runner.run({ flow: 'a', kind: 'result', result: 1 }); assert.equal(task.status, 'completed');
            const imported = await runner.importTask(runner.exportTask(task.taskId), { flow: 'a' });
            const executed = await runner.execute(imported.taskId, { report: { includeData: true, maxInlineBytes: 16384 } });
            assert.equal(executed.data.find(part => part.path.join('.') === 'policies.$data.decision').value.status, 'accepted');
        } finally { await runner.close(); await store.close(); }
    });
    test(`${name}: wide output fanout completes and transfers with incremental integrity`, async () => {
        const store = await factory(); const options = setup(store); let sends = 0;
        options.connectors = { sink: createCallbackConnector(() => { sends++; return { status: 'acknowledged' }; }) };
        options.transforms = { array: { revision: '1', execute: () => Array(10000).fill(0) } };
        options.flows.a.connections = Array.from({ length: 64 }, (_, index) => ({ id: `out-${index}`, connector: 'sink', when: 'always',
            payload: 'raw-result', transforms: [{ type: 'module', name: 'array' }] }));
        const runner = createRunner(options);
        try {
            const current = await runner.run({ flow: 'a', kind: 'result', result: 1 }, { waitFor: 'settled' });
            assert.equal(current.status, 'completed'); assert.equal(sends, 64);
            assert.deepEqual(current.deliveryCounts, { acknowledged: 64 });
            const chunks = []; for await (const chunk of runner.exportTask(current.taskId)) { assert(chunk.length < 262145); chunks.push(chunk); }
            const frames = chunks.map(chunk => JSON.parse(new TextDecoder().decode(chunk)));
            assert.equal(frames.at(-1).type, 'end');
            const lease = await runner.openImport({ flow: 'a' });
            for (const frame of frames.slice(0, -1)) {
                const [progress, replay] = await Promise.all([runner.appendImport(lease.id, frame), runner.appendImport(lease.id, frame)]);
                assert.equal(progress.verified, false);
                assert.equal(replay.nextSequence, progress.nextSequence); assert.equal(replay.verified, progress.verified);
                assert(replay.expiresAt >= progress.expiresAt);
            }
            await assert.rejects(runner.finishImport(lease.id), { code: 'INVALID_TRANSFER' });
            assert.equal((await runner.list()).items.length, 1);
            const trailer = frames.at(-1);
            await assert.rejects(runner.appendImport(lease.id, { ...trailer, checksum: '0'.repeat(64) }), { code: 'INVALID_TRANSFER' });
            await runner.appendImport(lease.id, trailer);
            const imported = await runner.finishImport(lease.id);
            assert.notEqual(imported.taskId, current.taskId);
            assert.equal((await runner.finishImport(lease.id)).taskId, imported.taskId);
            await runner.appendImport(lease.id, trailer); await runner.closeImport(lease.id);
            const reimported = await runner.importTask(chunks, { flow: 'a' }); assert.equal(reimported.taskId, imported.taskId);
        } finally { await runner.close(); await store.close(); }
    });
}

test('inspection pins a consistent execution and summary/admin reads do not open application chunks', async () => {
    const raw = createMemoryRepository(); let reads = 0;
    const repository = { ...raw, transaction: callback => raw.transaction(tx => callback({ ...tx, get(table, ...args) {
        if (table === 'chunks') reads++; return tx.get(table, ...args);
    } })) };
    const store = createExecutionStore(repository, { retentionMs: 1 }); const runner = createRunner(setup(store));
    try {
        const admitted = await runner.submit({ flow: 'a', kind: 'result', result: { pinned: true } });
        const lease = await runner.openSnapshot(admitted.taskId); reads = 0;
        await runner.get(admitted.taskId); await runner.list(); await runner.inspect(); await runner.cancel(admitted.taskId);
        assert.equal(reads, 0);
        const before = await runner.listSnapshot(lease.id); assert.equal(before.items[0].status, 'pending');
        const entries = await parts(runner, lease.id); const part = entries.find(item => item.path.join('.') === 'result');
        await runner.maintain(); assert.deepEqual(await runner.readValue(lease.id, part.id), { pinned: true });
        const other = createRunner({ ...setup(store), namespace: 'other' });
        try { await assert.rejects(other.listSnapshot(lease.id), { code: 'NOT_FOUND' }); } finally { await other.close(); }
        await runner.closeSnapshot(lease.id);
    } finally { await runner.close(); await store.close(); }
});

test('invalid optional report configuration fails before admission', async () => {
    const runner = createRunner(setup());
    try {
        await assert.rejects(runner.run({ flow: 'a', kind: 'result', result: 1 }, { report: { maxInlineBytes: 0 } }));
        assert.deepEqual((await runner.list()).items, []);
    } finally { await runner.close(); }
});

test('import cancellation reaches destination validation and prevents admission', async () => {
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); let waiting = false;
    const options = setup(); options.validators.v.validate = async value => { if (waiting) { entered.resolve(); await release.promise; } return value; };
    const runner = createRunner(options); const controller = new AbortController(); let importing;
    try {
        const task = await runner.run({ flow: 'a', kind: 'result', result: 1 });
        const chunks = []; for await (const chunk of runner.exportTask(task.taskId)) chunks.push(chunk);
        waiting = true; importing = runner.importTask(chunks, { flow: 'a', signal: controller.signal }); importing.catch(() => {});
        await entered.promise; controller.abort(); await assert.rejects(importing, { code: 'ABORTED' });
        assert.equal((await runner.list()).items.length, 1);
    } finally { release.resolve(); await importing?.catch(() => {}); await runner.close(); }
});
