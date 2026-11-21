import assert from 'node:assert/strict';
import test from 'node:test';
import { createMemoryStore, createRunner } from '../src/index.mjs';
import { collectExport } from './helpers/runner.mjs';

const settings = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
const input = { flow: 'a', kind: 'result', result: { verified: true } };
const target = { flow: 'a' };

test('file imports require one ordered archive and explicit sessions retain frame retries', async () => {
    const runner = createRunner({ ...settings, limits: { maxTransfers: 1 } });
    try {
        const original = await runner.run(input);
        const lines = await collectExport(runner, original.taskId);
        for (const invalid of [[lines[0], ...lines], [lines[0], ...lines.slice(2)], lines.slice(0, -1),
            [...lines, lines.at(-1)], [...lines, ...lines]]) {
            await assert.rejects(runner.importTask(invalid, target), { code: 'INVALID_TRANSFER' });
        }
        assert.equal((await runner.list()).items.length, 1);
        const imported = await runner.importTask(lines, target);
        assert.equal((await runner.importTask(lines, target)).taskId, imported.taskId);
        const lease = await runner.openImport(target);
        try {
            for (const line of lines) {
                const frame = JSON.parse(new TextDecoder().decode(line));
                await runner.appendImport(lease.id, frame);
                const retry = await runner.appendImport(lease.id, frame);
                assert.equal(retry.nextSequence, frame.seq + 1);
            }
            assert.equal((await runner.finishImport(lease.id)).taskId, imported.taskId);
        } finally { await runner.closeImport(lease.id); }
        assert.equal((await runner.list()).items.length, 2);
    } finally { await runner.close(); }
});

test('cancellation between buffered frames stops writes and releases the input and import slot', async () => {
    const store = createMemoryStore({ maxTransfers: 1 });
    const controller = new AbortController();
    const written = [];
    const runner = createRunner({ ...settings, store: { ...store,
        async appendImport(namespace, id, frame) {
            const progress = await store.appendImport(namespace, id, frame);
            written.push(frame.seq);
            controller.abort();
            return progress;
        } } });
    try {
        const original = await runner.run(input);
        const allFrames = Buffer.concat(await collectExport(runner, original.taskId));
        let returned = 0;
        function* chunks() { try { yield allFrames; } finally { returned++; } }
        await assert.rejects(runner.importTask(chunks(), { ...target, signal: controller.signal }), { code: 'ABORTED' });
        assert.deepEqual(written, [0]);
        assert.equal(returned, 1);
        assert.equal((await runner.list()).items.length, 1);
        const lease = await runner.openImport(target);
        await runner.closeImport(lease.id);
    } finally { await runner.close(); await store.close(); }
});

test('an aborted export read releases its snapshot and a pre-cancelled finish skips storage', async () => {
    const store = createMemoryStore({ maxTransfers: 1 });
    const controller = new AbortController();
    let reads = 0;
    const runner = createRunner({ ...settings, store: { ...store,
        async readExportFrames(...args) {
            const page = await store.readExportFrames(...args);
            controller.abort();
            return page;
        },
        async importedTask(...args) { reads++; return store.importedTask(...args); },
    } });
    try {
        const original = await runner.run(input);
        const stream = runner.exportTask(original.taskId, { signal: controller.signal });
        try { await assert.rejects(stream.next(), { code: 'ABORTED' }); }
        finally { await stream.return(); }
        const lease = await runner.openImport(target);
        try {
            await assert.rejects(runner.finishImport(lease.id, { signal: controller.signal }), { code: 'ABORTED' });
            assert.equal(reads, 0);
        } finally { await runner.closeImport(lease.id); }
    } finally { await runner.close(); await store.close(); }
});

test('a fully consumed archive leaves an exhausted iterator alone', async () => {
    const runner = createRunner(settings);
    try {
        const original = await runner.run(input);
        const iterator = (await collectExport(runner, original.taskId)).values();
        let earlyReturns = 0;
        iterator.return = () => { earlyReturns++; return { done: true }; };
        const imported = await runner.importTask(iterator, target);
        assert.ok(imported.taskId);
        assert.equal(earlyReturns, 0);
    } finally { await runner.close(); }
});

test('aborting a paused export frees its slot before the consumer asks for another frame', { timeout: 5000 }, async () => {
    const store = createMemoryStore({ maxTransfers: 1 }); let closes = 0;
    const controller = new AbortController();
    const closed = Promise.withResolvers();
    const runner = createRunner({ ...settings, store: { ...store, async closeSnapshot(...args) {
        closes++;
        await store.closeSnapshot(...args);
        closed.resolve();
    } } });
    try {
        const { taskId } = await runner.run(input);
        const stream = runner.exportTask(taskId, { signal: controller.signal });
        try {
            assert.equal((await stream.next()).done, false);
            controller.abort();
            await closed.promise;
            const lease = await runner.openImport(target);
            try { await assert.rejects(stream.next(), { code: 'ABORTED' }); }
            finally { await runner.closeImport(lease.id); }
        } finally { await stream.return(); }
        await runner.close();
        assert.equal(closes, 1);
    } finally { await runner.close(); await store.close(); }
});

test('stream cleanup retries transient close failures and preserves the import outcome', async () => {
    const store = createMemoryStore({ maxTransfers: 1 });
    const attempts = { closeSnapshot: 0, closeImport: 0 };
    const flaky = Object.fromEntries(Object.keys(attempts).map(method => [method, async (...args) => {
        attempts[method]++;
        if (attempts[method] === 1) throw Error('Transient close failure');
        return store[method](...args);
    }]));
    const runner = createRunner({ ...settings, store: { ...store, ...flaky } });
    try {
        const { taskId } = await runner.run(input);
        const lines = await collectExport(runner, taskId);
        await assert.rejects(runner.importTask(lines.slice(0, -1), target), { code: 'INVALID_TRANSFER' });
        assert.deepEqual(attempts, { closeSnapshot: 2, closeImport: 2 });
        const imported = await runner.importTask(lines, target);
        assert.equal((await runner.importTask(lines, target)).taskId, imported.taskId);
        assert.equal((await runner.list()).items.length, 2);
    } finally { await runner.close(); await store.close(); }
});
