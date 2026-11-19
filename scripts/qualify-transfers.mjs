import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRunner, createCallbackConnector } from '../src/index.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';

// Distinct values prevent content deduplication from shrinking the scale corpus.
const directory = await mkdtemp(join(tmpdir(), 'dbc-transfer-qualification-'));
const sourceStore = createSQLiteStore(join(directory, 'source.sqlite'));
const targetStore = createSQLiteStore(join(directory, 'target.sqlite'));
const validators = { json: { revision: '1', validate: value => value } };
const source = createRunner({ store: sourceStore, validators,
    connectors: { sink: createCallbackConnector(() => ({ status: 'acknowledged' })) },
    transforms: { body: { revision: '1', execute: (_value, { index }) => ({ index, body: String(index).padStart(3, '0') + 'x'.repeat(900000) }) } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'json', connections: Array.from({ length: 80 }, (_, index) => ({
        id: `out-${index}`, connector: 'sink', when: 'always', payload: 'raw-result', transforms: [{ type: 'module', name: 'body', options: { index } }],
    })) } } });
const target = createRunner({ store: targetStore, validators, flows: { a: { revision: '1', inputs: ['result'], validator: 'json' } } });
try {
    const started = performance.now();
    const original = await source.run({ flow: 'a', kind: 'result', result: { source: 'scale' } }, { waitFor: 'settled' });
    assert.equal(original.status, 'completed'); assert.equal(original.deliveryCounts.acknowledged, 80);
    const preparedMs = performance.now() - started;
    assert.equal(typeof globalThis.gc, 'function', 'Run transfer qualification with --expose-gc.');
    globalThis.gc();
    const baseline = process.memoryUsage(); let retainedHeap = baseline.heapUsed; let peakHeap = baseline.heapUsed; let peakRss = baseline.rss; let bytes = 0; let frames = 0; let maxFrame = 0;
    async function* stream() {
        for await (const chunk of source.exportTask(original.taskId)) {
            bytes += chunk.length; frames++; maxFrame = Math.max(maxFrame, chunk.length);
            const memory = process.memoryUsage(); peakHeap = Math.max(peakHeap, memory.heapUsed); peakRss = Math.max(peakRss, memory.rss);
            // Separate reachable transfer memory from V8's collection threshold.
            if (frames % 16 === 0) { globalThis.gc(); retainedHeap = Math.max(retainedHeap, process.memoryUsage().heapUsed); }
            // Consumer pulls one frame at a time; no collection or Buffer.concat of the archive.
            yield chunk;
        }
    }
    const imported = await target.importTask(stream(), { flow: 'a' });
    assert(bytes > 64 * 1024 * 1024); assert(maxFrame <= 262145);
    const result = await target.get(imported.taskId, { includeData: true, maxInlineBytes: 16384 });
    assert(result.data.some(part => part.path.join('.') === 'result' && part.value.source === 'scale'));
    assert.equal(imported.lineage.depth, 0); assert.equal((await target.list()).items.length, 1);
    const heapGrowth = peakHeap - baseline.heapUsed;
    globalThis.gc(); retainedHeap = Math.max(retainedHeap, process.memoryUsage().heapUsed);
    const retainedGrowth = retainedHeap - baseline.heapUsed;
    console.log(JSON.stringify({ values: 80, preparedMs, transferMs: performance.now() - started - preparedMs,
        bytes, frames, maxFrame, heapGrowth, retainedGrowth, rssGrowth: peakRss - baseline.rss, result: 'passed' }));
    assert(retainedGrowth < 16 * 1024 * 1024, `Retained transfer heap growth was ${retainedGrowth} bytes.`);
    assert(heapGrowth < 64 * 1024 * 1024, `Transfer heap growth was ${heapGrowth} bytes.`);

} finally {
    await source.close(); await target.close(); await sourceStore.close(); await targetStore.close();
    await rm(directory, { recursive: true, force: true });
}
