import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { createIncoming } from '../src/node/incoming.mjs';
import { createSpoolSource } from '../src/node/spool.mjs';
import { bounded } from '../src/util.mjs';
import { createRunner, createCallbackConnector } from '../src/index.mjs';

function nested(depth = 100) { let value = 1; for (let level = 0; level < depth; level++) value = { value }; return value; }
for (const format of ['json', 'jsonl']) test(`file ${format} identity capture preserves application depth`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-file-value-')); const controller = new AbortController(); let iterator;
    try {
        await writeFile(join(directory, 'task.json'), JSON.stringify(nested()) + '\n');
        const source = await createIncoming('file', { path: 'task.json', flow: 'a', format, inputMode: 'result' }, { baseDir: directory });
        iterator = source.open({ signal: controller.signal });
        const message = (await iterator.next()).value;
        assert.deepEqual(message.input.result, nested()); assert.equal(typeof message.input.idempotencyKey, 'string');
    } finally { controller.abort(); await iterator?.return(); await rm(directory, { recursive: true, force: true }); }
});

test('file JSON/JSONL normal input identities remain byte-compatible', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-file-key-'));
    try {
        await writeFile(join(directory, 'task.json'), '{"kind":"result","result":{"z":1,"a":"é","10":"ten","2":"two","01":"leading"}}\n');
        for (const format of ['json', 'jsonl']) {
            const source = await createIncoming('file', { path: 'task.json', flow: 'a', format }, { baseDir: directory });
            const iterator = source.open({ signal: new AbortController().signal });
            try {
                const message = (await iterator.next()).value;
                const canonical = `["task.json",${format === 'jsonl' ? '0,' : ''}{"kind":"result","result":{"2":"two","10":"ten","01":"leading","a":"é","z":1}}]`;
                assert.equal(message.input.idempotencyKey, createHash('sha256').update(canonical).digest('hex'));
            } finally { await iterator.return(); }
        }
    } finally { await rm(directory, { recursive: true, force: true }); }
});

test('runtime identity hashing does not charge its wrapper to a valid captured flow plan', async () => {
    let delivered;
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(async value => { delivered = value; return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', projection: { $literal: nested(95) } },
        ] } } });
    try { assert.equal((await runner.run({ flow: 'a', kind: 'result', result: 1 })).status, 'completed'); assert.deepEqual(delivered, nested(95)); }
    finally { await runner.close(); }
});

test('spool capture does not quarantine valid depth-boundary submissions', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-value-')); const controller = new AbortController(); let iterator;
    try {
        await mkdir(join(directory, 'inbox')); await writeFile(join(directory, 'inbox', 'task.json'), JSON.stringify({ kind: 'result', result: nested() }));
        iterator = createSpoolSource({ directory: join(directory, 'inbox'), flow: 'a', pollMs: 5 }).open({ signal: controller.signal });
        const step = await bounded(() => iterator.next(), { timeoutMs: 1000 });
        assert.deepEqual(step.value.input.result, nested()); await step.value.acknowledge();
    } finally { controller.abort(); await iterator?.return().catch(() => {}); await rm(directory, { recursive: true, force: true }); }
});
