import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { createRunner } from '../src/index.mjs';
import { chainHash, EMPTY_HASH, FRAME_BYTES, frameShape, parseFrames, wireBytes } from '../src/storage/framing.mjs';

const options = { validators: { saved: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'saved' } } };
const header = { type: 'header', seq: 0, kind: 'execution-export', schemaVersion: 3 };
const metadata = { id: 'task-1', namespace: 'default', flow: 'a', schemaVersion: 3, evidenceAt: '2026-01-01T00:00:00Z' };
const record = bytes => ({ type: 'record', seq: 1, entity: 'task', recordId: 'task-1', index: 0, final: true,
    data: bytes.toString('base64') });

/** Use a valid trailer so the importer must check the enclosed evidence itself. */
async function archive(frames) {
    let checksum = EMPTY_HASH;
    for (const frame of frames) checksum = await chainHash(checksum, wireBytes(frame));
    const end = { type: 'end', seq: frames.length, frameCount: frames.length, checksum };
    return [...frames, end].map(frame => Buffer.from(JSON.stringify(frame) + '\n'));
}

test('corrupt UTF-8, metadata and saved evidence references produce input errors before admission', async () => {
    const runner = createRunner({ ...options, limits: { maxTransfers: 1 } });
    try {
        for (const bytes of [Buffer.from([0xff]), Buffer.from([0xc3]), Buffer.from('null'),
            Buffer.from(JSON.stringify({ ...metadata, result: null }))]) {
            const chunks = await archive([header, record(bytes)]);
            await assert.rejects(runner.importTask(chunks, { flow: 'a' }), { code: 'INVALID_TRANSFER' });
        }
        // A matching digest cannot make BOM-prefixed JSON canonical.
        for (const blob of [Buffer.from([0xff]), Buffer.from('\uFEFFnull')]) {
            const sha256 = createHash('sha256').update(blob).digest('hex');
            const value = { ...metadata, result: { $data: { kind: 'json', sha256, bytes: blob.length } } };
            const chunks = await archive([header, record(Buffer.from(JSON.stringify(value))),
                { type: 'chunk', seq: 2, index: 0, sha256, data: blob.toString('base64') }]);
            await assert.rejects(runner.importTask(chunks, { flow: 'a' }), { code: 'INVALID_TRANSFER' });
        }
        assert.equal((await runner.list()).items.length, 0);
        const lease = await runner.openImport({ flow: 'a' });
        await runner.closeImport(lease.id);
    } finally { await runner.close(); }
});

test('stream decoding distinguishes invalid UTF-8 from an interrupted producer', async () => {
    const runner = createRunner(options);
    try {
        for (const chunks of [[Buffer.from([0xff])], [Buffer.from([0xc3])],
            [Buffer.from([0xc3]), Buffer.from([0x28])]]) {
            await assert.rejects(runner.importTask(chunks, { flow: 'a' }), { code: 'INVALID_TRANSFER' });
        }
        const interrupted = new TypeError('input stream interrupted');
        async function* chunks() { yield Buffer.from([0xc3]); throw interrupted; }
        await assert.rejects(runner.importTask(chunks(), { flow: 'a' }), error => error === interrupted);
    } finally { await runner.close(); }
});

test('import frame tags and digests must be strings', () => {
    for (const frame of [{ ...record(Buffer.from('{}')), type: ['record'] },
        { type: 'chunk', seq: 1, sha256: [EMPTY_HASH], index: 0, data: 'e30=' },
        { type: 'end', seq: 1, frameCount: 1, checksum: [EMPTY_HASH] }]) {
        assert.throws(() => frameShape(frame), { code: 'INVALID_TRANSFER' });
    }
});

test('fragmented Unicode frames retain the exact byte limit across lines', async () => {
    const value = 'é'.repeat((FRAME_BYTES - 2) / 2);
    function* fragments(text) {
        const bytes = Buffer.from(text);
        for (let offset = 0; offset < bytes.length; offset += 127) yield bytes.subarray(offset, offset + 127);
    }
    const line = JSON.stringify(value);
    const results = [];
    for await (const frame of parseFrames(fragments('0\n' + line + '\n1\n'))) results.push(frame);
    assert.deepEqual(results, [0, value, 1]);
    await assert.rejects(parseFrames(fragments(line + ' \n')).next(), { code: 'INVALID_TRANSFER' });
});
