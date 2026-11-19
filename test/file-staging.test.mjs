import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createFileConnector } from '../src/node/connectors.mjs';
import { createOutput } from '../src/node/io.mjs';

// Both names fit common filesystem component limits before staging metadata is added.
const filenames = ['a'.repeat(230) + '.json', '漢'.repeat(78) + '.json'];

for (const overwrite of [false, true]) test(`file connector stages long filenames independently of their basename (overwrite=${overwrite})`, async t => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-staging-'));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const connector = createFileConnector({ directory, overwrite });
    for (const filename of filenames) {
        const destination = { filename };
        const context = { deliveryId: 'long-file', destination };
        const receipt = await connector.send({ value: 1 }, context);
        assert.equal(receipt.status, 'acknowledged');
        assert.equal(receipt.receipt.duplicate, false);
        assert.deepEqual(JSON.parse(await readFile(join(directory, filename), 'utf8')), { value: 1 });
        assert.equal((await connector.send({ value: 1 }, context)).receipt.duplicate, true);
        if (overwrite) {
            await connector.send({ value: 2 }, context);
            assert.deepEqual(JSON.parse(await readFile(join(directory, filename), 'utf8')), { value: 2 });
        } else await assert.rejects(connector.send({ value: 2 }, context), { code: 'DELIVERY_CONFLICT', ambiguous: false });
    }
    assert.deepEqual((await readdir(directory)).sort(), [...filenames].sort());
});

for (const force of [false, true]) test(`CLI output stages long filenames and cleans aborted writes (force=${force})`, async t => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-staging-'));
    t.after(() => rm(directory, { recursive: true, force: true }));
    for (const filename of filenames) {
        const path = join(directory, filename);
        if (force) await writeFile(path, 'previous');
        const writer = await createOutput(path, { force });
        try { await writer.write({ value: 1 }); await writer.finish(); }
        finally { await writer.abort(); }
        assert.equal(await readFile(path, 'utf8'), '{"value":1}\n');
    }
    const controller = new AbortController();
    const writer = await createOutput(join(directory, 'aborted.json'), { signal: controller.signal, force });
    await writer.write({ value: 2 }); controller.abort();
    await assert.rejects(writer.finish(), { code: 'ABORTED' });
    await writer.abort();
    assert.deepEqual((await readdir(directory)).sort(), [...filenames].sort());
});
