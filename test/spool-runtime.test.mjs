import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createSpoolSource } from '../src/node/spool.mjs';

test('spool runtime validation preserves queued input before opening a consumer', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-runtime-'));
    const [major, minor] = process.versions.uv.split('.').map(Number);
    const legacyWindows = process.platform === 'win32' && major === 1 && minor < 51;
    try {
        await writeFile(join(directory, 'one.json'), '42');
        const create = () => createSpoolSource({ directory, flow: 'a', inputMode: 'result' });
        if (legacyWindows) assert.throws(create, error => {
            assert.equal(error.code, 'UNSUPPORTED_RUNTIME');
            assert.match(error.message, /Node 22\.17/);
            return true;
        });
        else await create().close();
        assert.deepEqual(await readdir(directory), ['one.json']);
        assert.equal(await readFile(join(directory, 'one.json'), 'utf8'), '42');
    } finally { await rm(directory, { recursive: true, force: true }); }
});
