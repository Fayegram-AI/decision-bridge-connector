import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createSpoolSource } from '../src/node/spool.mjs';

for (const phase of ['claim', 'open']) for (const replacement of ['directory', 'symlink'])
    test(`spool quarantines a file replaced by a ${replacement} during ${phase}`,
    { skip: replacement === 'symlink' && process.platform === 'win32', timeout: 10000 }, async t => {
        const fixture = await fs.mkdtemp(join(tmpdir(), 'dbc-spool-ownership-'));
        const directory = join(fixture, 'queue');
        const queued = join(directory, 'a-replaced.json');
        const target = join(fixture, 'outside.json');
        const controller = new AbortController();
        const source = createSpoolSource({ directory, flow: 'a', inputMode: 'event', pollMs: 1 });
        const iterator = source.open({ signal: controller.signal });
        const rename = fs.rename;
        const open = fs.open;
        let replaced = false;
        async function replace(path) {
            replaced = true;
            await fs.unlink(path);
            if (replacement === 'symlink') await fs.symlink(target, path);
            else await fs.mkdir(path);
        }
        try {
            await fs.mkdir(directory);
            await fs.writeFile(queued, '{"marker":"queued"}');
            await fs.writeFile(target, '{"marker":"outside"}');
            await fs.writeFile(join(directory, 'b-safe.json'), '{"marker":"safe"}');
            // Synchronize the producer replacement precisely between enumeration and claim.
            t.mock.method(fs, 'rename', async (from, to) => {
                if (phase === 'claim' && from === queued && !replaced) await replace(from);
                return rename(from, to);
            });
            // Also race the open after lstat: the opened handle must still be verified.
            t.mock.method(fs, 'open', async (path, ...options) => {
                if (phase === 'open' && String(path).endsWith('a-replaced.json') && !replaced) await replace(path);
                return open(path, ...options);
            });
            syncBuiltinESMExports();
            const message = (await iterator.next()).value;
            assert.equal(replaced, true);
            assert.equal(message.input.event.marker, 'safe');
            await message.acknowledge();
            const [claim] = await fs.readdir(join(directory, '.quarantine'));
            assert(claim);
            const evidence = await fs.lstat(join(directory, '.quarantine', claim, 'a-replaced.json'));
            assert(replacement === 'symlink' ? evidence.isSymbolicLink() : evidence.isDirectory());
            assert.equal(await fs.readFile(target, 'utf8'), '{"marker":"outside"}');
        } finally {
            t.mock.restoreAll(); syncBuiltinESMExports(); controller.abort();
            await source.close(); await iterator.return(); await fs.rm(fixture, { recursive: true, force: true });
        }
    });
