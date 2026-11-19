import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { syncBuiltinESMExports } from 'node:module';
import { join } from 'node:path';
import { tmpdir, release } from 'node:os';
import { createSpoolSource } from '../src/node/spool.mjs';
import { bounded } from '../src/util.mjs';

test('spool admits native regular files with exact handle identity', async () => {
    const directory = await fs.mkdtemp(join(tmpdir(), 'dbc-spool-native-identity-'));
    const controller = new AbortController();
    const source = createSpoolSource({ directory, flow: 'a', inputMode: 'result', pollMs: 1 });
    const iterator = source.open({ signal: controller.signal });
    try {
        const path = join(directory, 'one.json'); await fs.writeFile(path, '42');
        const expected = await fs.lstat(path, { bigint: true }); const handle = await fs.open(path, 'r');
        try {
            const actual = await handle.stat({ bigint: true });
            // Retain native measurements on affected runners without printing payloads or paths.
            if (expected.dev !== actual.dev) console.log('Native spool device identity:', JSON.stringify({
                platform: process.platform, os: release(), node: process.version, uv: process.versions.uv,
                pathDevice: String(expected.dev), handleDevice: String(actual.dev),
                pathInode: String(expected.ino), handleInode: String(actual.ino),
            }));
        } finally { await handle.close(); }
        const message = await bounded(() => iterator.next(), { timeoutMs: 3000 });
        await message.value.acknowledge(); assert.equal(message.value.input.result, 42);
        assert.equal((await fs.readdir(join(directory, '.processed'))).length, 1);
        assert.deepEqual(await fs.readdir(join(directory, '.quarantine')), []);
    } finally { controller.abort(); await source.close(); await iterator.return(); await fs.rm(directory, { recursive: true, force: true }); }
});

for (const scenario of ['different device high bits', 'different device', 'adjacent large inode'])
    test(`spool verifies file identity with ${scenario}`, async t => {
        const directory = await fs.mkdtemp(join(tmpdir(), 'dbc-spool-stat-identity-'));
        const controller = new AbortController();
        const source = createSpoolSource({ directory, flow: 'a', inputMode: 'result', pollMs: 1 });
        const iterator = source.open({ signal: controller.signal });
        const lstat = fs.lstat; const open = fs.open;
        const inode = 1n << 54n;
        const statistics = (info, options, pathBased) => {
            let dev = BigInt(info.dev); let ino = BigInt(info.ino);
            if (scenario === 'different device high bits') {
                dev = BigInt.asUintN(32, dev) + (pathBased ? 1n << 40n : 0n);
            } else if (scenario === 'different device' && pathBased) dev ^= 1n;
            else if (scenario === 'adjacent large inode') ino = inode + (pathBased ? 0n : 1n);
            return { ...info, isFile: () => info.isFile(),
                dev: options?.bigint ? dev : Number(dev), ino: options?.bigint ? ino : Number(ino) };
        };
        try {
            await fs.writeFile(join(directory, 'a-checked.json'), '1');
            await fs.writeFile(join(directory, 'b-safe.json'), '2');
            t.mock.method(fs, 'lstat', async (path, options) => {
                const info = await lstat(path, options);
                return String(path).endsWith('a-checked.json') ? statistics(info, options, true) : info;
            });
            t.mock.method(fs, 'open', async (path, ...options) => {
                const handle = await open(path, ...options);
                if (String(path).endsWith('a-checked.json')) {
                    const stat = handle.stat.bind(handle);
                    handle.stat = async options => statistics(await stat(options), options, false);
                }
                return handle;
            });
            syncBuiltinESMExports();
            const message = await bounded(() => iterator.next(), { timeoutMs: 3000 });
            await message.value.acknowledge(); assert.equal(message.value.input.result, 2);
            assert.equal((await fs.readdir(join(directory, '.quarantine'))).length, 1);
        } finally {
            t.mock.restoreAll(); syncBuiltinESMExports(); controller.abort(); await source.close();
            await iterator.return(); await fs.rm(directory, { recursive: true, force: true });
        }
    });
