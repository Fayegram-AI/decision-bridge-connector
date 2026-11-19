import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createIncoming } from '../src/node/incoming.mjs';

test('incoming process cwd is captured and resolved from its configuration directory', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-process-cwd-')); const child = join(directory, 'child');
    await mkdir(child); await writeFile(join(directory, 'value.json'), '1'); await writeFile(join(child, 'value.json'), '2');
    try {
        for (const cwd of [undefined, 'child', child]) {
            const expected = cwd === undefined ? directory : child;
            const options = { command: process.execPath, args: ['-e',
                'process.stdout.write(JSON.stringify({cwd:process.cwd(),value:require("node:fs").readFileSync("value.json","utf8")}))'],
                ...(cwd === undefined ? {} : { cwd }) };
            const source = await createIncoming('process', { flow: 'a', inputMode: 'result', format: 'json', process: options }, { baseDir: directory });
            // Later caller changes cannot alter a constructed source's path or executable arguments.
            options.cwd = 'missing'; options.args[1] = 'throw Error("Changed arguments");';
            const iterator = source.open({ signal: new AbortController().signal });
            try {
                const message = (await iterator.next()).value;
                assert.equal(await realpath(message.input.result.cwd), await realpath(expected));
                assert.equal(message.input.result.value, cwd === undefined ? '1' : '2');
                assert.equal((await iterator.next()).done, true);
            } finally { await iterator.return(); }
        }
        await assert.rejects(createIncoming('process', { flow: 'a', process: { command: process.execPath, cwd: null } }), { code: 'CONFIGURATION_ERROR' });
    } finally { await rm(directory, { recursive: true, force: true }); }
});
