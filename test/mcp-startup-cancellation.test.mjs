import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

test('CLI cancellation closes an owned MCP process before its handshake timeout', { timeout: 12000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-mcp-startup-'));
    try {
        const config = { schemaVersion: 3, connectors: { m: { type: 'mcp', options: { transport: 'stdio',
            command: process.execPath, args: ['-e', 'setInterval(()=>{},1000)'], tool: 'fixture', timeoutMs: 60000 } } },
        validators: { v: { type: 'schema', options: { schema: {} } } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
        await writeFile(join(directory, 'config.json'), JSON.stringify(config));
        const cli = new URL('../src/node/cli.mjs', import.meta.url).href;
        // Observe only processes created by this independent fixture. The fallback
        // cleanup also prevents a regression from leaving its child behind.
        const script = `import cp from 'node:child_process'; import { syncBuiltinESMExports } from 'node:module';
import { once } from 'node:events'; import { main } from ${JSON.stringify(cli)};
const original = cp.spawn; const children = []; let timer;
cp.spawn = (...args) => { const child = original(...args); children.push(child);
    child.once('spawn', () => process.emit('SIGINT')); return child; }; syncBuiltinESMExports();
try {
    const exitCode = await main(['serve', '--config', process.argv[1]]);
    await Promise.race([Promise.all(children.map(child => child.exitCode !== null || child.signalCode !== null ? undefined : once(child, 'close'))),
        new Promise(resolve => { timer = setTimeout(resolve, 3000); })]); clearTimeout(timer);
    console.log(JSON.stringify({ exitCode, spawned: children.length,
        alive: children.filter(child => child.exitCode === null && child.signalCode === null).length }));
} finally { clearTimeout(timer); cp.spawn = original; syncBuiltinESMExports();
    for (const child of children) if (child.exitCode === null && child.signalCode === null) {
        const closed = once(child, 'close'); child.kill('SIGKILL'); await closed.catch(() => {});
    }
}`;
        const result = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', script, join(directory, 'config.json')],
            { timeout: 9000, killSignal: 'SIGKILL', encoding: 'utf8' });
        assert.deepEqual(JSON.parse(result.stdout), { exitCode: 130, spawned: 1, alive: 0 });
        assert.equal(result.stderr, '');
    } finally { await rm(directory, { recursive: true, force: true }); }
});
