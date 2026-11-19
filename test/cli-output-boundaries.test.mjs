import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable, Writable } from 'node:stream';
import { EventEmitter } from 'node:events';
import { mkdtemp, rm, writeFile, readFile, access } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { main } from '../src/node/cli.mjs';
import { bounded } from '../src/util.mjs';

async function fixture() {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-output-'));
    const effects = join(directory, 'effects.txt'); const config = join(directory, 'config.json');
    await writeFile(join(directory, 'extension.mjs'), `
        import { appendFile } from 'node:fs/promises';
        export default { apiVersion: 1, revision: '1', schema: { type: 'object' }, create({ effects }) {
            return { revision: '1', async execute() { await appendFile(effects, 'evaluate\\n'); return { ok: true }; },
                async send() { await appendFile(effects, 'deliver\\n'); return { status: 'acknowledged' }; } };
        } };
    `);
    await writeFile(config, JSON.stringify({ schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
        extensions: [{ kind: 'evaluator', name: 'fixture', module: './extension.mjs' }, { kind: 'outgoing', name: 'fixture', module: './extension.mjs' }],
        evaluators: { e: { type: 'fixture', options: { effects } } }, connectors: { sink: { type: 'fixture', options: { effects } } },
        flows: { a: { revision: '1', operation: 'extraction', inputs: ['request'], evaluator: 'e',
            policies: { action: { revision: '1', accept: { path: 'ok', equals: true } } },
            connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] } } }));
    const input = () => Readable.from(['{"flow":"a","kind":"request","request":1}\n']);
    const sink = () => new Writable({ write(_chunk, _encoding, callback) { callback(); } });
    return { directory, effects, config, input, sink };
}

test('invalid CLI reply choices cannot invoke inference or outgoing effects', async () => {
    const f = await fixture();
    try {
        for (const args of [['run', '--reply', 'invalid'], ['run', '--reply', 'final'], ['run', '--reply', 'final', '--policy', 'missing'],
            ['cancel', 'unknown', '--reply', 'invalid'], ['preview', '--reply', 'raw']]) {
            assert.equal(await main([...args, '--config', f.config], { stdin: f.input(), stdout: f.sink(), stderr: f.sink() }), 2);
            await assert.rejects(access(f.effects), { code: 'ENOENT' });
        }
        assert.equal(await main(['batch', '--config', f.config, '--reply', 'final', '--policy', 'missing'],
            { stdin: f.input(), stdout: f.sink(), stderr: f.sink() }), 1);
        await assert.rejects(access(f.effects), { code: 'ENOENT' });
        assert.equal(await main(['run', '--config', f.config, '--reply', 'final', '--policy', 'action'],
            { stdin: f.input(), stdout: f.sink(), stderr: f.sink() }), 0);
        assert.equal(await readFile(f.effects, 'utf8'), 'evaluate\ndeliver\n');
    } finally { await rm(f.directory, { recursive: true, force: true }); }
});

test('SIGINT cancels blocked CLI output and broken diagnostics cannot prevent cleanup', async () => {
    const f = await fixture(); const listeners = process.listenerCount('SIGINT');
    try {
        for (const command of [['config', 'inspect'], ['run']]) {
            const entered = Promise.withResolvers(); const stdout = new EventEmitter(); let callback;
            stdout.write = (_text, done) => { callback = done; entered.resolve(); return false; };
            stdout.destroy = () => { stdout.destroyed = true; };
            const running = main([...command, '--config', f.config], { stdin: f.input(), stdout, stderr: f.sink() });
            await bounded(() => entered.promise, { timeoutMs: 5000 }); process.emit('SIGINT');
            assert.equal(await bounded(() => running, { timeoutMs: 5000 }), 130); assert.equal(stdout.destroyed, true);
            callback(new Error('Late EPIPE')); stdout.emit('error', new Error('Late EPIPE'));
            await new Promise(resolve => setImmediate(resolve)); stdout.emit('close');
            assert.equal(process.listenerCount('SIGINT'), listeners);
        }
        const stderr = new Writable({ write(_chunk, _encoding, callback) { callback(new Error('Broken diagnostics')); } });
        assert.equal(await main(['invalid', '--bad', 'flag'], { stdout: f.sink(), stderr }), 2);
    } finally { await rm(f.directory, { recursive: true, force: true }); }
});
