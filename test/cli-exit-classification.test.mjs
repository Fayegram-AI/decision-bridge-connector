import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { Readable, Writable } from 'node:stream';
import { main } from '../src/node/cli.mjs';

test('CLI batch emits a result for every record regardless of stream chunking', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-batch-'));
    const path = join(directory, 'config.json');
    const config = { schemaVersion: 3, validators: { v: { type: 'schema', options: { schema: {} } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
    try {
        await writeFile(path, JSON.stringify(config));
        const pieces = [Buffer.from('1\n'), Buffer.alloc(1048577, 120), Buffer.from([10, 255, 10, 50])];
        const sink = write => new Writable({ write(chunk, _encoding, done) { write(chunk.toString()); done(); } });
        for (const chunks of [pieces, [Buffer.concat(pieces)]]) {
            let stdout = '';
            let stderr = '';
            const code = await main(['batch', '--config', path, '--flow', 'a', '--kind', 'result'], {
                stdin: Readable.from(chunks), stdout: sink(text => { stdout += text; }), stderr: sink(text => { stderr += text; }),
            });
            assert.equal(code, 1);
            assert.equal(stderr, '');
            const results = stdout.trim().split('\n').map(line => JSON.parse(line));
            assert.deepEqual(results.map(row => [row.line, row.error?.code ?? row.value.status]),
                [[1, 'completed'], [2, 'INPUT_TOO_LARGE'], [3, 'INVALID_UTF8'], [4, 'completed']]);
        }
    } finally { await rm(directory, { recursive: true, force: true }); }
});

const execute = promisify(execFile);
for (const mode of ['operation', 'startup', 'configuration', 'schema', 'config-output', 'input']) test(`CLI ${mode} failure uses its documented exit class`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-exit-'));
    try {
        const library = new URL('../src/index.mjs', import.meta.url).href;
        await writeFile(join(directory, 'store.mjs'), `import {createMemoryStore,ConnectorError} from ${JSON.stringify(library)};
export default {apiVersion:1,revision:'1',schema:{type:'object'},create(){
    ${mode === 'startup' ? "throw new ConnectorError('NETWORK_ERROR','Synthetic connection failure.');" : ''}
    return {...createMemoryStore(),async inspect(){throw new ConnectorError('READ_FAULT','Synthetic read failure.');}};
}};`);
        const configuration = { schemaVersion: 3, extensions: [{ kind: 'store', name: 'probe', module: './store.mjs' }],
            store: { type: 'probe' }, validators: { v: { type: 'schema', options: { schema: mode === 'schema' ? { type: 'not-a-type' } : {} } } },
            flows: { a: { revision: '1', inputs: mode === 'configuration' ? 'invalid' : ['result'], validator: 'v' } } };
        const config = join(directory, 'config.json'); await writeFile(config, JSON.stringify(configuration));
        const args = [fileURLToPath(new URL('../bin/decision-bridge-connectors.mjs', import.meta.url)), mode === 'input' ? 'run' : 'inspect', '--config', config];
        if (mode === 'input') { await writeFile(join(directory, 'input.json'), '{invalid'); args.push('--input', join(directory, 'input.json')); }
        if (mode === 'config-output') args.splice(0, args.length, '--input-type=module', '-e', `
import {main} from ${JSON.stringify(new URL('../src/node/cli.mjs', import.meta.url).href)};
import {Writable} from 'node:stream';
const stdout=new Writable({write(_chunk,_encoding,done){done(Object.assign(new Error('Synthetic closed output.'),{code:'EPIPE'}));}});
process.exitCode=await main(['config','inspect','--config',${JSON.stringify(config)}],{stdout});`);
        try { await execute(process.execPath, args, { encoding: 'utf8', timeout: 5000 }); assert.fail('Expected the CLI to fail.'); }
        catch (error) {
            assert.equal(error.code, ['operation', 'startup', 'config-output'].includes(mode) ? 1 : 2);
            assert.equal(JSON.parse(error.stderr).ok, false);
        }
    } finally { await rm(directory, { recursive: true, force: true }); }
});
