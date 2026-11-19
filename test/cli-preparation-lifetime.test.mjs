import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createConfiguredRunner } from '../src/node/configuration.mjs';

const execute = promisify(execFile);

test('CLI interruption cancels derived validation before creating another execution', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-preparation-')); let runner;
    try {
        await writeFile(join(directory, 'validator.mjs'), `export default { apiVersion: 1, revision: '1', schema: { type: 'object' }, create() {
    return { validate(value) {
        if (process.env.DBC_TEST_CANCEL_PREPARATION === '1') {
            process.emit('SIGINT');
            return new Promise(() => {});
        }
        return value;
    } };
} };`);
        const configuration = { schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
            extensions: [{ kind: 'validator', name: 'interruptible', module: './validator.mjs' }],
            validators: { v: { type: 'interruptible' } },
            flows: { a: { revision: '1', inputs: ['result'], validator: 'v', timeoutMs: 30000 } } };
        const configPath = join(directory, 'config.json'); await writeFile(configPath, JSON.stringify(configuration));
        runner = await createConfiguredRunner(configuration, { baseDir: directory });
        const original = await runner.run({ flow: 'a', kind: 'result', result: 1 }); await runner.close(); runner = null;
        const cli = new URL('../src/node/cli.mjs', import.meta.url).href;
        // Emit inside an independent process, without platform-specific signal delivery.
        const script = `import { main } from ${JSON.stringify(cli)};
const exitCode = await main(process.argv.slice(1));
process.stdout.write(JSON.stringify({ exitCode }) + '\\n');`;
        const result = await execute(process.execPath, ['--input-type=module', '-e', script, '--',
            'reprocess', original.taskId, '--config', configPath], { encoding: 'utf8', timeout: 5000,
            killSignal: 'SIGKILL', env: { ...process.env, DBC_TEST_CANCEL_PREPARATION: '1' } });
        assert.equal(JSON.parse(result.stdout).exitCode, 130);
        runner = await createConfiguredRunner(configuration, { baseDir: directory });
        assert.equal((await runner.list()).items.length, 1);
        assert.equal((await runner.get(original.taskId)).status, 'completed');
    } finally { await runner?.close(); await rm(directory, { recursive: true, force: true }); }
});
