import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { access, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createConfiguredRunner } from './helpers/runner.mjs';

const execute = promisify(execFile);
const bin = fileURLToPath(new URL('../bin/decision-bridge-connectors.mjs', import.meta.url));

test('CLI serve rejects invalid source names and volatile spool acknowledgement before claiming input', { timeout: 30000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-source-preflight-'));
    const incoming = join(directory, 'incoming'); const config = join(directory, 'config.json');
    const original = '{"kind":"result","result":42}';
    try {
        await mkdir(incoming); await writeFile(join(incoming, 'valid.json'), original);
        for (const name of ['x'.repeat(257), 'one']) {
            await writeFile(config, JSON.stringify({ schemaVersion: 3,
                validators: { v: { type: 'schema', options: { schema: {} } } },
                flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } },
                incoming: { [name]: { type: 'spool', options: { directory: incoming, flow: 'a' } } } }));
            await assert.rejects(execute(process.execPath, [bin, 'serve', '--config', config],
                { cwd: directory, encoding: 'utf8', timeout: 10000 }), error => error.code === 2 &&
                    error.stdout.trim() === '' && error.stderr.includes('"code":"CONFIGURATION_ERROR"'));
            assert.deepEqual(await readdir(incoming), ['valid.json']);
            assert.equal(await readFile(join(incoming, 'valid.json'), 'utf8'), original);
        }
    } finally { await rm(directory, { recursive: true }); }
});

test('CLI rejects unprepared retries and empty derived keys without creating work or files', { timeout: 30000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-retry-'));
    const configuration = { schemaVersion: 3, placements: ['local'], store: { type: 'sqlite', options: { path: 'state.sqlite' } },
        validators: { v: { type: 'schema', options: { schema: {} } } }, connectors: { sink: { type: 'file', options: { directory: 'out' } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
            { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result', outputSchema: false, placement: 'local' },
        ] } } };
    const config = join(directory, 'config.json'); const options = join(directory, 'options.json');
    try {
        await writeFile(config, JSON.stringify(configuration)); await writeFile(options, '{"idempotencyKey":""}');
        const initial = await createConfiguredRunner(configuration, { baseDir: directory }); let task;
        try { task = await initial.run({ flow: 'a', kind: 'result', result: 1 }); }
        finally { await initial.close(); }
        for (const [args, code] of [
            [['retry', task.deliveries[0].id], 'CONFLICT'],
            [['reprocess', task.taskId, '--options', options], 'CONFIGURATION_ERROR'],
            [['redeliver', task.taskId, '--flow', 'a', '--options', options], 'CONFIGURATION_ERROR'],
        ]) await assert.rejects(execute(process.execPath, [bin, ...args, '--config', config],
            { cwd: directory, encoding: 'utf8', timeout: 10000 }), error => error.code === 2 &&
                error.stdout.trim() === '' && error.stderr.includes(`"code":"${code}"`));
        const reader = await createConfiguredRunner(configuration, { baseDir: directory });
        try {
            const page = await reader.list(); assert.equal(page.items.length, 1);
            assert.equal(page.items[0].deliveries[0].status, 'failed'); assert.equal(page.items[0].deliveries[0].attempts.length, 0);
        } finally { await reader.close(); }
        await assert.rejects(access(join(directory, 'out')), { code: 'ENOENT' });
    } finally { await rm(directory, { recursive: true }); }
});

test('CLI list honors the configured default and explicit page limits', { timeout: 20000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-pages-'));
    const configuration = { schemaVersion: 3, limits: { pageSize: 1 }, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
        validators: { v: { type: 'schema', options: { schema: { type: 'number' } } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
    const config = join(directory, 'config.json'); const options = join(directory, 'options.json');
    const command = async args => JSON.parse((await execute(process.execPath, [bin, ...args, '--config', config],
        { cwd: directory, encoding: 'utf8', timeout: 10000 })).stdout);
    try {
        await writeFile(config, JSON.stringify(configuration));
        const runner = await createConfiguredRunner(configuration, { baseDir: directory });
        try { for (let index = 0; index < 3; index++) await runner.submit({ flow: 'a', kind: 'result', result: index }); }
        finally { await runner.close(); }
        const first = await command(['list']); assert.equal(first.items.length, 1); assert(first.next);
        await writeFile(options, JSON.stringify({ after: first.next, limit: 2 }));
        const rest = await command(['list', '--options', options]); assert.equal(rest.items.length, 2);
        assert.equal(new Set([...first.items, ...rest.items].map(item => item.taskId)).size, 3);
    } finally { await rm(directory, { recursive: true, force: true }); }
});

test('CLI submit persists admission without execution options and a later worker resumes it', { timeout: 20000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-submit-'));
    const configuration = { schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
        validators: { v: { type: 'schema', options: { schema: { type: 'number' } } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
    const config = join(directory, 'config.json'); const input = join(directory, 'input.json');
    const command = async args => JSON.parse((await execute(process.execPath, [bin, ...args, '--config', config],
        { cwd: directory, encoding: 'utf8', timeout: 10000 })).stdout);
    try {
        await writeFile(config, JSON.stringify(configuration)); await writeFile(input, '42');
        const admitted = await command(['submit', '--flow', 'a', '--kind', 'result', '--input', input, '--wait', 'settled', '--wait-ms', '1']);
        assert.equal(admitted.status, 'running'); assert.equal(admitted.requiredSettled, false);
        const saved = await command(['status', admitted.taskId]);
        assert.equal(saved.taskId, admitted.taskId); assert.equal(saved.status, 'running');
        const worker = await createConfiguredRunner(configuration, { baseDir: directory });
        try { assert.equal((await worker.execute(admitted.taskId)).status, 'completed'); }
        finally { await worker.close(); }
        assert.equal((await command(['status', admitted.taskId])).status, 'completed');
    } finally { await rm(directory, { recursive: true }); }
});

test('CLI pause rejects a missing destination target without blocking saved work', { timeout: 20000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-pause-'));
    const configuration = { schemaVersion: 3, store: { type: 'sqlite', options: { path: 'state.sqlite' } },
        validators: { v: { type: 'schema', options: { schema: {} } } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
    const config = join(directory, 'config.json'); const options = join(directory, 'options.json'); let task;
    try {
        await writeFile(config, JSON.stringify(configuration)); await writeFile(options, '{"type":"destination"}');
        const initial = await createConfiguredRunner(configuration, { baseDir: directory });
        try { task = await initial.submit({ flow: 'a', kind: 'result', result: 42 }); } finally { await initial.close(); }
        await assert.rejects(execute(process.execPath, [bin, 'pause', '--config', config, '--options', options],
            { cwd: directory, encoding: 'utf8', timeout: 10000 }), error => error.code === 2 && error.stdout.trim() === '' &&
                error.stderr.includes('"code":"CONFIGURATION_ERROR"'));
        const reader = await createConfiguredRunner(configuration, { baseDir: directory });
        try { assert.equal((await reader.execute(task.taskId)).status, 'completed'); } finally { await reader.close(); }
    } finally { await rm(directory, { recursive: true }); }
});
