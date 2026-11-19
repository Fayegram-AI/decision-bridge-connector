import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRegistry, createRunner } from '../src/index.mjs';
import { createConfiguredRunner, loadConfig } from '../src/node/configuration.mjs';
import { createHost } from '../src/node/host.mjs';
import { createService } from '../src/node/service.mjs';
import { SOURCE_PREFLIGHT } from '../src/node/source-recovery.mjs';

const execute = promisify(execFile);
const configuration = { schemaVersion: 3, validators: { v: { type: 'schema', options: { schema: {} } } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
const runnerOptions = { validators: { v: { revision: '1', validate: value => value } }, flows: configuration.flows };

test('configuration files enforce wire size, strict UTF-8 JSON and pre-read cancellation', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-config-read-'));
    try {
        const file = join(directory, 'config.json'); const json = JSON.stringify(configuration);
        await writeFile(file, json.padEnd(4 * 1024 * 1024));
        assert.equal((await loadConfig(file)).config.schemaVersion, 3);
        await writeFile(file, json.padEnd(4 * 1024 * 1024 + 1));
        await assert.rejects(loadConfig(file), { code: 'INPUT_TOO_LARGE' });
        const bytes = Buffer.from(JSON.stringify({ ...configuration, namespace: 'XX' }));
        const position = bytes.indexOf('XX'); bytes[position] = 0xc3; bytes[position + 1] = 0x28;
        await writeFile(file, bytes);
        await assert.rejects(loadConfig(file), { code: 'INVALID_JSON' });
        await writeFile(file, '{"apiKey":"fixture-secret", broken}');
        await assert.rejects(loadConfig(file), error => error.code === 'INVALID_JSON' && !error.message.includes('fixture-secret'));
        await assert.rejects(loadConfig(join(directory, 'missing.json'), { signal: AbortSignal.abort() }), { code: 'ABORTED' });
    } finally { await rm(directory, { recursive: true, force: true }); }
});

test('environment references require exact nonempty string selectors before property lookup', async () => {
    let creates = 0;
    const makeRegistry = () => createRegistry().register('outgoing', 'fixture', { apiVersion: 1, revision: '1', schema: {},
        create() { creates++; return { send() {} }; } });
    for (const selector of [null, 0, true, '', ['KEY'], {}]) {
        await assert.rejects(createConfiguredRunner({ ...configuration, connectors: { sink: { type: 'fixture',
            options: { key: { $env: selector } } } } }, { registry: makeRegistry(),
            env: { null: 'fixture', 0: 'fixture', true: 'fixture', '': 'fixture', KEY: 'fixture', '[object Object]': 'fixture' } }),
        { code: 'CONFIGURATION_ERROR' });
    }
    assert.equal(creates, 0);
});

test('cancelled configured startup closes prior resources and resources arriving later', async () => {
    const entered = Promise.withResolvers(); const released = Promise.withResolvers(); const lateClosed = Promise.withResolvers();
    const controller = new AbortController(); let firstClosed = 0; let laterCreated = false;
    const registry = createRegistry().register('outgoing', 'fixture', { apiVersion: 1, revision: '1', schema: {},
        create(options, { signal }) {
            if (options.late) { entered.resolve(signal); return released.promise; }
            if (options.never) laterCreated = true;
            return { send() {}, close() { firstClosed++; } };
        } });
    const startup = createConfiguredRunner({ ...configuration, connectors: { first: { type: 'fixture' },
        late: { type: 'fixture', options: { late: true } }, never: { type: 'fixture', options: { never: true } } } },
    { registry, signal: controller.signal });
    const rejected = assert.rejects(startup, { code: 'ABORTED' });
    try {
        const signal = await entered.promise; controller.abort(); await rejected;
        assert.equal(signal.aborted, true); assert.equal(firstClosed, 1); assert.equal(laterCreated, false);
        released.resolve({ send() {}, close() { lateClosed.resolve(); } });
        await lateClosed.promise;
    } finally { released.resolve({ close() {} }); await startup.catch(() => {}); }
});

test('cancelled extension evaluation releases its graph and a later startup can load the same source', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cancel-graph-')); const key = crypto.randomUUID();
    const state = globalThis[key] = { entered: Promise.withResolvers(), release: Promise.withResolvers(), creates: 0 };
    const controller = new AbortController(); let runner;
    try {
        await writeFile(join(directory, 'extension.mjs'), `const state = globalThis[${JSON.stringify(key)}];
state.entered.resolve(); await state.release.promise;
export default { apiVersion: 1, revision: '1', schema: {}, create() { state.creates++; return { send() {} }; } };`);
        const config = { ...configuration, extensions: [{ kind: 'outgoing', name: 'fixture', module: './extension.mjs' }],
            connectors: { fixture: { type: 'fixture' } } };
        const startup = createConfiguredRunner(config, { baseDir: directory, signal: controller.signal });
        const rejected = assert.rejects(startup, { code: 'ABORTED' });
        await state.entered.promise; controller.abort(); await rejected;
        state.release.resolve(); await new Promise(resolve => setImmediate(resolve)); assert.equal(state.creates, 0);
        runner = await createConfiguredRunner(config, { baseDir: directory }); assert.equal(state.creates, 1);
    } finally { state.release.resolve(); await runner?.close(); delete globalThis[key]; await rm(directory, { recursive: true, force: true }); }
});

for (const stage of ['preflight', 'recovery', 'source']) test(`host startup cancellation interrupts ${stage} without activating work`, async () => {
    const entered = Promise.withResolvers(); const controller = new AbortController(); let closed = 0; let sourceSignal;
    const runner = createRunner(runnerOptions);
    const source = { start({ signal }) {
        sourceSignal = signal;
        if (stage === 'source') { entered.resolve(); return new Promise(() => {}); }
    }, close() { closed++; },
    ...(stage === 'preflight' ? { [SOURCE_PREFLIGHT]() { entered.resolve(); return new Promise(() => {}); } } : {}) };
    const borrowed = stage === 'recovery' ? { ...runner, recover() { entered.resolve(); return new Promise(() => {}); } } : runner;
    const startup = createHost({ runner: borrowed, role: stage === 'recovery' ? 'all' : 'ingress',
        sources: { fixture: source }, signal: controller.signal, drainMs: 1000 });
    const rejected = assert.rejects(startup, { code: 'ABORTED' });
    try {
        await entered.promise; controller.abort(); await rejected;
        assert.equal(closed, stage === 'source' ? 1 : 0);
        if (stage === 'source') assert.equal(sourceSignal.aborted, true);
        assert.equal((await runner.run({ flow: 'a', kind: 'result', result: 1 })).status, 'completed');
    } finally { await runner.close(); }
});

test('startup signals detach after the host and configured runner become ready', async () => {
    const controller = new AbortController(); let sourceSignal;
    const runner = await createConfiguredRunner(configuration, { signal: controller.signal });
    const host = await createHost({ runner, role: 'ingress', signal: controller.signal,
        sources: { fixture: { start({ signal }) { sourceSignal = signal; } } } });
    try {
        controller.abort(); assert.equal(sourceSignal.aborted, false); assert.equal((await host.health()).ready, true);
        assert.equal((await runner.run({ flow: 'a', kind: 'result', result: 1 })).status, 'completed');
    } finally { await host.close(); }
    await assert.rejects(createService(configuration, { signal: AbortSignal.abort() }), { code: 'ABORTED' });
});

for (const stage of ['module', 'factory', 'source']) test(`CLI interruption cancels ${stage} startup before its ordinary timeout`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-cli-startup-'));
    try {
        const code = stage === 'module' ? `process.emit('SIGINT'); await new Promise(() => {});` : '';
        const factory = stage === 'factory' ? `process.emit('SIGINT'); return new Promise(() => {});` :
            `return { start() { process.emit('SIGINT'); return new Promise(() => {}); }, close() {} };`;
        await writeFile(join(directory, 'extension.mjs'), `${code}
export default { apiVersion: 1, revision: '1', schema: {}, create() { ${factory} } };`);
        const config = { ...configuration, extensions: [{ kind: 'incoming', name: 'fixture', module: './extension.mjs' }],
            incoming: { fixture: { type: 'fixture' } }, host: { role: 'ingress' } };
        await writeFile(join(directory, 'config.json'), JSON.stringify(config));
        const cli = new URL('../src/node/cli.mjs', import.meta.url).href;
        const script = `import { main } from ${JSON.stringify(cli)}; console.log(JSON.stringify({ exitCode: await main(process.argv.slice(1)) }));`;
        const result = await execute(process.execPath, ['--input-type=module', '-e', script, '--', 'serve', '--config', join(directory, 'config.json')],
            { timeout: 5000, killSignal: 'SIGKILL', encoding: 'utf8' });
        assert.equal(JSON.parse(result.stdout).exitCode, 130); assert.equal(result.stderr, '');
    } finally { await rm(directory, { recursive: true, force: true }); }
});
