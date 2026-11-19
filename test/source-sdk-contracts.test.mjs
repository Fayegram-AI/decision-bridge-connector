import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRunner, consumeSource } from '../src/index.mjs';
import { createIncoming } from '../src/node/incoming.mjs';
import { createSpoolSource } from '../src/node/spool.mjs';
import { createRedisSource } from '../src/integrations/redis-source.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { bounded, delay } from '../src/util.mjs';

const options = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };

test('built-in file sources can be consumed directly with validated routing', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-sdk-source-')); const runner = createRunner(options);
    try {
        await writeFile(join(directory, 'input.json'), '42');
        const source = await createIncoming('file', { path: 'input.json', flow: 'a', inputMode: 'result', format: 'json' }, { baseDir: directory });
        const report = await consumeSource(runner, source);
        assert.equal(report.completed, 1); assert.equal(report.received, 1);
        const invalid = await createIncoming('file', { path: 'missing.json', flow: 'missing', inputMode: 'result' }, { baseDir: directory });
        await assert.rejects(consumeSource(runner, invalid), /target flow/);
        await assert.rejects(consumeSource(runner, createSpoolSource({ directory, flow: 'a' })), /durable admission/);
    } finally { await runner.close(); await rm(directory, { recursive: true, force: true }); }
});

test('Redis source bounds and clients fail before any commands', () => {
    let commands = 0; const base = { flow: 'a', stream: 'events', group: 'workers', client: { sendCommand() { commands++; } } };
    for (const pollMs of [0, -1, 2147483648, NaN, 0.1]) assert.throws(() => createRedisSource({ ...base, pollMs }), /pollMs/);
    for (const claimIdleMs of [-1, NaN, 0.1, Number.MAX_SAFE_INTEGER + 1]) assert.throws(() => createRedisSource({ ...base, claimIdleMs }), /claimIdleMs/);
    assert.throws(() => createRedisSource({ ...base, client: {} }), /sendCommand/);
    assert.equal(commands, 0); assert.equal(createRedisSource({ ...base, claimIdleMs: 0 }).revision, '1');
});

test('concurrent spool consumption holds heartbeats until ACK and archives each input once', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-sdk-spool-')); const control = new AbortController();
    const store = createSQLiteStore(join(directory, 'state.sqlite')); const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    let validations = 0;
    const runner = createRunner({ ...options, store, validators: { v: { revision: '1', async validate(value) {
        if (++validations === 2) entered.resolve(); await release.promise; return value;
    } } } });
    const source = createSpoolSource({ directory, flow: 'a', inputMode: 'result', pollMs: 5, leaseMs: 60 });
    try {
        await writeFile(join(directory, 'one.json'), '1'); await writeFile(join(directory, 'two.json'), '2');
        let acknowledged = 0;
        const consumption = consumeSource(runner, source, { signal: control.signal, acknowledge: 'admitted', concurrency: 2,
            onOutcome(outcome) { if (outcome.acknowledgement.status === 'acknowledged' && ++acknowledged === 2) control.abort(); } });
        await bounded(() => entered.promise, { timeoutMs: 3000 });
        await delay(160); assert.equal(validations, 2);
        release.resolve(); await bounded(() => consumption, { timeoutMs: 5000 });
        assert.equal((await readdir(join(directory, '.processed'))).length, 2);
        assert.equal((await readdir(join(directory, '.processing'))).length, 0);
    } finally { release.resolve(); control.abort(); await runner.close(); await store.close(); await rm(directory, { recursive: true, force: true }); }
});
