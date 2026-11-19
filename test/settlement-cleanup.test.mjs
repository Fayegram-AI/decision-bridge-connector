import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createMemoryStore, ConnectorError } from '../src/index.mjs';
import { createHost } from '../src/node/host.mjs';
import { createResourceOwner } from '../src/node/resources.mjs';
import { bounded } from '../src/util.mjs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { main } from '../src/node/cli.mjs';
import { createConfiguredRunner } from '../src/node/configuration.mjs';

const configuration = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };

for (const failure of ['reply reservation', 'ack settlement']) test(`host does not reject after ACK when ${failure} fails`, async () => {
    const backing = createMemoryStore(); const ended = Promise.withResolvers(); const events = [];
    // Inject a durable-store channel failure without depending on filesystem timing.
    const store = { ...backing, capabilities: { ...backing.capabilities, durable: true },
        async recordAcknowledgement(...args) {
            if (failure === 'ack settlement' && args[2].status !== 'in_flight') throw new ConnectorError('STORE_UNAVAILABLE', 'Settlement failed.');
            return backing.recordAcknowledgement(...args);
        },
        async recordReply(...args) {
            if (failure === 'reply reservation') throw new ConnectorError('STORE_UNAVAILABLE', 'Reservation failed.');
            return backing.recordReply(...args);
        } };
    const runner = createRunner({ ...configuration, store });
    const host = await createHost({ runner, role: 'ingress', onError: error => events.push(error.code), sources: { source: {
        async *open() { try { yield { input: { flow: 'a', kind: 'result', result: 1 },
            acknowledge() { events.push('ack'); }, reply() { events.push('reply'); }, reject() { events.push('reject'); } };
        } finally { ended.resolve(); } },
    } } });
    try {
        await bounded(() => ended.promise, { timeoutMs: 3000 });
        assert.deepEqual(events, ['ack', 'STORE_UNAVAILABLE']);
    } finally { await host.close(); await backing.close(); }
});

for (const status of ['failed', 'unknown']) test(`host reports a returned ${status} acknowledgement without error details`, async () => {
    const backing = createMemoryStore(); const ended = Promise.withResolvers(); const errors = [];
    const runner = createRunner({ ...configuration, store: { ...backing, capabilities: { ...backing.capabilities, durable: true } } });
    const host = await createHost({ runner, role: 'ingress', onError: error => errors.push(error), sources: { source: {
        async *open() { try { yield { input: { flow: 'a', kind: 'result', result: 1 }, acknowledge: () => ({ status }) }; }
            finally { ended.resolve(); } },
    } } });
    try {
        await bounded(() => ended.promise, { timeoutMs: 3000 });
        assert.equal(errors.length, 1); assert.equal(errors[0].code, status === 'failed' ? 'ACK_FAILED' : 'ACK_UNKNOWN');
        assert.equal(errors[0].ambiguous, status === 'unknown'); assert.equal((await host.health()).ready, true);
    } finally { await host.close(); await backing.close(); }
});

for (const ambiguous of [true, false]) test(`aggregation ACK failure preserves its ${ambiguous ? 'uncertain' : 'definite'} settlement before a task exists`, async () => {
    const backing = createMemoryStore(); const ended = Promise.withResolvers(); const errors = []; let rejected = 0;
    const runner = createRunner({ ...configuration, store: { ...backing, capabilities: { ...backing.capabilities, durable: true } } });
    const host = await createHost({ runner, role: 'ingress', onError: error => errors.push(error), aggregators: {
        pair: { async add() { return { state: 'pending' }; } },
    }, sources: { source: { aggregation: { rule: 'pair', correlationPath: 'id' }, async *open() {
        try { yield { input: { kind: 'event', event: { id: 'group' }, idempotencyKey: 'member' },
            acknowledge() { throw new ConnectorError('ACK_BROKEN', 'ACK failed.', { ambiguous }); }, reject() { rejected++; } }; }
        finally { ended.resolve(); }
    } } } });
    try {
        await bounded(() => ended.promise, { timeoutMs: 3000 });
        assert.equal(rejected, ambiguous ? 0 : 1); assert.equal(errors.length, 1); assert.equal(errors[0].code, 'ACK_BROKEN');
    } finally { await host.close(); await backing.close(); }
});

test('resource cleanup attempts every resource once and preserves all failures', async () => {
    const owner = createResourceOwner(); const calls = [];
    for (const name of ['one', 'two', 'good']) await owner.create(() => ({ close() {
        calls.push(name); if (name !== 'good') throw new ConnectorError('CLOSE_FAILED', name);
    } }));
    const first = await owner.closeAll().catch(error => error);
    assert(first instanceof AggregateError); assert.equal(first.errors.length, 2);
    await assert.rejects(owner.closeAll(), AggregateError);
    assert.deepEqual(calls, ['good', 'two', 'one']);
});

test('CLI cleanup failures are sanitized operational errors and startup retains its primary cause', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-close-cli-')); let stdout = ''; let stderr = '';
    const config = { schemaVersion: 3, extensions: [{ kind: 'outgoing', name: 'owned', module: './owned.mjs' }],
        connectors: { out: { type: 'owned' } }, validators: { v: { type: 'schema', options: { schema: {} } } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
    try {
        await writeFile(join(directory, 'owned.mjs'), `export default {apiVersion:1,revision:'1',schema:{},create(){return {
name:'owned',send(){return {status:'acknowledged'};},close(){throw new Error('private cleanup secret');}};}};`);
        await writeFile(join(directory, 'config.json'), JSON.stringify(config));
        const exit = await main(['run', '--config', join(directory, 'config.json')], {
            stdin: Readable.from([JSON.stringify({ flow: 'a', kind: 'result', result: 1 })]),
            stdout: new Writable({ write(chunk, _encoding, done) { stdout += chunk; done(); } }),
            stderr: new Writable({ write(chunk, _encoding, done) { stderr += chunk; done(); } }),
        });
        assert.equal(exit, 1); assert(stdout.includes('completed')); assert(stderr.includes('RESOURCE_CLOSE_FAILED'));
        assert(!stderr.includes('private cleanup secret'));
        const invalid = { ...config, flows: { a: { ...config.flows.a, validator: 'absent' } } };
        await assert.rejects(createConfiguredRunner(invalid, { baseDir: directory }), error =>
            error instanceof AggregateError && error.cause.code === 'CONFIGURATION_ERROR' && error.errors.length === 2);
    } finally { await rm(directory, { recursive: true, force: true }); }
});
