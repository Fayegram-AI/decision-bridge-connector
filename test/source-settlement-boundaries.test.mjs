import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRunner, createMemoryStore, createIterableSource, consumeSource, ConnectorError } from '../src/index.mjs';
import { createSpoolSource } from '../src/node/spool.mjs';
import { bounded, delay } from '../src/util.mjs';

const options = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };

for (const durable of [false, true]) for (const status of ['failed', 'submitted']) {
    test(`SDK ${durable ? 'durable' : 'volatile'} ACK preserves a returned ${status} disposition`, async () => {
        const backing = createMemoryStore(); const outcomes = [];
        const runner = createRunner({ ...options, store: { ...backing, capabilities: { ...backing.capabilities, durable } } });
        try {
            const source = createIterableSource([{ input: { flow: 'a', kind: 'result', result: 1 },
                acknowledge: () => ({ status, error: { code: 'ACK_RECIPIENT', message: 'Recipient evidence.', retryable: false } }) }], { name: 'receipt' });
            const summary = await consumeSource(runner, source, { onOutcome: outcome => outcomes.push(outcome) });
            const ack = outcomes[0].acknowledgement;
            assert.equal(ack.status, status === 'failed' ? 'failed' : 'unknown');
            assert.equal(ack.error.code, 'ACK_RECIPIENT'); assert.equal(ack.error.ambiguous, status !== 'failed');
            assert.equal(summary.acknowledgementFailed, status === 'failed' ? 1 : 0);
            assert.equal(summary.acknowledgementUnknown, status === 'failed' ? 0 : 1);
        } finally { await runner.close(); await backing.close(); }
    });
}

for (const point of ['before', 'after-success', 'after-failure']) test(`SDK ACK ${point} persistence failure preserves external settlement evidence`, async () => {
    const backing = createMemoryStore(); const events = []; const outcomes = [];
    const store = { ...backing, capabilities: { ...backing.capabilities, durable: true }, async recordAcknowledgement(...args) {
        if ((point === 'before') === (args[2].status === 'in_flight'))
            throw new ConnectorError('STORE_UNAVAILABLE', 'Write rejected.', { ambiguous: point === 'before' });
        return backing.recordAcknowledgement(...args);
    } };
    const runner = createRunner({ ...options, store });
    try {
        const source = createIterableSource([{ input: { flow: 'a', kind: 'result', result: 1 }, acknowledge() {
            events.push('ack'); return point === 'after-failure' ? { status: 'failed' } : undefined;
        }, reply: () => ({ status: 'failed' }), reject() { events.push('reject'); } }], { name: 'persistence' });
        await consumeSource(runner, source, { acknowledge: 'admitted', onOutcome: outcome => outcomes.push(outcome) });
        assert.deepEqual(events, point === 'after-success' ? ['ack'] : point === 'before' ? ['reject'] : ['ack', 'reject']);
        assert.equal(outcomes[0].acknowledgement.status, point === 'after-success' ? 'unknown' : 'failed');
        assert.equal(outcomes[0].acknowledgement.error.code, 'STORE_UNAVAILABLE');
    } finally { await runner.close(); await backing.close(); }
});

test('SDK ACK receipt evidence keeps its independent application depth', async () => {
    const runner = createRunner(options); const outcomes = []; let receipt = 1;
    for (let depth = 0; depth < 100; depth++) receipt = [receipt];
    try {
        await consumeSource(runner, createIterableSource([{ input: { flow: 'a', kind: 'result', result: 1 },
            acknowledge: () => ({ status: 'acknowledged', receipt }) }], { name: 'depth' }), { onOutcome: value => outcomes.push(value) });
        assert.equal(outcomes[0].acknowledgement.status, 'acknowledged');
        assert.deepEqual(outcomes[0].acknowledgement.receipt, receipt);
    } finally { await runner.close(); }
});

test('closing a spool during hashing cannot publish a late claim or retain its heartbeat', async t => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-spool-close-hash-'));
    const source = createSpoolSource({ directory, flow: 'a', inputMode: 'result', leaseMs: 90, pollMs: 5 });
    const entered = Promise.withResolvers(); const release = Promise.withResolvers(); const digest = crypto.subtle.digest;
    const mock = t.mock.method(crypto.subtle, 'digest', async function (...args) {
        entered.resolve(); await release.promise; return digest.apply(this, args);
    });
    let iterator; let recovery; let recovered;
    try {
        await writeFile(join(directory, 'one.json'), '1'); iterator = source.open();
        const pending = iterator.next();
        await bounded(() => entered.promise, { timeoutMs: 2000 });
        await source.close({ timeoutMs: 100 }); release.resolve();
        await assert.rejects(pending, { code: 'ABORTED' }); mock.mock.restore();
        await delay(150);
        recovery = createSpoolSource({ directory, flow: 'a', inputMode: 'result', leaseMs: 90, pollMs: 5 });
        recovered = recovery.open();
        const message = await bounded(() => recovered.next(), { timeoutMs: 2000 });
        assert.equal(message.value.input.result, 1); await message.value.acknowledge();
        assert.equal((await readdir(join(directory, '.processing'))).length, 0);
    } finally {
        release.resolve(); mock.mock.restore(); await source.close(); await iterator?.return();
        await recovery?.close(); await recovered?.return(); await rm(directory, { recursive: true, force: true });
    }
});
