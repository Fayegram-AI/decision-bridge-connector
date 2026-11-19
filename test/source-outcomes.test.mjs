import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createIterableSource, consumeSource } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';

const configuration = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
const input = { flow: 'a', kind: 'result', result: 1 };
const stores = [['sqlite', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-source-outcomes-'));
    return { store: createSQLiteStore(join(directory, 'state.sqlite')), cleanup: () => rm(directory, { recursive: true }) };
}], ...(process.env.DBC_POSTGRES_URL ? [['postgres', async () => ({
    store: await createPostgresStore({ connectionString: process.env.DBC_POSTGRES_URL }), cleanup: async () => {},
})]] : [])];

for (const [name, factory] of stores) test(`${name}: SDK source acknowledgements persist attempts, receipts and uncertain outcomes`, { timeout: 15000 }, async () => {
    const { store, cleanup } = await factory();
    const runner = createRunner({ ...configuration, store, namespace: `source-${crypto.randomUUID()}` });
    try {
        for (const milestone of ['admitted', 'completed']) {
            for (const mode of ['acknowledged', 'unknown', 'lost']) {
                let calls = 0; const outcomes = [];
                const message = { input, async acknowledge(context) {
                    assert.equal(this, message); assert.equal(context.milestone, milestone);
                    assert(context.signal instanceof AbortSignal); assert(context.taskId);
                    const saved = await runner.get(context.taskId);
                    assert.equal(saved.sourceAcknowledgement.status, 'in_flight');
                    calls++;
                    if (mode === 'lost') throw new Error('Acknowledgement response lost.');
                    return { status: mode, receipt: { sourceId: 'one' } };
                } };
                const result = await consumeSource(runner, createIterableSource([message], { name: 'source' }),
                    { acknowledge: milestone, onOutcome: outcome => { outcomes.push(outcome); } });
                assert.equal(result.completed, 1); assert.equal(calls, 1);
                const status = mode === 'acknowledged' ? 'acknowledged' : 'unknown';
                assert.equal(result[status === 'acknowledged' ? 'acknowledged' : 'acknowledgementUnknown'], 1);
                const saved = (await runner.get(outcomes[0].taskId)).sourceAcknowledgement;
                assert.equal(saved.status, status); assert(saved.attemptId);
                assert.equal(saved.attemptId, outcomes[0].acknowledgement.attemptId);
                if (mode !== 'lost') assert.deepEqual(saved.receipt, { sourceId: 'one' });
                await runner.tick(); assert.equal(calls, 1);
            }
        }
    } finally { await runner.close(); await store.close(); await cleanup(); }
});

test('source backpressure includes the configured caller reply budget', { timeout: 8000 }, async () => {
    const runner = createRunner(configuration); const outcomes = []; let replied = 0; let acknowledged = 0;
    const messages = [1, 2].map(result => ({ input: { ...input, result },
        async reply(report, { signal }) {
            assert(signal instanceof AbortSignal); assert.equal(report.status, 'completed');
            await sleep(1300, undefined, { signal }); replied++; return { status: 'acknowledged' };
        }, acknowledge() { acknowledged++; },
    }));
    try {
        const result = await consumeSource(runner, createIterableSource(messages, { name: 'replies' }), { concurrency: 1,
            processingTimeoutMs: 100, acknowledgementTimeoutMs: 10, observerTimeoutMs: 10, replyTimeoutMs: 2000,
            onOutcome: (outcome, { signal }) => { assert(signal instanceof AbortSignal); outcomes.push(outcome); } });
        assert.equal(result.stopped, 'exhausted'); assert.equal(result.received, 2);
        assert.equal(result.sourceErrors, 0); assert.equal(result.completed, 2);
        assert.equal(replied, 2); assert.equal(acknowledged, 2); assert.equal(outcomes.length, 2);
    } finally { await runner.close(); }
});

test('source cancellation reaches a pending caller reply', { timeout: 5000 }, async () => {
    const runner = createRunner(configuration); const controller = new AbortController(); const entered = Promise.withResolvers();
    let replySignal; let acknowledged = 0;
    const source = createIterableSource([{ input, reply(report, { signal }) {
        replySignal = signal; entered.resolve(); return sleep(10000, undefined, { signal });
    }, acknowledge() { acknowledged++; } }], { name: 'cancelled-reply' });
    try {
        const pending = consumeSource(runner, source, { signal: controller.signal, replyTimeoutMs: 2000 });
        await entered.promise; controller.abort();
        assert.equal((await pending).stopped, 'cancelled');
        assert.equal(replySignal.aborted, true); assert.equal(acknowledged, 0);
    } finally { controller.abort(); await runner.close(); }
});

test('a reply path that ignores its deadline cannot stall the source consumer', { timeout: 5000 }, async () => {
    const runner = createRunner(configuration); const outcomes = [];
    const source = createIterableSource([1, 2].map(result => ({ input: { ...input, result }, reply() {} })), { name: 'stalled-reply' });
    try {
        const result = await consumeSource({ ...runner, reply: () => new Promise(() => {}) }, source,
            { concurrency: 1, replyTimeoutMs: 25, onOutcome: outcome => { outcomes.push(outcome); } });
        assert.equal(result.stopped, 'exhausted'); assert.equal(result.received, 2); assert.equal(result.unknown, 2);
        assert(outcomes.every(outcome => outcome.error.code === 'TIMEOUT' && outcome.error.stage === 'reply'));
    } finally { await runner.close(); }
});
