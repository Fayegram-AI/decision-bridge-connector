import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createIterableSource, consumeSource, createDurableAggregator, ConnectorError } from '../src/index.mjs';
import { createHost } from '../src/node/host.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { bounded } from '../src/util.mjs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const input = { flow: 'a', kind: 'result', result: 1 };
const configuration = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
const wait = promise => bounded(() => promise, { timeoutMs: 3000 });
function oneMessage(message, ended) {
    return { async *open() { try { yield message; } finally { ended.resolve(); } } };
}
async function persistentRunner() {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-host-reply-'));
    const store = createSQLiteStore(join(directory, 'state.sqlite'));
    const runner = createRunner({ ...configuration, store });
    return { runner, store, async close() {
        await runner.close(); await store.close();
        assert(directory.startsWith(join(tmpdir(), 'dbc-host-reply-'))); await rm(directory, { recursive: true, force: true });
    } };
}

for (const mode of ['throws', 'unknown', 'failed']) test(`SDK source reports ${mode} replies without acknowledging completed consumption`, async () => {
    const runner = createRunner(configuration); const outcomes = []; let acknowledgements = 0;
    const message = { input,
        reply(_report, { signal }) {
            assert.equal(this, message); assert(signal instanceof AbortSignal);
            if (mode === 'throws') throw new ConnectorError('REPLY_BROKEN', 'Reply failed.');
            return { status: mode };
        },
        acknowledge() { acknowledgements++; },
    };
    try {
        const summary = await consumeSource(runner, createIterableSource([message], { name: 'reply-source' }),
            { onOutcome: value => { outcomes.push(value); } });
        assert.equal(summary.completed, 0); assert.equal(acknowledgements, 0);
        assert.equal(summary[mode === 'failed' ? 'failed' : 'unknown'], 1);
        assert.equal(outcomes.length, 1); assert.equal(outcomes[0].error.stage, 'reply');
        assert.equal(outcomes[0].error.code, mode === 'throws' ? 'REPLY_BROKEN' : mode === 'unknown' ? 'REPLY_UNKNOWN' : 'REPLY_FAILED');
        assert.equal(outcomes[0].error.ambiguous, mode !== 'failed');
        assert.equal((await runner.get(outcomes[0].taskId)).reply.status, mode === 'failed' ? 'failed' : 'unknown');
    } finally { await runner.close(); }
});

test('hosted source reply methods retain the original message receiver', async () => {
    const runner = createRunner(configuration); const called = Promise.withResolvers(); const ended = Promise.withResolvers();
    const message = { input, reply(report, context) {
        called.resolve({ receiver: this, report, context }); return { status: 'submitted' };
    } };
    let host;
    try {
        host = await createHost({ runner, role: 'ingress', sources: { source: oneMessage(message, ended) } });
        const value = await wait(called.promise); await wait(ended.promise);
        assert.equal(value.receiver, message); assert(value.context.signal instanceof AbortSignal);
        assert.equal(value.report.flow, 'a');
        assert.equal((await runner.get(value.report.taskId)).reply.status, 'submitted');
    } finally { await host?.close(); await runner.close(); }
});

test('hosted reply failures reach diagnostics without rejecting acknowledged admission', async () => {
    const fixture = await persistentRunner(); const { runner } = fixture;
    const ended = Promise.withResolvers(); const errors = []; let acknowledgements = 0; let rejections = 0; let taskId;
    const message = { input,
        acknowledge(report) { assert.equal(this, message); acknowledgements++; taskId = report.taskId; },
        reply() { throw new ConnectorError('REPLY_BROKEN', 'Reply failed.'); },
        reject() { rejections++; },
    };
    let host;
    try {
        host = await createHost({ runner, role: 'ingress', onError: value => { errors.push(value); },
            sources: { source: oneMessage(message, ended) } });
        await wait(ended.promise);
        assert.equal(acknowledgements, 1); assert.equal(rejections, 0);
        const saved = await runner.get(taskId);
        assert.equal(saved.sourceAcknowledgement.status, 'acknowledged'); assert.equal(saved.reply.status, 'unknown');
        assert.equal(errors.length, 1); assert.equal(errors[0].code, 'REPLY_BROKEN'); assert.equal(errors[0].stage, 'reply');
        assert.equal((await host.health()).errors[0].code, 'REPLY_BROKEN');
    } finally { await host?.close(); await fixture.close(); }
});

test('hosted acknowledgement diagnostics preserve their recorded error details', async () => {
    const fixture = await persistentRunner(); const { runner, store } = fixture;
    const ended = Promise.withResolvers(); const errors = []; let taskId; let host;
    const message = { input, acknowledge(report) {
        taskId = report.taskId;
        throw new ConnectorError('ACK_BROKEN', 'Acknowledgement failed.', { retryable: true, ambiguous: true });
    } };
    try {
        host = await createHost({ runner, role: 'ingress', onError: error => { errors.push(error); },
            sources: { source: oneMessage(message, ended) } });
        await wait(ended.promise);
        const saved = (await store.get(taskId, runner.namespace)).task.sourceAcknowledgement;
        assert.equal(saved.status, 'unknown'); assert.equal(saved.error.code, 'ACK_BROKEN');
        assert.equal(errors.length, 1); assert.equal(errors[0].code, saved.error.code);
        assert.equal(errors[0].message, saved.error.message); assert.equal(errors[0].stage, saved.error.stage);
        assert.equal(errors[0].retryable, true); assert.equal(errors[0].ambiguous, true);
        assert.equal((await host.health()).errors[0].code, 'ACK_BROKEN');
    } finally { await host?.close(); await fixture.close(); }
});

test('hosted aggregation replies receive cancellation context before an execution exists', async () => {
    const runner = createRunner({ evaluators: { e: { revision: '1', execute: value => value } },
        flows: { a: { revision: '1', inputs: ['event'], template: { $path: '$' }, evaluator: 'e' } } });
    const aggregator = createDurableAggregator({ runner, rule: { id: 'pair', revision: '1', sources: ['one', 'two'], flow: 'a', windowMs: 60000 } });
    const called = Promise.withResolvers(); const ended = Promise.withResolvers();
    const message = { input: { flow: 'a', kind: 'event', event: { id: 'group' }, idempotencyKey: 'entry' },
        reply(report, context) { called.resolve({ receiver: this, report, context }); return { status: 'submitted' }; },
    };
    let host;
    try {
        host = await createHost({ runner, role: 'ingress', aggregators: { pair: aggregator }, sources: { source: {
            ...oneMessage(message, ended), aggregation: { rule: 'pair', member: 'one', correlationPath: 'id' },
        } } });
        const value = await wait(called.promise); await wait(ended.promise);
        assert.equal(value.receiver, message); assert.equal(value.report.state, 'pending');
        assert(value.context?.signal instanceof AbortSignal); assert.equal(value.context.signal.aborted, false);
        assert.equal((await runner.list()).items.length, 0);
    } finally { await host?.close(); await runner.close(); }
});

test('hosted sources reject invalid reply callbacks before admission', async () => {
    const runner = createRunner(configuration); const ended = Promise.withResolvers(); const rejected = [];
    const message = { input, reply: 123, reject(error) { rejected.push(error); } }; let host;
    try {
        host = await createHost({ runner, role: 'ingress', sources: { source: oneMessage(message, ended) } });
        await wait(ended.promise);
        assert.equal((await runner.list()).items.length, 0); assert.equal(rejected.length, 1);
        assert.equal(rejected[0].code, 'CONFIGURATION_ERROR');
    } finally { await host?.close(); await runner.close(); }
});
