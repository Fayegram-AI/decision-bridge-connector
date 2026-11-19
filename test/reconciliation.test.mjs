import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as sleep } from 'node:timers/promises';
import { createCallbackConnector, ConnectorError, createDurableAggregator, createIterableSource, consumeSource } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createHost } from '../src/node/host.mjs';

test('operator reconciliation resumes unknown evaluation from validated evidence without reinference', async () => {
    let calls = 0;
    const runner = createRunner({ evaluators: { e: { revision: '1', async execute() { calls++; throw new ConnectorError('TIMEOUT', 'Response missing.'); }, validateSaved: value => value } },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e' } } });
    try {
        const task = await runner.run({ flow: 'a', kind: 'request', request: {} });
        assert.equal(task.status, 'unknown');
        await assert.rejects(runner.resolveEvaluation(task.taskId, { status: 'succeeded', result: 1 }));
        await runner.resolveEvaluation(task.taskId, { status: 'succeeded', result: { recovered: true }, reason: 'Provider receipt verified' });
        assert.equal((await runner.execute(task.taskId)).status, 'completed'); assert.equal(calls, 1);
    } finally { await runner.close(); }
});

test('required failure output is delivered before run returns an evaluation failure', async () => {
    let received = 0;
    const runner = createRunner({ evaluators: { e: { revision: '1', async execute() { throw new ConnectorError('INVALID_RESPONSE', 'Invalid.', { ambiguous: false }); } } },
        connectors: { alert: createCallbackConnector(async () => { received++; return { status: 'acknowledged' }; }) },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e', connections: [{ id: 'failure', connector: 'alert', when: 'failure', payload: 'failure' }] } } });
    try {
        const task = await runner.run({ flow: 'a', kind: 'request', request: {} });
        assert.equal(task.status, 'failed'); assert.equal(task.evaluation.status, 'failed'); assert.equal(received, 1);
    } finally { await runner.close(); }
});

test('lost source acknowledgement is retained independently from successful execution and reply', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-ack-')); const store = createSQLiteStore(join(directory, 'state.sqlite'));
    const runner = createRunner({ store, validators: { v: { revision: '1', validate: value => value } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    const host = await createHost({ runner, pollMs: 5, sources: { test: { async *open() {
        yield { input: { flow: 'a', kind: 'result', result: 1 }, acknowledge() { throw Error('Lost ACK'); }, reply: () => ({ status: 'acknowledged' }) };
    } } } });
    try {
        let task;
        for (let i = 0; i < 100; i++) { task = (await runner.list()).items[0]; if (task?.status === 'completed' && task.reply.status === 'acknowledged') break; await sleep(10); }
        assert.equal(task.status, 'completed'); assert.equal(task.sourceAcknowledgement.status, 'unknown'); assert.equal(task.reply.status, 'acknowledged');
        await store.recordReply(runner.namespace, task.taskId, { status: 'in_flight', attemptId: 'crashed', expiresAt: Date.now() - 1 });
        await runner.recover(); assert.equal((await runner.get(task.taskId)).reply.status, 'unknown');
    } finally { await host.close(); await store.close(); await rm(directory, { recursive: true }); }
});

test('durable aggregation rejects late members and flushes explicit incomplete evidence', async () => {
    const runner = createRunner({ evaluators: { e: { revision: '1', execute: async value => value } },
        flows: { a: { revision: '1', inputs: ['event'], evaluator: 'e', template: { $path: '$' } } } });
    const aggregate = createDurableAggregator({ runner, rule: { id: 'pair', revision: '1', flow: 'a', sources: ['one', 'two'], windowMs: 10, incomplete: 'emit' } });
    try {
        await aggregate.add({ source: 'one', eventId: '1', correlationKey: 'case', event: 1 }); await sleep(15);
        await assert.rejects(aggregate.add({ source: 'two', eventId: '2', correlationKey: 'case', event: 2 }), { code: 'LATE_EVENT' });
        await aggregate.flush(); assert.equal((await runner.list()).items.length, 1);
    } finally { await runner.close(); }
});

test('iterable sources use the same flow admission and caller reply contracts', async () => {
    let replies = 0;
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } }, flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    try {
        const source = createIterableSource([{ input: { flow: 'a', kind: 'result', result: 1 }, reply() { replies++; return { status: 'acknowledged' }; } }], { name: 'iterable' });
        await consumeSource(runner, source, { acknowledge: 'none' });
        assert.equal(replies, 1); assert.equal((await runner.list()).items[0].status, 'completed');
    } finally { await runner.close(); }
});
