import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { createMemoryStore, createCallbackConnector, createDurableAggregator, ConnectorError } from '../src/index.mjs';
import { createRunner, collectExport } from './helpers/runner.mjs';

function setup({ flow = {}, send, store = createMemoryStore(), namespace = 'default' } = {}) {
    let evaluated = 0; const sent = [];
    const runner = createRunner({ namespace, store,
        evaluators: { e: { revision: '1', execute: async request => { evaluated++; return request; } } },
        validators: { v: { revision: '1', validate: value => value } },
        connectors: { sink: createCallbackConnector(send ?? (async value => { sent.push(value); return { status: 'acknowledged' }; })) },
        flows: { a: { revision: '1', inputs: ['request', 'result', 'event'], evaluator: 'e', validator: 'v', template: { $path: '$' },
            connections: [{ id: 'out', connector: 'sink', when: 'evaluation.succeeded', payload: 'raw-result' }], ...flow } } });
    return { runner, sent, evaluated: () => evaluated, store };
}

test('policy reprocessing preserves evaluation identity and redelivery requires an explicit flow', async () => {
    const { runner, sent, evaluated, store } = setup();
    try {
        const original = await runner.run({ flow: 'a', kind: 'request', request: { answer: 2 } });
        const derived = await runner.reprocess(original.taskId); await runner.execute(derived.taskId);
        assert.equal(derived.evaluation.id, original.evaluation.id); assert.equal(evaluated(), 1); assert.equal(sent.length, 1);
        await assert.rejects(runner.redeliver(original.taskId), { code: 'CONFIGURATION_ERROR' });
        const delivery = await runner.redeliver(original.taskId, { flow: 'a' }); await runner.execute(delivery.taskId);
        assert.equal(sent.length, 2); assert.equal(evaluated(), 1);
    } finally { await runner.close(); await store.close(); }
});

test('definite retries reuse payload and delivery identity; unknown effects are held', async () => {
    const attempts = [];
    const { runner, store, evaluated } = setup({ send: async (value, context) => {
        attempts.push({ value, delivery: context.deliveryId, attempt: context.attemptId });
        if (attempts.length === 1) throw new ConnectorError('BUSY', 'Busy.', { retryable: true, ambiguous: false });
        return { status: 'acknowledged' };
    } });
    try {
        assert.equal((await runner.run({ flow: 'a', kind: 'request', request: 'hello' })).status, 'completed');
        assert.equal(evaluated(), 1); assert.equal(attempts.length, 2);
        assert.equal(attempts[0].delivery, attempts[1].delivery); assert.notEqual(attempts[0].attempt, attempts[1].attempt);
    } finally { await runner.close(); await store.close(); }
    const unknown = setup({ send: async () => ({ status: 'unknown' }) });
    try {
        const report = await unknown.runner.run({ flow: 'a', kind: 'result', result: {} });
        assert.equal(report.status, 'unknown'); assert.equal(report.deliveries[0].attempts.length, 1);
        await assert.rejects(unknown.runner.retryDelivery(report.deliveries[0].id));
    } finally { await unknown.runner.close(); await unknown.store.close(); }
});

test('stale results are held and capacity rejects additional admission', async () => {
    const store = createMemoryStore({ maxPending: 1 }); const { runner, sent } = setup({ store, flow: { maxAgeMs: 1000 } });
    try {
        const report = await runner.run({ flow: 'a', kind: 'result', result: {}, evidenceAt: '2020-01-01T00:00:00Z' });
        assert.equal(report.deliveries[0].status, 'held'); assert.equal(sent.length, 0);
        await assert.rejects(runner.submit({ flow: 'a', kind: 'result', result: {} }), { code: 'RESOURCE_LIMIT' });
    } finally { await runner.close(); await store.close(); }
});

test('changed flow revisions cannot claim previously admitted work', async () => {
    const store = createMemoryStore(); const old = setup({ store }); const next = setup({ store, flow: { revision: '2' } });
    try {
        const admitted = await old.runner.submit({ flow: 'a', kind: 'request', request: 'old' });
        assert.equal(await next.runner.tick(), 0); assert.equal(next.evaluated(), 0);
        assert.equal((await old.runner.execute(admitted.taskId)).status, 'completed'); assert.equal(old.evaluated(), 1);
    } finally { await old.runner.close(); await next.runner.close(); await store.close(); }
});

test('aggregation survives object recreation and emits a correlation group once', async () => {
    const { runner, store, sent } = setup();
    const rule = { id: 'pair', revision: '1', sources: ['a', 'b'], windowMs: 10000, flow: 'a' };
    try {
        await createDurableAggregator({ runner, rule }).add({ source: 'a', eventId: '1', correlationKey: 'pair', event: 1 });
        const second = createDurableAggregator({ runner, rule });
        const group = await second.add({ source: 'b', eventId: '2', correlationKey: 'pair', event: 2 });
        assert.equal(group.state, 'complete'); assert.equal(group.emitted, true);
        await second.add({ source: 'b', eventId: '2', correlationKey: 'pair', event: 2 });
        const page = await runner.list(); assert.equal(page.items.length, 1);
        await runner.execute(page.items[0].taskId); assert.equal(sent.length, 1);
    } finally { await runner.close(); await store.close(); }
});

test('export requires explicit import flow and revalidates saved evidence', async () => {
    const { runner, store } = setup();
    try {
        const first = await runner.run({ flow: 'a', kind: 'result', result: 'saved' });
        const exported = await collectExport(runner, first.taskId);
        await assert.rejects(runner.importTask(exported));
        const imported = await runner.importTask(exported, { flow: 'a' });
        assert.equal(imported.evaluation.result, 'saved');
        await assert.rejects(runner.importTask(exported.slice(0, -1), { flow: 'a' }));
    } finally { await runner.close(); await store.close(); }
});

test('caller reply failure does not change the evaluation or repeat delivery', async () => {
    const { runner, store, sent } = setup();
    try {
        const first = await runner.run({ flow: 'a', kind: 'request', request: 1 });
        await runner.reply(first.taskId, () => { throw Error('Disconnected'); });
        const current = await runner.get(first.taskId);
        assert.equal(current.status, 'completed'); assert.equal(current.reply.status, 'unknown'); assert.equal(sent.length, 1);
    } finally { await runner.close(); await store.close(); }
});
