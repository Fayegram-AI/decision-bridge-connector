import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createMemoryStore, createCallbackConnector } from '../src/index.mjs';
import { createHost } from '../src/node/host.mjs';
import { attachLiveController, createLiveSource } from '../src/integrations/live.mjs';
import { bounded } from '../src/util.mjs';

function notification() {
    return { type: 'decision', eventType: 'decision.event', eventId: 'answer',
        correlation: { matched: false, stale: false, sourceEventId: null, delegationId: null, triggerId: null, sourceEventType: null },
        decision: { model: 'gpt-6-luna', answers: [{ type: 'refusal', name: 'ready' }],
            usage: { input_tokens: 1, output_tokens: 0, total_tokens: 1,
                input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 }, output_tokens_details: { reasoning_tokens: 0 } } } };
}
const configuration = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };
function fixture(options = {}) {
    const subscribed = Promise.withResolvers(); let listener; let connections = 0; let closes = 0;
    const controller = { subscribe(callback) { listener = callback; subscribed.resolve(); return () => { listener = undefined; }; }, dispose() {} };
    const source = createLiveSource({ flow: 'a', sessionId: 'host', ...options, async connect() {
        connections++;
        return { controller, closed: new Promise(() => {}), close() { closes++; } };
    } });
    return { source, controller, subscribed: subscribed.promise, emit() { listener(notification()); },
        connections: () => connections, closes: () => closes };
}

test('Live waits for all hosted sources before connecting and admitting a decision', async () => {
    const runner = createRunner(configuration); const outcome = Promise.withResolvers();
    const live = fixture({ onOutcome: outcome.resolve }); const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    const starting = createHost({ runner, role: 'ingress', sources: { live: live.source,
        later: { async start() { entered.resolve(); await release.promise; } } } });
    let host;
    try {
        await entered.promise;
        assert.equal(live.connections(), 0); assert.equal(live.source.status().state, 'waiting_activation');
        assert.equal(live.source.health().ready, false);
        release.resolve(); host = await starting; await live.subscribed; live.emit();
        const observed = await bounded(() => outcome.promise, { timeoutMs: 2000 });
        assert.equal(observed.error, undefined); assert.equal((await runner.list()).items.length, 1);
        assert.equal(live.source.status().attachment.admitted, 1);
        assert.equal(live.source.status().attachment.failed, 0);
    } finally { release.resolve(); host ??= await starting; await host.close(); }
});

test('abandoned Live startup and standalone staged close never connect or consume retries', async () => {
    const runner = createRunner(configuration); const live = fixture();
    try {
        await assert.rejects(createHost({ runner, role: 'ingress', sources: { live: live.source,
            later: { async start() { throw new Error('Later source failed.'); } } } }), /Later source failed/);
        assert.equal(live.connections(), 0); assert.equal(live.source.status().reconnects, 0);
        const staged = fixture(); const activation = Promise.withResolvers();
        await staged.source.start({ runner, whenActive: activation.promise });
        await bounded(() => staged.source.close(), { timeoutMs: 500 });
        activation.reject(new Error('Abandoned activation.'));
        await new Promise(resolve => setImmediate(resolve));
        assert.equal(staged.connections(), 0); assert.equal(staged.source.status().state, 'stopped');
    } finally { await live.source.close(); await runner.close(); }
});

test('Live validates target bindings before hosted worker effects or standalone connection', async () => {
    let effects = 0;
    const runner = createRunner({ ...configuration, evaluators: { e: { revision: '1', execute() { effects++; return 1; } } },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e' } } });
    const guarded = { ...runner, recover() { effects++; } };
    try {
        for (const flow of ['a', 'missing']) {
            const live = fixture({ flow });
            await assert.rejects(createHost({ runner: guarded, sources: { live: live.source } }), { code: 'CONFIGURATION_ERROR' });
            await assert.rejects(live.source.start({ runner }), { code: 'CONFIGURATION_ERROR' });
            assert.equal(live.connections(), 0); await live.source.close();
        }
        assert.equal(effects, 0);
    } finally { await runner.close(); }
});

for (const role of ['ingress', 'all']) test(`Live ${role} respects host execution ownership`, async () => {
    const store = createMemoryStore(); let sends = 0;
    const options = { ...configuration, store, connectors: { sink: createCallbackConnector(() => { sends++; return { status: 'acknowledged' }; }) },
        flows: { a: { ...configuration.flows.a, connections: [{ id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' }] } } };
    const runner = createRunner(options); const outcome = Promise.withResolvers(); const live = fixture({ onOutcome: outcome.resolve });
    const host = await createHost({ runner, role, sources: { live: live.source }, pollMs: 5 });
    let worker;
    try {
        await live.subscribed; live.emit();
        const observed = await bounded(() => outcome.promise, { timeoutMs: 2000 });
        const counters = live.source.status().attachment;
        assert.equal(counters.admitted, 1); assert.equal(counters.failed, 0);
        if (role === 'ingress') {
            assert.equal(sends, 0); assert.equal(counters.completed, 0); assert.equal(observed.report.settled, false);
            worker = createRunner(options);
            assert.equal((await worker.execute(observed.report.taskId)).status, 'completed');
        } else { assert.equal(counters.completed, 1); assert.equal(observed.report.status, 'completed'); }
        assert.equal(sends, 1);
    } finally { await host.close(); await worker?.close(); await store.close(); }
});

test('direct Live attachments keep run as the default and reject unknown processing modes', () => {
    const runner = createRunner(configuration); const live = fixture();
    assert.throws(() => attachLiveController({ runner, controller: live.controller, flow: 'a', processing: 'other' }), { code: 'CONFIGURATION_ERROR' });
    return runner.close();
});
