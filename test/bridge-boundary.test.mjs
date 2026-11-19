import test from 'node:test';
import assert from 'node:assert/strict';
import { createBridge, createProvider } from '@fayegram-ai/decision-bridge';
import { CloudflareExtractionProvider, OpenRouterExtractionProvider } from '@fayegram-ai/decision-bridge/extraction';
import { createDecisionRouter } from '@fayegram-ai/decision-bridge/routing';
import { createLiveDecisions } from '@fayegram-ai/decision-bridge/live';
import { decisionEvaluator, extractionEvaluator, routedDecisionEvaluator } from '../src/index.mjs';
import { createRunner, createConfiguredRunner } from './helpers/runner.mjs';
import { attachLiveController, createLiveSource, validateLiveDecisionNotification } from '../src/integrations/live.mjs';
import { setTimeout as sleep } from 'node:timers/promises';

const json = body => new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });
const request = { state: 'Synthetic evidence.', questions: { ready: { type: 'noul', instructions: 'Ready?' } } };
const runnerFor = (evaluator, operation = 'decision') => createRunner({ evaluators: { e: evaluator },
    flows: { main: { revision: '1', inputs: ['request', ...(evaluator.validateSaved ? ['result'] : [])], evaluator: 'e', operation } } });

test('configured native decisions retain provider rounding policy during saved admission', async t => {
    const criteria = Object.fromEntries(Array.from({ length: 32 }, (_, i) => ['option_' + i, 'Option ' + i]));
    t.mock.method(globalThis, 'fetch', async () => json({ model: 'served', usage: { input_tokens: 1, output_tokens: 0 },
        answers: { pick: { type: 'choice', choice: 'option_0', probabilities: Object.fromEntries(Object.keys(criteria).map(k => [k, 0.0313])) } } }));
    const runner = await createConfiguredRunner({ schemaVersion: 3,
        evaluators: { native: { type: 'decision', options: { provider: { type: 'openrouter-decisions', model: 'vendor/model', apiKey: 'fixture' } } } },
        flows: { main: { revision: '1', inputs: ['request', 'result'], evaluator: 'native', connections: [] } } });
    const request = { state: 'Evidence', questions: { pick: { type: 'choice', instructions: 'Choose', criteria } } };
    try {
        const first = await runner.run({ flow: 'main', kind: 'request', request });
        assert.equal(first.status, 'completed');
        const saved = await runner.run({ flow: 'main', kind: 'result', result: first.evaluation.result, validation: { request } });
        assert.equal(saved.status, 'completed');
    } finally { await runner.close(); }
});

test('OpenRouter extraction survives execution, saved admission and reprocessing without another call', async () => {
    let calls = 0;
    const provider = new OpenRouterExtractionProvider({ model: 'vendor/extract', apiKey: 'fixture', fetchImpl: async () => {
        calls++; return json({ model: 'served', answers: { name: { type: 'text', value: 'Ada' }, future: { type: 'forecast', value: [1, 2] } },
            usage: { input_tokens: 1, output_tokens: 0 }, state_text: null });
    } });
    const evaluator = extractionEvaluator(provider);
    const runner = runnerFor(evaluator, 'extraction');
    const request = { model: ' vendor/extract ', state: 'Ada', fields: { name: { type: 'text' },
        future: { type: 'forecast', min: 0, max: 10, horizon: 2, interval: 'day' } } };
    try {
        const first = await runner.run({ flow: 'main', kind: 'request', request });
        assert.equal(first.status, 'completed');
        assert.equal(first.evaluation.result.extraction.truncated, undefined);
        assert.equal(first.evaluation.result.extraction.state_text, null);
        const saved = await runner.run({ flow: 'main', kind: 'result', result: first.evaluation.result, validation: { request } });
        assert.equal(saved.status, 'completed');
        await runner.reprocess(first.taskId); assert.equal(calls, 1);
        const wrong = structuredClone(first.evaluation.result); wrong.meta.provider = 'cloudflare-extraction';
        assert.throws(() => evaluator.validateSaved(wrong, request), { code: 'INVALID_SAVED_OUTPUT' });
        const malformed = structuredClone(first.evaluation.result); malformed.extraction.answers.future.value = 'invalid forecast';
        assert.throws(() => evaluator.validateSaved(malformed, request), { code: 'INVALID_SAVED_OUTPUT' });
        assert.equal(calls, 1);
    } finally { await runner.close(); }
});

test('published Bridge preserves native model provenance and validates saved decisions offline', async () => {
    let calls = 0;
    const bridge = createBridge({ provider: createProvider({ type: 'openai-decisions', apiKey: 'test-only', fetchImpl: async (_url, init) => {
        calls++; const input = JSON.parse(init.body);
        return json({ model: input.model, answers: [{ type: 'predicate', name: 'ready', probability: 0.7 }],
            usage: { input_tokens: 2, output_tokens: 0, total_tokens: 2, input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 }, output_tokens_details: { reasoning_tokens: 0 } } });
    } }) });
    const runner = runnerFor(decisionEvaluator(bridge));
    try {
        const input = { flow: 'main', kind: 'request', request: { ...request, model: 'default' } };
        await runner.preview(input); assert.equal(calls, 0);
        const first = await runner.run(input); assert.equal(first.status, 'completed');
        assert.equal(first.evaluation.result.meta.requestedModel, 'default');
        assert.equal(first.evaluation.result.answers.ready.noul, 0.7);
        await runner.reprocess(first.taskId); assert.equal(calls, 1);
        const saved = await runner.run({ flow: 'main', kind: 'result', result: first.evaluation.result, validation: { request: input.request } });
        const imported = await runner.importTask(await runner.exportTask(saved.taskId), { flow: 'main' });
        assert.equal((await runner.execute(imported.taskId)).status, 'completed'); assert.equal(calls, 1);
    } finally { await runner.close(); }
});

test('published extraction adapter preserves native fields and saved result validation does not infer', async () => {
    let calls = 0;
    const native = { model: 'clairic', answers: { total: { type: 'number', value: 4, confidence: 0.8 } },
        truncated: false, usage: { input_tokens: 1, output_tokens: 0 }, vendor_extra: { retained: true } };
    const provider = new CloudflareExtractionProvider({ apiKey: 'test-only', accountId: 'test-only', fetchImpl: async () => { calls++; return json({ success: true, result: native }); } });
    const runner = runnerFor(extractionEvaluator(provider), 'extraction');
    try {
        const first = await runner.run({ flow: 'main', kind: 'request', request: { state: 'total 4', fields: { total: { type: 'number' } } } });
        assert.equal(first.status, 'completed'); assert.deepEqual(first.evaluation.result.extraction, native);
        await runner.reprocess(first.taskId); assert.equal(calls, 1);
    } finally { await runner.close(); }
});

test('published routing diagnostics survive the connector boundary', async () => {
    const provider = name => ({ name, kind: 'decision', defaultModel: 'test', probabilitySource: 'native',
        async evaluate() { return { model: 'test', answers: { ready: { type: 'noul', noul: 0.9 } }, usage: { input_tokens: 1, output_tokens: 0 } }; } });
    const router = createDecisionRouter({ defaultRoute: 'default', policyRevision: 'r1', routes: { default: { provider: provider('default') }, selected: { provider: provider('selected') } } });
    const runner = runnerFor(routedDecisionEvaluator(router, { callOptions: { route: 'selected' } }));
    try {
        const result = await runner.run({ flow: 'main', kind: 'request', request });
        assert.equal(result.status, 'completed'); assert.equal(result.evaluation.result.meta.routing.selectedRoute, 'selected');
        assert.equal(result.evaluation.result.meta.probabilitySource, 'native');
    } finally { await runner.close(); }
});

function liveFixture() {
    let listener; let disposed = false;
    const controller = createLiveDecisions({ transport: { send() {}, subscribe(fn) { listener = fn; return () => { disposed = true; }; } },
        configuration: { triggers: [{ type: 'decisions', event_type: 'session.input_transcript.delta', decisions: { questions: [{ type: 'predicate', name: 'ready', instructions: 'Ready?' }] } }] } });
    return { controller, emit(event) { listener({ kind: 'event', event }); }, disposed: () => disposed };
}

test('Live correlation, native refusal and borrowed ownership survive result admission', async () => {
    const f = liveFixture(); const reports = [];
    const runner = createRunner({ validators: { live: { revision: '1', validate: value => ({ notification: validateLiveDecisionNotification(value.notification) }) } },
        flows: { main: { revision: '1', operation: 'live', inputs: ['result'], validator: 'live' } } });
    const attachment = attachLiveController({ controller: f.controller, runner, flow: 'main', sessionId: 'stable', onOutcome: value => reports.push(value) });
    const decision = { model: 'gpt-6-luna', future: { absent: null }, answers: [{ type: 'refusal', name: 'ready', future: [0, null] }],
        usage: { input_tokens: 1, output_tokens: 0, total_tokens: 1,
            input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0,
                cache_write_tokens_details: { ttl_30m_tokens: 0, ttl_12h_tokens: 0, future: null } },
            output_tokens_details: { reasoning_tokens: 0 }, future: { zero: 0 } } };
    try {
        f.emit({ type: 'session.input_transcript.delta', event_id: 'source', delta: 'hello', start_ms: 0, end_ms: 1 });
        const event = { type: 'decision.event', event_id: 'answer', delegation_id: null, trigger_id: 'trigger', source_event_id: 'source', source_event_type: 'session.input_transcript.delta', event: { type: 'decision.completed', decision } };
        f.emit(event); f.emit(event); await attachment.drain();
        assert.equal(reports.length, 1); assert.equal(reports[0].report.status, 'completed');
        assert.deepEqual(reports[0].report.evaluation.result.notification.decision, decision);
        assert.equal(reports[0].report.evaluation.result.notification.correlation.matched, true);
        await attachment.stop(); assert.equal(f.disposed(), false);
    } finally { await attachment.stop(); f.controller.dispose(); await runner.close(); }
});

test('Live reconnects owned controllers within a finite retry budget', async () => {
    let calls = 0; let closed = 0;
    const source = createLiveSource({ flow: 'main', sessionId: 'stable', reconnect: { maxAttempts: 2, delayMs: 1 },
        async connect() { calls++; return { controller: liveFixture().controller, closed: Promise.resolve(), close() { closed++; } }; } });
    await source.start({ runner: { ready: Promise.resolve({ main: { inputs: ['result'] } }), run() {} } });
    for (let i = 0; i < 100 && source.status().state !== 'exhausted'; i++) await sleep(10);
    assert.equal(calls, 2); assert.equal(closed, 2); assert.equal(source.status().state, 'exhausted'); await source.close();
});
