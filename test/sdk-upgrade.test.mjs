import test from 'node:test';
import assert from 'node:assert/strict';
import { createConfiguredRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { createBridge, createProvider } from '@fayegram-ai/decision-bridge';
import { createGenerativeProvider } from '@fayegram-ai/decision-bridge/custom/generative';
import { createLiveDecisions } from '@fayegram-ai/decision-bridge/live';
import { attachLiveController } from '../src/integrations/live.mjs';
import { decisionEvaluator } from '../src/index.mjs';
import { createRunner, collectExport } from './helpers/runner.mjs';

const request = { state: 'Fixture evidence', questions: { ready: { type: 'noul', instructions: 'Ready?' } } };
const config = (type, provider = {}) => ({ schemaVersion: 3,
    evaluators: { e: { type: type.endsWith('-extraction') ? 'extraction' : 'decision',
        options: { provider: { type, ...provider } } } },
    flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e',
        operation: type.endsWith('-extraction') ? 'extraction' : 'decision' } },
});
const types = [
    ['jev', 'TYPESAFE_API_KEY'], ['openai-decisions', 'OPENAI_API_KEY'],
    ['gemini-decisions', 'GEMINI_API_KEY'], ['openrouter-decisions', 'OPENROUTER_API_KEY'],
    ['openrouter-extraction', 'OPENROUTER_API_KEY'], ['cloudflare-decisions', 'CLOUDFLARE_API_TOKEN'],
    ['cloudflare-extraction', 'CLOUDFLARE_API_TOKEN'], ['openai-compatible', 'OPENAI_API_KEY'],
    ['openrouter', 'OPENROUTER_API_KEY'], ['anthropic', 'ANTHROPIC_API_KEY'],
];

test('configured providers resolve aliases within the supplied environment and retain exact references', async t => {
    for (const [type, name] of types) await t.test(type, async () => {
        const provider = { model: type.startsWith('openrouter') ? 'vendor/model'
            : type === 'anthropic' ? 'claude-sonnet-4-5' : type === 'openai-compatible' ? 'fixture-model' : undefined };
        // Configuration is JSON: genuinely unset options must be omitted.
        if (provider.model === undefined) delete provider.model;
        const accounts = type.startsWith('cloudflare-') ? { DECISION_BRIDGE_CLOUDFLARE_ACCOUNT_ID: 'fixture-account' } : {};
        const start = async (options, env) => {
            const runner = await createConfiguredRunner(config(type, { ...provider, ...options }), { env });
            await runner.close();
        };
        await start({}, { ...accounts, [name]: '', [`DECISION_BRIDGE_${name}`]: 'scoped-fixture' });
        await start({}, { ...accounts, [name]: 'conventional-fixture' });
        await assert.rejects(start({}, { ...accounts, [name]: 'conventional-fixture', [`DECISION_BRIDGE_${name}`]: '' }));
        await start({ apiKey: 'explicit-fixture' }, { ...accounts, [`DECISION_BRIDGE_${name}`]: '' });
        await start({ apiKey: { $env: name } }, { ...accounts, [name]: 'selected-fixture', [`DECISION_BRIDGE_${name}`]: '' });
        await assert.rejects(start({ apiKey: { $env: name } }, { ...accounts, [`DECISION_BRIDGE_${name}`]: 'scoped-fixture' }),
            { code: 'CREDENTIAL_UNAVAILABLE' });
        await assert.rejects(start({ baseURL: 'https://third-party.invalid' }, { ...accounts, [name]: 'conventional-fixture' }));
    });
});

test('injected environments cannot borrow ambient provider credentials or partial Cloudflare pairs', async t => {
    for (const name of ['OPENAI_API_KEY', 'DECISION_BRIDGE_OPENAI_API_KEY', 'OPENROUTER_API_KEY', 'DECISION_BRIDGE_OPENROUTER_API_KEY']) {
        const before = process.env[name]; process.env[name] = 'ambient-fixture';
        t.after(() => { if (before === undefined) delete process.env[name]; else process.env[name] = before; });
    }
    await assert.rejects(createConfiguredRunner(config('openai-decisions'), { env: {} }));
    await assert.rejects(createConfiguredRunner(config('gemini-decisions'), { env: { GOOGLE_API_KEY: 'not-an-alias' } }));
    for (const type of ['cloudflare-decisions', 'cloudflare-extraction']) {
        await assert.rejects(createConfiguredRunner(config(type), { env: { CLOUDFLARE_API_TOKEN: 'fixture' } }));
        await assert.rejects(createConfiguredRunner(config(type), { env: { CLOUDFLARE_ACCOUNT_ID: 'fixture' } }));
        await assert.rejects(createConfiguredRunner(config(type), { env: { CLOUDFLARE_API_TOKEN: 'fixture',
            CLOUDFLARE_ACCOUNT_ID: 'valid-fixture', DECISION_BRIDGE_CLOUDFLARE_ACCOUNT_ID: '' } }));
        const runner = await createConfiguredRunner(config(type, { accountId: { $env: 'CHOSEN_ACCOUNT' } }), {
            env: { CLOUDFLARE_API_TOKEN: 'fixture', CHOSEN_ACCOUNT: 'fixture', DECISION_BRIDGE_CLOUDFLARE_ACCOUNT_ID: '' },
        });
        await runner.close();
    }
});

test('lazy decision and extraction routes capture aliases without consulting later ambient state', async t => {
    for (const name of ['OPENROUTER_API_KEY', 'DECISION_BRIDGE_OPENROUTER_API_KEY']) {
        const before = process.env[name]; process.env[name] = 'ambient-route-fixture';
        t.after(() => { if (before === undefined) delete process.env[name]; else process.env[name] = before; });
    }
    const headers = [];
    const extractionRequest = { images: ['data:image/webp;base64,UklGRiIAAABXRUJQVlA4IBYAAAAwAQCdASoBAAEADsD+JaQAA3AAAAAA'],
        fields: { name: { type: 'text' } } };
    t.mock.method(globalThis, 'fetch', async (_url, init) => {
        headers.push(init.headers);
        const extraction = JSON.parse(init.body).fields !== undefined;
        return Response.json({ model: 'served', answers: extraction ? { name: { type: 'text', value: 'Ada', spans: [null, [0, 3]] } }
            : { ready: { type: 'noul', noul: 0, confidence: 0, future: { value: null } } },
        usage: { input_tokens: 0, output_tokens: 0, future: { zero: 0 } }, ...(extraction ? { state_text: null } : {}) });
    });
    for (const operation of ['decision', 'extraction']) {
        const settings = { schemaVersion: 3, evaluators: { e: { type: 'routing', options: { operation,
            routing: { defaultRoute: 'r', routes: { r: { config: {
                type: `openrouter-${operation === 'decision' ? 'decisions' : 'extraction'}`, model: 'vendor/model',
            } } } } } } }, flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e', operation } } };
        const env = { OPENROUTER_API_KEY: 'old-fixture', DECISION_BRIDGE_OPENROUTER_API_KEY: 'captured-fixture' };
        const runner = await createConfiguredRunner(settings, { env });
        try {
            env.DECISION_BRIDGE_OPENROUTER_API_KEY = 'changed-fixture';
            const result = await runner.run({ flow: 'a', kind: 'request', request: operation === 'decision' ? request : extractionRequest });
            assert.equal(result.status, 'completed', JSON.stringify(result.errors));
            assert.equal(new Headers(headers.at(-1)).get('authorization'), 'Bearer captured-fixture');
            if (operation === 'decision') {
                assert.deepEqual(result.evaluation.result.meta.openRouter.usage.future, { zero: 0 });
                assert.equal(result.evaluation.result.meta.openRouter.answers.ready.confidence, 0);
            }
            const saved = await runner.store.get(result.taskId, runner.namespace);
            assert.equal(JSON.stringify(saved).includes('captured-fixture'), false);
        } finally { await runner.close(); }
        const isolated = await createConfiguredRunner(settings, { env: {} });
        try {
            const result = await isolated.run({ flow: 'a', kind: 'request', request: operation === 'decision' ? request : extractionRequest });
            assert.notEqual(result.status, 'completed');
        } finally { await isolated.close(); }
    }
    assert.equal(headers.length, 2);
});

test('Live item acknowledgements retain additive native fields through the attachment', async () => {
    let receive; const notifications = [];
    const item = { id: 'fixture-item', role: 'user', content: 'Fixture', future: { zero: 0, absent: null } };
    const controller = createLiveDecisions({ transport: {
        subscribe(callback) { receive = callback; return () => {}; },
        send(command) { receive({ kind: 'event', event: { type: 'decision.item.created', event_id: 'fixture-ack',
            client_event_id: command.event_id, item, future: [] } }); },
    } });
    const attachment = attachLiveController({ controller, flow: 'a', runner: { run() { throw Error('Acknowledgements must not evaluate'); } },
        onNotification: value => notifications.push(value) });
    try {
        const ack = await attachment.controller.createItem({ id: item.id, role: item.role, content: item.content });
        await attachment.drain();
        assert.deepEqual(ack.item, item); assert.deepEqual(notifications[0].item, item);
        assert.equal(attachment.status().decisions, 0);
    } finally { await attachment.stop(); controller.dispose(); }
});

test('native additive usage survives SQLite, saved admission, reprocess and transfer without inference', async () => {
    let calls = 0;
    const usage = { input_tokens: 2, output_tokens: 0, total_tokens: 2,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 2,
            cache_write_tokens_details: { ttl_30m_tokens: 0, ttl_12h_tokens: 2, future: null } },
        output_tokens_details: { reasoning_tokens: 0 }, future: { zero: 0, absent: null } };
    const provider = createProvider({ type: 'openai-decisions', apiKey: 'fixture', fetchImpl: async () => {
        calls++;
        return Response.json({ model: 'served', future: { retained: true }, usage,
            answers: [{ type: 'predicate', name: 'ready', probability: 0.812345678901234,
                probabilities: { opaque: true }, confidence: null, future: [0, null] }] });
    } });
    const store = createSQLiteStore(':memory:');
    const evaluator = decisionEvaluator(createBridge({ provider }));
    const runner = createRunner({ store, evaluators: { e: evaluator },
        flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'e' } } });
    try {
        const first = await runner.run({ flow: 'a', kind: 'request', request });
        assert.equal(first.status, 'completed');
        const result = first.evaluation.result;
        assert.deepEqual(result.meta.openaiDecisions.usage, usage);
        assert.equal(result.answers.ready.noul, 0.812345678901234);
        assert.equal(Object.hasOwn(result.answers.ready, 'confidence'), false);
        assert.equal(Object.hasOwn(result.answers.ready, 'probabilities'), false);
        assert.equal((await runner.run({ flow: 'a', kind: 'result', result, validation: { request } })).status, 'completed');
        const replay = await runner.reprocess(first.taskId);
        assert.equal((await runner.execute(replay.taskId)).status, 'completed');
        const imported = await runner.importTask(await collectExport(runner, first.taskId), { flow: 'a' });
        assert.deepEqual((await runner.execute(imported.taskId)).evaluation.result.meta.openaiDecisions.usage, usage);
        const invalid = structuredClone(result); invalid.answers.ready.noul = -1;
        assert.throws(() => evaluator.validateSaved(invalid, request));
        assert.equal(first.schemaVersion, 3); assert.equal(calls, 1);
    } finally { await runner.close(); await store.close(); }
});

test('Anthropic effective model chooses schema output while retaining explicit tool validation and API version', async t => {
    const wires = [];
    t.mock.method(globalThis, 'fetch', async (_url, init) => {
        wires.push({ body: JSON.parse(init.body), headers: new Headers(init.headers) });
        return Response.json({ id: 'fixture', type: 'message', role: 'assistant', model: 'claude-opus-5-5',
            content: [{ type: 'text', text: JSON.stringify({ answers: { ready: { noul: 0, future: null } }, future: [] }) }],
            stop_reason: 'end_turn', usage: { input_tokens: 1, output_tokens: 0 } });
    });
    const runner = await createConfiguredRunner(config('anthropic', { model: 'claude-sonnet-4-5' }), {
        env: { DECISION_BRIDGE_ANTHROPIC_API_KEY: 'anthropic-fixture' },
    });
    try {
        const input = { ...request, model: 'claude-opus-5-5' };
        const result = await runner.run({ flow: 'a', kind: 'request', request: input });
        assert.equal(result.status, 'completed'); assert.equal(result.evaluation.result.answers.ready.noul, 0);
        assert.equal(wires[0].body.output_config.format.type, 'json_schema');
        assert.equal(Object.hasOwn(wires[0].body, 'tools'), false);
        assert.equal(wires[0].headers.get('anthropic-version'), '2023-06-01');
        assert.equal(wires[0].headers.get('x-api-key'), 'anthropic-fixture');
        await runner.reprocess(result.taskId); assert.equal(wires.length, 1);
        const explicit = createGenerativeProvider({ type: 'anthropic', apiKey: 'fixture', model: input.model, responseFormat: 'tool' });
        await assert.rejects(explicit.evaluate(input)); assert.equal(wires.length, 1);
    } finally { await runner.close(); }
});
