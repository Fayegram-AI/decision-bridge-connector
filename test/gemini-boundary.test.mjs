import test from 'node:test';
import assert from 'node:assert/strict';
import { createBridge, createProvider } from '@fayegram-ai/decision-bridge';
import { decisionEvaluator } from '../src/index.mjs';
import { createRunner, createConfiguredRunner } from './helpers/runner.mjs';
import { createSQLiteStore } from '../src/storage/sqlite.mjs';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import { main } from '../src/node/cli.mjs';

const probability = 0.812345678901234;
const usage = { total_input_tokens: 17, total_output_tokens: 0, total_tokens: 17,
    input_tokens_by_modality: [{ modality: 'TEXT', tokens: 17 }] };
const feedback = { safety_ratings: [{ category: 'fixture', probability: 'NEGLIGIBLE' }] };
function nativeResponse(request) {
    const answers = Object.fromEntries(Object.entries(request.questions).map(([id, question]) => {
        if (question.type === 'noul') return [id, { type: 'noul', noul: probability, confidence: probability }];
        const keys = question.type === 'choice' ? Object.keys(question.criteria) : question.criteria.map((_, index) => String(index));
        const probabilities = Object.fromEntries(keys.map((key, index) => [key, index === 0 ? probability : 1 - probability]));
        return [id, { type: question.type, confidence: probability, probabilities,
            ...(question.type === 'choice' ? { choice: keys[0] } : { score: 1 - probability,
                legend: Object.fromEntries(keys.map((key, index) => [key, question.criteria[index]])) }) }];
    }));
    return { answers, usage, prompt_feedback: feedback };
}
const questions = { ready: { type: 'noul', instructions: 'Ready?' },
    action: { type: 'choice', instructions: 'Act?', options: [{ value: true, description: 'Yes' }, { value: false, description: 'No' }] },
    urgency: { type: 'score', instructions: 'Urgency?', levels: [{ label: 'routine', description: 'Routine' }, { label: 'urgent', description: 'Urgent' }] } };
const media = resolution => [{ type: 'document', url: 'https://example.invalid/report.pdf?signature=fixture' },
    { type: 'audio', url: 'https://example.invalid/audio.ogg', mimeType: 'audio/ogg' },
    { type: 'video', url: 'https://example.invalid/video.mp4', mimeType: 'video/mp4', ...(resolution ? { resolution: 'high' } : {}) },
    { type: 'image', url: 'https://example.invalid/photo.jpg', mimeType: 'image/jpeg', ...(resolution ? { resolution: 'ultra_high' } : {}) }];

test('configured Gemini preserves media schemas, native values, saved validation and schema-3 transfer', async t => {
    let calls = 0; const wires = [];
    t.mock.method(globalThis, 'fetch', async (url, init) => {
        assert.equal(url, 'https://generativelanguage.googleapis.com/v1beta/decisions');
        assert.equal(new Headers(init.headers).get('x-goog-api-key'), 'fixture-gemini');
        calls++; const wire = JSON.parse(init.body); wires.push(wire); return Response.json(nativeResponse(wire));
    });
    const example = JSON.parse(await readFile(new URL('../examples/gemini.config.json', import.meta.url), 'utf8'));
    const runner = await createConfiguredRunner({ ...example, store: { type: 'sqlite', options: { path: ':memory:' } } },
    { env: { DECISION_BRIDGE_GEMINI_API_KEY: 'fixture-gemini' } });
    try {
        for (const resolution of [false, true]) {
            const request = { input: media(resolution), questions };
            await runner.preview({ flow: 'review', kind: 'request', request });
            const first = await runner.run({ flow: 'review', kind: resolution ? 'event' : 'request', [resolution ? 'event' : 'request']: request });
            const result = first.evaluation.result;
            assert.equal(first.schemaVersion, 3); assert.equal(result.meta.schemaVersion, resolution ? 8 : 7);
            assert.equal(result.meta.provider, 'gemini-decisions'); assert.equal(result.meta.requestedModel, 'gemini-decision-1');
            assert.equal(result.meta.probabilitySource, 'native'); assert.equal(result.meta.confidenceSource, 'native');
            assert.equal(result.answers.ready.noul, probability); assert.equal(result.answers.action.choice, true);
            assert.equal(result.answers.action.probabilities[0].probability, probability);
            assert.equal(result.answers.urgency.score, 1 - probability);
            assert.equal(result.answers.urgency.legend['0'].label, 'routine');
            assert.deepEqual(result.meta.geminiDecisions.usage, usage); assert.deepEqual(result.meta.geminiDecisions.promptFeedback, feedback);
            assert.equal(result.meta.geminiDecisions.answers.ready.confidence, probability);
            const saved = await runner.run({ flow: 'review', kind: 'result', result, validation: { request } });
            assert.equal(saved.status, 'completed');
            const reprocessed = await runner.reprocess(first.taskId); assert.equal((await runner.execute(reprocessed.taskId)).status, 'completed');
            const exported = []; for await (const chunk of runner.exportTask(first.taskId)) exported.push(chunk);
            assert.equal(JSON.parse(new TextDecoder().decode(exported[0])).schemaVersion, 3);
            const imported = await runner.importTask(exported, { flow: 'review' });
            assert.equal((await runner.execute(imported.taskId)).evaluation.result.meta.schemaVersion, resolution ? 8 : 7);
        }
        assert.equal(calls, 2);
        assert.equal(wires[0].input[0].uri, media(false)[0].url); assert.equal(wires[0].input[0].mime_type, 'application/pdf');
        assert.equal(wires[1].input[2].resolution, 'high'); assert.equal(wires[1].input[3].resolution, 'ultra_high');
        await assert.rejects(runner.submit({ flow: 'review', kind: 'request', request: {
            input: [{ type: 'image', url: 'https://example.invalid/image.jpg' }], questions } }));
        assert.equal(calls, 2);
        const saved = await runner.store.get((await runner.list()).items[0].taskId, runner.namespace);
        assert.equal(JSON.stringify(saved).includes('fixture-gemini'), false);
    } finally { await runner.close(); }
});

test('Gemini routing uses the existing public provider selection contract', async t => {
    let calls = 0;
    t.mock.method(globalThis, 'fetch', async (_url, init) => { calls++; return Response.json(nativeResponse(JSON.parse(init.body))); });
    const runner = await createConfiguredRunner({ schemaVersion: 3,
        evaluators: { routed: { type: 'routing', options: { routing: { defaultRoute: 'gemini',
            routes: { gemini: { config: { type: 'gemini-decisions', apiKey: 'fixture' } } } } } } },
        flows: { a: { revision: '1', inputs: ['request'], evaluator: 'routed' } } });
    try {
        const result = await runner.run({ flow: 'a', kind: 'request', request: { input: media(true), questions } });
        assert.equal(result.status, 'completed'); assert.equal(result.evaluation.result.meta.routing.selectedRoute, 'gemini');
        assert.equal(calls, 1);
    } finally { await runner.close(); }
});

test('Gemini replays a request at the Connector node limit without enlarging its budgets', async () => {
    let calls = 0;
    const provider = createProvider({ type: 'gemini-decisions', apiKey: 'fixture', fetchImpl: async (_url, init) => {
        calls++; return Response.json(nativeResponse(JSON.parse(init.body)));
    } });
    const evaluator = decisionEvaluator(createBridge({ provider }), { validationOptions: request => {
        assert(Object.isFrozen(request)); assert(Object.isFrozen(request.state));
        return provider.getResultValidationOptions(request.model);
    } });
    const store = createSQLiteStore(':memory:');
    const runner = createRunner({ store, evaluators: { gemini: evaluator }, flows: { a: { revision: '1', inputs: ['request', 'result'], evaluator: 'gemini' } } });
    try {
        // These six structural nodes plus the state entries reach exactly 500,000 nodes.
        // Core adds the resolved model to its own immutable validation result.
        const request = { state: Array(499994).fill(0), questions: { ready: questions.ready } };
        const result = await runner.run({ flow: 'a', kind: 'request', request }); assert.equal(result.status, 'completed');
        assert.equal(result.evaluation.result.meta.schemaVersion, 1);
        assert.equal((await runner.reprocess(result.taskId)).flow, 'a');
        const exported = []; for await (const chunk of runner.exportTask(result.taskId)) exported.push(chunk);
        const imported = await runner.importTask(exported, { flow: 'a' });
        assert.equal((await runner.execute(imported.taskId)).status, 'completed');
        await assert.rejects(runner.submit({ flow: 'a', kind: 'request', request: { ...request, state: Array(500000).fill(0) } }), { code: 'RESOURCE_LIMIT' });
        await assert.rejects(runner.submit({ flow: 'a', kind: 'request', request: { ...request, state: 'x'.repeat(1048576) } }), { code: 'RESOURCE_LIMIT' });
        assert.equal(store.limits.maxBytes, 1048576); assert.equal(calls, 1);
    } finally { await runner.close(); await store.close(); }
});

test('Gemini safety diagnostics survive Connector error capture', async () => {
    const provider = createProvider({ type: 'gemini-decisions', apiKey: 'fixture', fetchImpl: async () => Response.json({ prompt_feedback: { block_reason: 'SAFETY' } }) });
    const runner = createRunner({ evaluators: { e: decisionEvaluator(createBridge({ provider })) }, flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e' } } });
    try {
        const result = await runner.run({ flow: 'a', kind: 'request', request: { state: 'Fixture', questions: { ready: questions.ready } } });
        assert.equal(result.errors[0].code, 'MODEL_REFUSAL'); assert.equal(result.errors[0].upstreamErrorCode, 'SAFETY');
    } finally { await runner.close(); }
});

test('CLI JSONL batch admits common Gemini requests with independent media versions', async t => {
    const directory = await mkdtemp(join(tmpdir(), 'dbc-gemini-batch-')); let calls = 0; let output = ''; let errors = '';
    const capture = append => new Writable({ write(chunk, _encoding, done) { append(chunk.toString()); done(); } });
    t.mock.method(globalThis, 'fetch', async (_url, init) => { calls++; return Response.json(nativeResponse(JSON.parse(init.body))); });
    try {
        const example = JSON.parse(await readFile(new URL('../examples/gemini.config.json', import.meta.url), 'utf8'));
        example.evaluators.gemini.options.provider.apiKey = 'fixture';
        const filename = join(directory, 'config.json'); await writeFile(filename, JSON.stringify(example));
        const plain = JSON.parse(await readFile(new URL('../examples/gemini-task.json', import.meta.url), 'utf8'));
        const lines = [plain, { flow: 'review', kind: 'request', request: { input: media(true), questions } }];
        const exit = await main(['batch', '--config', filename], { stdin: Readable.from(lines.map(value => JSON.stringify(value) + '\n')),
            stdout: capture(value => { output += value; }), stderr: capture(value => { errors += value; }) });
        assert.equal(exit, 0, errors); assert.equal(errors, ''); assert.equal(calls, 2);
        const results = output.trim().split('\n').map(line => JSON.parse(line));
        assert.deepEqual(results.map(item => [item.line, item.ok, item.value.status]), [[1, true, 'completed'], [2, true, 'completed']]);
    } finally { await rm(directory, { recursive: true, force: true }); }
});
