/** Exercise the core dependency through installed public Connector adapters. */
export const coreScenario = `import assert from 'node:assert/strict';
import { createBridge, createProvider } from '@fayegram-ai/decision-bridge';
import { createExtractionProvider } from '@fayegram-ai/decision-bridge/extraction';
import { decisionEvaluator, extractionEvaluator } from '@fayegram-ai/decision-bridge-connectors';
import { createConfiguredRunner } from '@fayegram-ai/decision-bridge-connectors/node';
const usage = { input_tokens: 1, output_tokens: 0 };
const request = { state: 'Ada', fields: { name: { type: 'text' } } };
for (const type of ['cloudflare-extraction', 'openrouter-extraction']) {
    const cloudflare = type === 'cloudflare-extraction';
    const native = { model: cloudflare ? 'clairic' : 'served', answers: { name: { type: 'text', value: 'Ada' } }, usage,
        ...(cloudflare ? { truncated: false } : { state_text: null }) };
    const provider = createExtractionProvider({ type, apiKey: 'fixture',
        ...(cloudflare ? { accountId: 'fixture' } : { model: 'vendor/model' }),
        fetchImpl: async () => Response.json(cloudflare ? { success: true, result: native } : native) });
    const evaluator = extractionEvaluator(provider);
    const result = await evaluator.execute(request, {});
    assert.deepEqual(evaluator.validateSaved(result, request), result);
    const wrong = structuredClone(result); wrong.meta.provider = 'wrong';
    assert.throws(() => evaluator.validateSaved(wrong, request), { code: 'INVALID_SAVED_OUTPUT' });
}
const bridge = createBridge({ provider: createProvider({ type: 'openrouter-decisions', apiKey: 'fixture', model: 'vendor/model',
    fetchImpl: async () => Response.json({ model: 'served', answers: { ready: { type: 'noul', noul: 0.8 } }, usage }) }) });
const evaluator = decisionEvaluator(bridge);
const input = { state: 'Evidence', questions: { ready: { type: 'noul', instructions: 'Ready?' } } };
const result = await evaluator.execute(input, {});
assert.equal(evaluator.validateSaved(result, input).meta.probabilitySource, 'native');
const originalFetch = globalThis.fetch; let geminiCalls = 0;
globalThis.fetch = async (_url, init) => {
    geminiCalls++; const wire = JSON.parse(init.body);
    return Response.json({ answers: Object.fromEntries(Object.keys(wire.questions).map(id =>
        [id, { type: 'noul', noul: 0.812345678901234, confidence: 0.812345678901234 }])),
        usage: { total_input_tokens: 1, total_output_tokens: 0 } });
};
let configured;
try {
    configured = await createConfiguredRunner({ schemaVersion: 3,
        evaluators: { g: { type: 'decision', options: { provider: { type: 'gemini-decisions' } } } },
        flows: { review: { revision: '1', inputs: ['request', 'result'], evaluator: 'g' } } },
    { env: { DECISION_BRIDGE_GEMINI_API_KEY: 'fixture' } });
    const report = await configured.run({ flow: 'review', kind: 'request', request: {
        input: [{ type: 'document', url: 'https://example.invalid/file.pdf' },
            { type: 'image', url: 'https://example.invalid/photo.jpg', mimeType: 'image/jpeg', resolution: 'ultra_high' }],
        questions: { ready: { type: 'noul', instructions: 'Ready?' } },
    } });
    const saved = (await configured.store.get(report.taskId, configured.namespace)).task.result;
    assert.equal(report.schemaVersion, 3); assert.equal(saved.meta.schemaVersion, 8);
    assert.equal(saved.meta.provider, 'gemini-decisions'); assert.equal(saved.answers.ready.noul, 0.812345678901234);
    await configured.reprocess(report.taskId); assert.equal(geminiCalls, 1);
} finally { await configured?.close(); globalThis.fetch = originalFetch; }
console.log('Installed Core: native decisions, extraction, Gemini media and saved-result contracts passed.');
`;
