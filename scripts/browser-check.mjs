import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { webcrypto } from 'node:crypto';
import vm from 'node:vm';

assert.equal(typeof vm.SourceTextModule, 'function',
  'Run this check with node --experimental-vm-modules scripts/browser-check.mjs.');

const rootURL = new URL('../src/index.mjs', import.meta.url).href;
const liveURL = new URL('../src/integrations/live.mjs', import.meta.url).href;
const allowedPublicImports = new Set([
  '@fayegram-ai/decision-bridge', '@fayegram-ai/decision-bridge/extraction',
  '@fayegram-ai/decision-bridge/live', '@fayegram-ai/decision-bridge/routing',
  '@fayegram-ai/decision-bridge/custom/decision', '@fayegram-ai/decision-bridge/custom/generative',
]);
const context = vm.createContext({
  AbortController, AbortSignal, TextEncoder, TextDecoder, URL, URLSearchParams,
  Headers, Request, Response, ReadableStream, WritableStream, TransformStream,
  Blob, FormData, crypto: webcrypto, performance, setTimeout, clearTimeout,
  setInterval, clearInterval, atob, btoa,
  fetch: async () => { throw Error('The portable graph check must not access a provider.'); },
});
const modules = new Map();

function resolve(specifier, referencing) {
  assert(!specifier.startsWith('node:'), `Node dependency leaked into the portable graph: ${specifier}`);
  if (specifier.startsWith('./') || specifier.startsWith('../')) return new URL(specifier, referencing.identifier).href;
  if ([rootURL, liveURL].includes(specifier)) return specifier;
  assert(allowedPublicImports.has(specifier), `Unexpected package in the portable graph: ${specifier}`);
  return import.meta.resolve(specifier);
}
async function getModule(url) {
  if (!modules.has(url)) modules.set(url, (async () => new vm.SourceTextModule(await readFile(new URL(url), 'utf8'), {
    context, identifier: url,
    initializeImportMeta(meta) { meta.url = url; },
    importModuleDynamically() { throw Error('Unexpected dynamic import in the portable graph.'); },
  }))());
  return modules.get(url);
}
const fixture = new vm.SourceTextModule(`
  import { createRunner, createMemoryStore, createCallbackConnector, decisionEvaluator } from ${JSON.stringify(rootURL)};
  import { createBridge, createProvider } from '@fayegram-ai/decision-bridge';
  import { createLiveSource } from ${JSON.stringify(liveURL)};
  const staged = createLiveSource({ flow: 'portable', sessionId: 'browser', connect() { throw new Error('Unexpected connection'); } });
  await staged.close();
  if (typeof process !== 'undefined' || typeof Buffer !== 'undefined' || typeof require !== 'undefined') {
    throw new Error('The fixture must run without Node globals.');
  }
  let evaluated = 0;
  const sent = [];
  const runner = createRunner({
    store: createMemoryStore(),
    evaluators: { fixture: {
      operation: 'decision', revision: 'fixture-1',
      validate(request) { if (request.evidence !== 'portable') throw new Error('Bad request'); return request; },
      async execute(_request, { signal }) {
        if (!(signal instanceof AbortSignal)) throw new Error('Missing Web AbortSignal');
        evaluated++;
        return { model: 'fixture', answers: { ready: { type: 'noul', noul: 0.95 } },
          usage: { input_tokens: 0, output_tokens: 0 },
          meta: { provider: 'fixture', providerKind: 'decision', probabilitySource: 'native' } };
      },
    } },
    connectors: { audit: createCallbackConnector(async (payload, { signal }) => {
      if (!(signal instanceof AbortSignal)) throw new Error('Missing delivery AbortSignal');
      sent.push(payload); return { status: 'acknowledged', receipt: { accepted: true } };
    }) },
    flows: { portable: { revision: '1', inputs: ['request'], evaluator: 'fixture',
      policies: { ready: { revision: '1', accept: { question: 'ready', type: 'noul', min: 0.9 } } },
      connections: [{ id: 'audit', connector: 'audit', when: 'evaluation.succeeded', payload: 'raw-result' }] } },
  });
  const task = { flow: 'portable', kind: 'request', request: { evidence: 'portable' }, idempotencyKey: 'event-1' };
  const first = await runner.run(task);
  const duplicate = await runner.run(task);
  const lease = await runner.openSnapshot(first.taskId);
  const parts = await runner.listSnapshot(lease.id, { collection: 'data', limit: 100 });
  const resultPart = parts.items.find(item => item.path.join('.') === 'result');
  const decisionPart = parts.items.find(item => item.path.join('.') === 'policies.ready.decision');
  const result = await runner.readValue(lease.id, resultPart.id);
  const decision = await runner.readValue(lease.id, decisionPart.id);
  await runner.closeSnapshot(lease.id);
  if (first.status !== 'completed' || decision.status !== 'accepted' ||
      result.answers.ready.noul !== 0.95 ||
      first.deliveryCounts.acknowledged !== 1 || !Object.isFrozen(first) ||
      duplicate.taskId !== first.taskId || evaluated !== 1 || sent.length !== 1) {
    throw new Error('Portable execution, policy, delivery, or duplicate handling failed.');
  }
  await runner.close();
  const gemini = createProvider({ type: 'gemini-decisions', apiKey: 'fixture', fetchImpl: async (_url, init) => {
    const request = JSON.parse(init.body);
    return Response.json({ answers: Object.fromEntries(Object.keys(request.questions).map(id =>
      [id, { type: 'noul', noul: 0.8, confidence: 0.8 }])), usage: { total_input_tokens: 1, total_output_tokens: 0 } });
  } });
  const geminiRunner = createRunner({ evaluators: { g: decisionEvaluator(createBridge({ provider: gemini })) },
    flows: { review: { revision: '1', inputs: ['request'], evaluator: 'g' } } });
  const mediaReport = await geminiRunner.run({ flow: 'review', kind: 'request', request: {
    input: [{ type: 'document', url: 'https://example.invalid/file.pdf' },
      { type: 'image', url: 'https://example.invalid/image.jpg', mimeType: 'image/jpeg', resolution: 'high' }],
    questions: { ready: { type: 'noul', instructions: 'Ready?' } },
  } });
  const media = (await geminiRunner.store.get(mediaReport.taskId, geminiRunner.namespace)).task.result;
  if (media.meta.schemaVersion !== 8 || media.answers.ready.noul !== 0.8) throw new Error('Portable Gemini media failed.');
  await geminiRunner.close();
  export const outcome = { execution: first.status, decision: decision.status, evaluations: evaluated, deliveries: sent.length, geminiSchema: media.meta.schemaVersion };
`, { context, identifier: 'portable-fixture.mjs' });
await fixture.link((specifier, referencing) => getModule(resolve(specifier, referencing)));
await fixture.evaluate();
process.stdout.write(`${JSON.stringify({ ok: true, modules: modules.size, ...fixture.namespace.outcome })}\n`);
