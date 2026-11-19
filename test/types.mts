import { createRunner, createMemoryStore, createCallbackConnector, consumeSource, createIterableSource, type Submission, type Flow, type PolicyDecision, type RunnerOptions, type ConsumeSourceOptions } from '../src/index.mjs';
import { createConfiguredRunner, createHost, createService, loadConfig, createIncoming, createSpoolSource, type ConnectorConfiguration, type HttpOptions, type HostedIncomingConnector } from '../src/node/index.mjs';
import { createPostgresStore } from '../src/storage/postgres.mjs';
import type { ChainRetry } from '../src/index.mjs';
import { createMcpConnector, type McpOptions } from '@fayegram-ai/decision-bridge-connectors/mcp';
import { attachLiveController } from '../src/integrations/live.mjs';
import { decisionEvaluator } from '../src/index.mjs';
import { createBridge, createProvider, type EvaluationRequest } from '@fayegram-ai/decision-bridge';
const geminiRequest: EvaluationRequest = { input: [
    { type: 'document', url: 'https://example.invalid/report.pdf' },
    { type: 'audio', url: 'https://example.invalid/clip.ogg', mimeType: 'audio/ogg' },
    { type: 'image', url: 'https://example.invalid/photo.jpg', mimeType: 'image/jpeg', resolution: 'ultra_high' },
    { type: 'video', url: 'https://example.invalid/clip.mp4', mimeType: 'video/mp4', resolution: 'high' },
], questions: { ready: { type: 'noul', instructions: 'Ready?' } } };
const geminiProvider = createProvider({ type: 'gemini-decisions', apiKey: 'fixture' });
const geminiAdapter = decisionEvaluator(createBridge({ provider: geminiProvider }), {
    validationOptions: request => geminiProvider.getResultValidationOptions(request.model),
});
void geminiAdapter.validate?.(geminiRequest);
// @ts-expect-error Video resolution excludes the image-only ultra_high setting.
const invalidVideo: EvaluationRequest = { ...geminiRequest, input: [{ type: 'video', url: 'https://example.invalid/clip.mp4', mimeType: 'video/mp4', resolution: 'ultra_high' }] };
void invalidVideo;
const liveProcessing: Parameters<typeof attachLiveController>[0]['processing'] = 'admit';
// @ts-expect-error Live processing is an explicit admission or execution choice.
const invalidLiveProcessing: typeof liveProcessing = 'other';
void liveProcessing; void invalidLiveProcessing;
const input: Submission = { flow: 'review', kind: 'result', result: { score: 0.9 } };
void createSpoolSource({ directory: '.', flow: 'review', inputMode: 'event' });
// @ts-expect-error Incoming modes are a closed set of envelope forms.
void createSpoolSource({ directory: '.', flow: 'review', inputMode: 'unknown' });
const retainedContext: Submission = { flow: 'review', kind: 'request', request: {}, validation: { attestation: 'verified' } };
void retainedContext;
// @ts-expect-error Validation context belongs to a submission, not caller-override flow settings.
const invalidValidationDefault: Flow = { revision: '1', defaults: { validation: {} } };
void invalidValidationDefault;
// @ts-expect-error A submission has exactly one input form.
const ambiguous: Submission = { flow: 'review', kind: 'request', request: {}, result: {} };
// @ts-expect-error Flow selection is mandatory.
const missingFlow: Submission = { kind: 'event', event: {} };
const flow: Flow = { revision: '1', inputs: ['result'], validator: 'schema', connections: [{ id: 'audit', connector: 'audit', when: 'evaluation.succeeded', payload: 'raw-result' }] };
const booleanSchema: Flow = { ...flow, connections: [{ ...flow.connections![0]!, outputSchema: false }] };
const asyncOutputValidator: RunnerOptions['validateOutput'] = async schema => schema !== false;
void booleanSchema; void asyncOutputValidator;
const runner = createRunner({ flows: { review: flow }, store: createMemoryStore(), validators: { schema: { revision: '1', validate: value => value } }, connectors: { audit: createCallbackConnector(async () => ({ status: 'acknowledged' })) } });
void consumeSource(runner, await createIncoming('stdin', { flow: 'review', inputMode: 'result' }));
void consumeSource(runner, createSpoolSource({ directory: '.', flow: 'review' }));
const listenerOnly = await createIncoming('http', { flow: 'review' });
// @ts-expect-error A hosted listener is not a consumable iterable source.
void consumeSource(runner, listenerOnly);
void runner.run(input); void runner.redeliver('task', { flow: 'review' });
const preparation = { signal: new AbortController().signal };
void runner.preview(input, preparation);
void runner.reprocess('task', { ...preparation, flow: 'review' });
void runner.redeliver('task', { ...preparation, flow: 'review' });
void runner.reevaluate('task', preparation);
void runner.resolveEvaluation('task', { status: 'succeeded', result: 1, reason: 'Recovered.' }, preparation);
// @ts-expect-error Preparation cancellation requires a real AbortSignal.
void runner.preview(input, { signal: {} });
void runner.submit(input, { source: 'typed-source', signal: new AbortController().signal });
// @ts-expect-error Submission cancellation requires a real AbortSignal.
void runner.submit(input, { signal: {} });
void runner.recordCompletion('delivery', { status: 'completed', evidence: 'receipt' });
// @ts-expect-error Recording completion requires a final downstream outcome.
void runner.recordCompletion('delivery', { status: 'pending' });
const sourceOptions: ConsumeSourceOptions = { replyTimeoutMs: 10000, acknowledgementTimeoutMs: 5000,
    observerTimeoutMs: 1000, closeTimeoutMs: 5000, onOutcome: (outcome, { signal }) => { void outcome; void signal.aborted; } };
void consumeSource(runner, createIterableSource([{ input }], { name: 'typed-source' }), sourceOptions);
void createIterableSource([], { name: 'typed-close' }).close?.({ signal: preparation.signal, timeoutMs: 1000 });
void runner.store.aggregate('default', 'group', current => current);
void runner.store.dueAggregates('default', 'runtime-revision');
void runner.store.recordReply('default', 'task', { status: 'in_flight', attemptId: 'reply-one', expiresAt: 100 });
// @ts-expect-error In-flight channel reservations require a recovery deadline.
void runner.store.recordReply('default', 'task', { status: 'in_flight', attemptId: 'reply-one' });
// @ts-expect-error A channel settlement must identify the attempt it completes.
void runner.store.recordAcknowledgement('default', 'task', { status: 'acknowledged' });
// @ts-expect-error Redelivery requires an explicit authorized flow.
void runner.redeliver('task');
const decision: PolicyDecision = { status: 'review', reasons: [], policyRevision: '1' };
// @ts-expect-error Review does not carry an accepted action outcome.
const invalidDecision: PolicyDecision = { ...decision, outcome: 'act' };
const configuration: ConnectorConfiguration = { schemaVersion: 3, flows: { review: flow } };
void createConfiguredRunner(configuration); void createHost({ runner, role: 'worker' });
const startupSignal = new AbortController().signal;
void createMcpConnector({ tool: 'fixture', client: { callTool: async () => ({ content: [] }) } }, { signal: startupSignal });
void loadConfig('config.json', { signal: startupSignal });
void createConfiguredRunner(configuration, { signal: startupSignal });
void createHost({ runner, role: 'ingress', signal: startupSignal });
void createService(configuration, { signal: startupSignal });
const aggregatedSource: HostedIncomingConnector = { ...createSpoolSource({ directory: '.', flow: 'review' }),
    aggregation: { rule: 'pair', correlationPath: '$.case', eventIdPath: '$.id' } };
void createHost({ runner, role: 'ingress', sources: { one: aggregatedSource } });
// @ts-expect-error Aggregated sources require an explicit correlation selector.
const missingSelector: HostedIncomingConnector = { aggregation: { rule: 'pair' } };
void missingSelector;
const httpOptions: HttpOptions = { runner, flow: 'review', reply: 'complete', replyTimeoutMs: 5000, allowRun: false };
const retry: ChainRetry = { timeoutMs: 1000, delayMs: 10, maxDelayMs: 100 };
// @ts-expect-error A chain needs an attempt or elapsed-time budget.
const unboundedRetry: ChainRetry = { delayMs: 10, maxDelayMs: 100 };
const authorizedHttp: HttpOptions = { ...httpOptions, authorizationTimeoutMs: 1000, authorize: ({ signal }) => !signal.aborted };
const authorizedMcp: McpOptions = { runner, flows: ['review'], authorizationTimeoutMs: 1000, authorize: ({ signal }) => !signal.aborted };
const browserHttp: HttpOptions = { ...httpOptions, allowedHosts: ['bridge.example'], allowedOrigin: 'https://app.example' };
const browserMcp: McpOptions = { ...authorizedMcp, allowedHosts: ['bridge.example'], allowedOrigin: 'https://app.example' };
// @ts-expect-error Deployment hosts are an explicit list, not a wildcard string.
const wildcardHttp: HttpOptions = { ...httpOptions, allowedHosts: '*' };
void browserHttp; void browserMcp; void wildcardHttp;
void retry; void unboundedRetry; void authorizedHttp; void authorizedMcp;
createCallbackConnector(async (_value, context) => {
    await context.onAcceptance?.({ status: 'acknowledged', receipt: { response: 42 } });
    // @ts-expect-error An acceptance checkpoint must confirm acknowledgement.
    await context.onAcceptance?.({ status: 'unknown' });
    return { status: 'acknowledged' };
});
void httpOptions;
void createPostgresStore({ connectionString: 'postgres://example', maxPending: 100 });
void ambiguous; void missingFlow; void invalidDecision;
