import type { Bridge, EvaluationRequest, EvaluationResult, JsonValue, ResolvedRequest, ResultValidationOptions } from '@fayegram-ai/decision-bridge';
import type { ExtractionProvider, CommonExtractionRequest as ExtractionRequest, AnyExtractionResult as ExtractionResult } from '@fayegram-ai/decision-bridge/extraction';
import type { DecisionRouter, ExtractionRouter, RoutedDecisionResult, RoutedExtractionResult, RoutingCallOptions } from '@fayegram-ai/decision-bridge/routing';
export type ReadonlyJsonValue = string | number | boolean | null | readonly ReadonlyJsonValue[] | { readonly [key: string]: ReadonlyJsonValue };
export type { JsonValue } from '@fayegram-ai/decision-bridge';
export type DeepReadonly<T> = T extends (...args: never[]) => unknown ? T : T extends object ? { readonly [K in keyof T]: DeepReadonly<T[K]> } : T;
export type Operation = 'decision' | 'extraction' | 'live';
export const VERSION: '0.1.1';
export const SCHEMA_VERSION: 3;
export interface ErrorDetails { readonly code: string; readonly message: string; readonly stage?: string; readonly retryable?: boolean; readonly ambiguous?: boolean; readonly status?: number; readonly retryAfterMs?: number; readonly inputInvalid?: boolean; }
export class ConnectorError extends Error { readonly code: string; constructor(code: string, message: string, details?: Partial<ErrorDetails>); }
export function publicError(error: unknown, stage?: string): ErrorDetails;

export interface SubmissionSettings {
    readonly flow: string; readonly idempotencyKey?: string; readonly context?: JsonValue;
    readonly correlationId?: string; readonly causationId?: string; readonly questionRevision?: string; readonly deadlineAt?: string;
    /** Retained for saved-result validation, including evaluation reconciliation. */
    readonly validation?: JsonValue;
}
export type Submission<Request = JsonValue, Result = JsonValue> = SubmissionSettings & (
    | { readonly kind: 'request'; readonly request: Request; readonly event?: never; readonly result?: never }
    | { readonly kind: 'event'; readonly event: JsonValue; readonly request?: never; readonly result?: never }
    | { readonly kind: 'result'; readonly result: Result; readonly request?: never; readonly event?: never; readonly evidenceAt?: string }
);
export interface ExecutionContext { readonly signal: AbortSignal; readonly timeoutMs: number; }
export interface Evaluator<Request = unknown, Result = unknown> {
    readonly operation?: Operation; readonly revision: string;
    validate?(request: Request): Request | void | Promise<Request | void>;
    execute(request: Request, context: ExecutionContext): Result | Promise<Result>;
    validateSaved?(result: unknown, request: Request, context: ExecutionContext): Result | void | Promise<Result | void>;
}
export interface ResultValidator<Result = unknown> {
    readonly revision: string; readonly provenance?: 'structural' | 'verified';
    validate(result: unknown, context: { readonly validation: JsonValue; readonly signal: AbortSignal }): Result | void | Promise<Result | void>;
}
/** Common requests include Core media/resolution forms; result metadata versions are independent of saved schema 3. */
export function decisionEvaluator(bridge: Bridge, options?: { revision?: string; validationOptions?: ResultValidationOptions | ((request: ResolvedRequest) => ResultValidationOptions) }): Evaluator<EvaluationRequest, EvaluationResult>;
export function extractionEvaluator<R, T>(provider: ExtractionProvider<R, T>, options?: { revision?: string }): Evaluator<R, T>;
export function routedDecisionEvaluator(router: DecisionRouter, options?: { revision?: string; callOptions?: RoutingCallOptions }): Evaluator<EvaluationRequest, RoutedDecisionResult>;
export function routedExtractionEvaluator(router: ExtractionRouter, options?: { revision?: string; callOptions?: RoutingCallOptions }): Evaluator<ExtractionRequest, RoutedExtractionResult>;

export type Projection = JsonValue;
export type PolicyCondition = { all: readonly PolicyCondition[] } | { any: readonly PolicyCondition[] } | { not: PolicyCondition }
    | { question: string; type: 'choice'; value?: string | boolean; minProbability?: number; minMargin?: number; abstainOptions?: readonly (string | boolean)[] }
    | { question: string; type: 'noul'; min?: number; max?: number }
    | { question: string; type: 'score'; min?: number; max?: number; minMass?: number; atLeastLevel?: string | number }
    | { question: string; type: 'box'; minConfidence?: number; region?: { top: number; bottom: number; left: number; right: number }; relation?: 'inside' | 'intersects' }
    | { path: string; equals?: JsonValue; min?: number; max?: number; missing?: 'unmatched' };
export interface Policy { readonly id?: string; readonly revision: string; readonly accept?: PolicyCondition; readonly review?: PolicyCondition; readonly abstain?: PolicyCondition; readonly otherwise?: 'review' | 'abstained'; readonly outcome?: Projection; readonly allowedProbabilitySources?: readonly string[]; }
export type PolicyDecision = { readonly status: 'accepted'; readonly outcome?: JsonValue; readonly reasons: readonly JsonValue[]; readonly policyRevision: string }
    | { readonly status: 'review' | 'abstained'; readonly reasons: readonly JsonValue[]; readonly policyRevision: string; readonly outcome?: never };
export type FinalDecision = PolicyDecision & { readonly id: string; readonly evaluationId: string; readonly policyId: string };
export function validatePolicy<P extends Policy>(policy: P): DeepReadonly<P>;
export function applyPolicy(result: unknown, policy: Policy, options?: { operation?: Operation; correlation?: JsonValue; questionRevision?: string }): PolicyDecision;
export function project(value: unknown, projection: Projection): JsonValue;
export function validateProjection(value: unknown): Projection;
export interface RequestTemplate { readonly id?: string; readonly revision: string; readonly request: Projection; }
export function validateTemplate(value: unknown): RequestTemplate;
export function mapRequest(event: unknown, template: RequestTemplate): Readonly<Record<string, JsonValue>>;
export type Transform = { readonly type: 'project'; readonly value: Projection } | { readonly type: 'json' }
    | { readonly type: 'text'; readonly template: string; readonly escape?: 'json' | 'html' | 'uri' | 'none' }
    | { readonly type: 'coerce'; readonly to: 'string' | 'number' | 'boolean' }
    | { readonly type: 'module'; readonly name: string; readonly options?: JsonValue };
export interface TransformExtension { readonly revision: string; execute(value: JsonValue, options: JsonValue): JsonValue | Promise<JsonValue>; }
export function validateTransform(steps?: readonly Transform[]): readonly Transform[];
export function transform(value: JsonValue, steps?: readonly Transform[], modules?: Record<string, TransformExtension>): Promise<JsonValue>;

export interface ConnectorCapabilities { readonly direction: 'incoming' | 'outgoing' | 'both'; readonly runtime: 'portable' | 'node'; readonly acknowledgement: string; readonly idempotent?: boolean; readonly local?: boolean; readonly ordered?: boolean; }
export interface DeliveryContext {
    readonly signal: AbortSignal; readonly taskId: string; readonly eventId: string; readonly deliveryId: string; readonly attemptId: string; readonly idempotencyKey: string; readonly destination?: JsonValue; readonly encoding?: 'json' | 'text' | 'jsonl';
    /** Await once after confirmed acceptance, before collecting optional response data. */
    readonly onAcceptance?: (receipt: DeliveryReceipt & { readonly status: 'acknowledged' }) => void | Promise<void>;
}
export interface DownstreamCompletion { readonly status: 'untracked' | 'pending' | 'completed' | 'failed' | 'unknown'; readonly [key: string]: JsonValue; }
export interface DeliveryReceipt { readonly status: 'acknowledged' | 'submitted' | 'unknown'; readonly receipt?: JsonValue; readonly completion?: DownstreamCompletion; }
export interface OutgoingConnector { readonly name: string; readonly revision: string; readonly capabilities: ConnectorCapabilities; send(payload: JsonValue, context: DeliveryContext): DeliveryReceipt | Promise<DeliveryReceipt>; close?(): void | Promise<void>; }
export function createCallbackConnector(send: OutgoingConnector['send'], options?: { name?: string; revision?: string; idempotent?: boolean; acknowledgement?: 'none' | 'accepted' | 'completed' }): OutgoingConnector;
export type ChainRetry = { readonly delayMs: number; readonly maxDelayMs: number } & (
    | { readonly maxAttempts: number; readonly timeoutMs?: number }
    | { readonly maxAttempts?: number; readonly timeoutMs: number });
export interface Connection {
    readonly id: string; readonly connector: string; readonly revision?: string; readonly policy?: string;
    readonly when: 'evaluation.succeeded' | 'decision.accepted' | 'decision.review' | 'decision.abstained' | 'failure' | 'always';
    readonly payload: 'raw-result' | 'final-decision' | 'final-decision-with-result' | 'failure' | 'report';
    readonly projection?: Projection; readonly transforms?: readonly Transform[]; readonly encoding?: 'json' | 'text' | 'jsonl';
    readonly required?: boolean; readonly timeoutMs?: number; readonly retry?: { maxAttempts?: number; delayMs?: number; maxDelayMs?: number; ambiguous?: 'hold' | 'retry' };
    readonly ordering?: { key: string; onFailure?: 'block' | 'continue' }; readonly dependsOn?: readonly string[];
    readonly destination?: JsonValue; readonly placement?: string; readonly outputSchema?: object | boolean; readonly nextFlow?: string; readonly chainRetry?: ChainRetry; readonly rate?: number;
}
export interface Flow {
    readonly revision: string; readonly operation?: Operation; readonly inputs?: readonly ('request' | 'event' | 'result')[];
    readonly evaluator?: string; readonly validator?: string; readonly template?: Projection;
    readonly policies?: Readonly<Record<string, Policy | { type: 'module'; name: string; options?: JsonValue }>>;
    readonly connections?: readonly Connection[];
    readonly defaults?: Partial<Omit<SubmissionSettings, 'flow' | 'idempotencyKey' | 'validation'>>;
    readonly locked?: Flow['defaults']; readonly overrides?: readonly ('context' | 'correlationId' | 'causationId' | 'questionRevision' | 'deadlineAt')[];
    readonly timeoutMs?: number; readonly deliveryTtlMs?: number; readonly maxAgeMs?: number; readonly stale?: 'hold' | 'expire';
    readonly maxDepth?: number; readonly maxExecutions?: number;
    /** Shared allowance for all active, unexpired job kinds in this flow. */
    readonly concurrency?: number;
    readonly rate?: number;
}
export type DeliveryStatus = 'blocked' | 'pending' | 'retry_pending' | 'active' | 'acknowledged' | 'failed' | 'unknown' | 'held' | 'skipped' | 'expired' | 'cancelled';
export interface DeliveryRecord { readonly id: string; readonly connectionId: string; readonly connector: string; readonly required: boolean; readonly status: DeliveryStatus; readonly attempts: readonly { id: string; status: string; startedAt: number; finishedAt?: number; error?: ErrorDetails }[]; readonly completion: DownstreamCompletion; readonly payload: JsonValue; readonly receipt: JsonValue; readonly error: ErrorDetails | null; readonly nextAttemptAt: number; readonly expiresAt: number; }
export interface ReportOptions { includeData?: boolean; maxInlineBytes?: number; }
export interface DataPart { readonly id: string; readonly entity: 'task' | 'job'; readonly entityId: string; readonly path: readonly string[]; readonly bytes: number; readonly sha256: string; readonly kind: 'json' | 'plan' | 'diagnostic'; readonly responseUnavailable?: true; }
export interface SnapshotLease { readonly id: string; readonly taskId: string; readonly flow: string; readonly expiresAt: number; readonly schemaVersion: 3; }
export interface ImportLease { readonly id: string; readonly flow: string; readonly expiresAt: number; readonly schemaVersion: 3; }
export interface SnapshotJob { readonly id: string; readonly kind: string; readonly status: string; readonly required: boolean; readonly connectionId: string | null; readonly connector: string | null; readonly attempts: number; readonly completion: string; readonly errorCode: string | null; readonly hasPayload: boolean; readonly hasReceipt: boolean; readonly childTaskId: string | null; readonly nextFlow: string | null; readonly admissionAttempts: number; readonly nextAttemptAt: number | null; readonly deadlineAt: number | null; }
export interface SnapshotQuery { collection?: 'jobs' | 'data'; after?: string; limit?: number; }
export interface SnapshotPage { readonly items: readonly (DataPart | SnapshotJob)[]; readonly next: string | null; }
export interface DataPage { readonly encoding: 'base64'; readonly data: string; readonly offset: number; readonly bytes: number; readonly sha256: string; readonly nextOffset: number | null; }
export type TransferFrame =
    | { readonly type: 'header'; readonly seq: 0; readonly kind: 'execution-export'; readonly schemaVersion: 3 }
    | { readonly type: 'record'; readonly seq: number; readonly entity: 'task' | 'job'; readonly recordId: string; readonly index: number; readonly final: boolean; readonly data: string }
    | { readonly type: 'chunk'; readonly seq: number; readonly sha256: string; readonly index: number; readonly data: string }
    | { readonly type: 'end'; readonly seq: number; readonly frameCount: number; readonly checksum: string };
export interface ExportPage { readonly frames: readonly TransferFrame[]; readonly next: string | null; }
export interface ImportProgress { readonly nextSequence: number; readonly verified: boolean; readonly expiresAt: number; }
export interface ExecutionReport {
    readonly schemaVersion: 3; readonly taskId: string; readonly namespace: string; readonly flow: string; readonly flowRevision: string;
    readonly status: 'running' | 'completed' | 'partial' | 'failed' | 'unknown' | 'cancelled'; readonly settled: boolean;
    readonly requiredSettled: boolean; readonly createdAt: number; readonly updatedAt: number;
    readonly evaluation: { readonly id: string; readonly status: string; readonly hasResult: boolean };
    readonly policyCounts: Readonly<Record<string, number>>; readonly deliveryCounts: Readonly<Record<string, number>>; readonly chainCounts: Readonly<Record<string, number>>; readonly errorCount: number;
    readonly lineage: { readonly rootId: string; readonly depth: number; readonly maxDepth: number; readonly maxExecutions: number };
    readonly reply: { readonly status: string }; readonly sourceAcknowledgement: { readonly status: string };
    readonly data?: readonly (DataPart & { readonly value: JsonValue })[]; readonly dataComplete?: boolean;
    readonly dataOmitted?: readonly DataPart[]; readonly omittedCount?: number; readonly dataError?: ErrorDetails;
}
export interface RunOptions { signal?: AbortSignal; waitMs?: number; waitFor?: 'admitted' | 'evaluated' | 'required' | 'settled'; source?: string; report?: ReportOptions; }
export interface ResourceLimits {
    maxPending?: number; maxBytes?: number; evaluationConcurrency?: number; deliveryConcurrency?: number; optionalConcurrency?: number; perDestination?: number; leaseMs?: number;
    /** Default list size, 1–100 (default 100); explicit limits may override it up to 100. */
    pageSize?: number;
    retentionMs?: number; dedupeMs?: number; maxTransfers?: number; transferIdleMs?: number; maxTransferBytes?: number;
}
export type TaskChannelDisposition =
    | { readonly status: 'in_flight'; readonly attemptId: string; readonly expiresAt: number }
    | { readonly status: 'acknowledged' | 'submitted' | 'unknown' | 'failed'; readonly attemptId: string;
        readonly receipt?: JsonValue; readonly error?: ErrorDetails };
export interface ExecutionStore {
    readonly capabilities: { readonly durable: boolean; readonly distributed: boolean; readonly schemaVersion: 3; readonly atomicDeduplication: true; readonly fencing: true; readonly dataParts: true; readonly snapshots: true };
    readonly limits: Readonly<Required<ResourceLimits>>;
    admit(task: Record<string, unknown>, jobs: readonly Record<string, unknown>[], dedupKey: string | null): Promise<Record<string, unknown>>;
    getJob(namespace: string, jobId: string): Promise<Record<string, unknown> | null>;
    openSnapshot(namespace: string, taskId: string): Promise<SnapshotLease>;
    listSnapshot(namespace: string, snapshotId: string, query?: SnapshotQuery): Promise<SnapshotPage>;
    readSnapshot(namespace: string, snapshotId: string, partId: string, query?: { offset?: number; maxBytes?: number }): Promise<DataPage>;
    readValue(namespace: string, snapshotId: string, partId: string): Promise<ReadonlyJsonValue>;
    closeSnapshot(namespace: string, snapshotId: string): Promise<void>;
    describeSnapshot(namespace: string, snapshotId: string): Promise<SnapshotLease>;
    openPreview(namespace: string, task: Record<string, unknown>, jobs: readonly Record<string, unknown>[]): Promise<SnapshotLease>;
    snapshotRecord(namespace: string, snapshotId: string, options?: { metadata?: boolean }): Promise<{ task: Record<string, unknown>; jobs: Record<string, unknown>[] }>;
    readExportFrames(namespace: string, snapshotId: string, query?: { after?: string; limit?: number }): Promise<ExportPage>;
    openImport(namespace: string, flow: string, runtimeRevision: string): Promise<ImportLease>;
    appendImport(namespace: string, transferId: string, frame: TransferFrame): Promise<ImportProgress>;
    importedTask(namespace: string, transferId: string): Promise<Record<string, unknown>>;
    completeImport(namespace: string, transferId: string, taskId: string): Promise<void>;
    closeImport(namespace: string, transferId: string): Promise<void>;
    describeImport(namespace: string, transferId: string): Promise<ImportLease>;
    /** With includeRecoverable, also count failed deliveries that can be retried or reconciled into chain work. */
    pendingJobs(namespace: string, revision: string, options?: { includeRecoverable?: boolean }): Promise<number>;
    pendingAggregates(namespace: string, runtimeRevision: string): Promise<number>;
    findChildAdmission(namespace: string, chainClaim: Record<string, unknown>): Promise<string | null>;
    get(taskId: string, namespace: string, options?: { metadata?: boolean; jobData?: boolean }): Promise<{ task: Record<string, unknown>; jobs: Record<string, unknown>[] } | null>;
    list(namespace: string, query?: { after?: string; limit?: number; state?: string; flow?: string }): Promise<{ items: Record<string, unknown>[]; next: string | null }>;
    /** Placement labels must be a unique array of valid identifiers and match exactly. */
    claim(namespace: string, kind: string, options: { worker: string; placements?: readonly string[]; taskId?: string; flow?: string; destination?: string; revision?: string }): Promise<{ job: Record<string, unknown>; task: Record<string, unknown> } | null>;
    /** Restore a recognized undispatched claim. Return false when the caller must requeue it. */
    releaseClaim?(claim: Record<string, unknown>): Promise<boolean>;
    renew(claim: Record<string, unknown>): Promise<void>;
    /** Fence acceptance checkpoints by lease and attempt; preserve them in terminal settlement. */
    commit(claim: Record<string, unknown>, update: { task?: Record<string, unknown>; jobs?: readonly Record<string, unknown>[]; job?: Record<string, unknown> }): Promise<void>;
    recover(namespace: string): Promise<number>; maintain(namespace: string): Promise<{ removed: number }>;
    inspect(namespace: string): Promise<{ counts: Record<string, number>; oldestPendingAgeMs: number }>;
    pending(namespace: string, revision: string): Promise<number>;
    /** Reject missing/invalid targets before writing any pause control. */
    pause(namespace: string, type: 'flow' | 'destination', name: string, paused: boolean): Promise<void>;
    /** Serialize with admission; retain confirmed acceptance and links to existing children. */
    cancel(namespace: string, taskId: string): Promise<void>;
    /** Retry requires a prepared payload; acknowledged resolution clears the current error and preserves attempts. */
    mutateJob(namespace: string, jobId: string, operation: string, value?: unknown): Promise<string>;
    recordReply(namespace: string, taskId: string, reply: TaskChannelDisposition): Promise<void>;
    recordAcknowledgement(namespace: string, taskId: string, acknowledgement: TaskChannelDisposition): Promise<void>;
    resolveEvaluation(namespace: string, taskId: string, resolution: { status: 'succeeded' | 'failed'; reason: string; result?: unknown }): Promise<void>;
    /** Null leaves storage unchanged. Serialize retirement with updates; rule quotas count retained groups across revisions, absent ruleId uses the namespace quota. */
    aggregate(namespace: string, key: string, update: (value: Record<string, unknown> | null) => Record<string, unknown> | null, maxGroups?: number): Promise<Record<string, unknown> | null>;
    dueAggregates(namespace: string, runtimeRevision?: string): Promise<Record<string, unknown>[]>;
    /** Reject new transactions, drain accepted transactions, and close owned resources; safe to repeat. */
    close(): Promise<void>;
}
export function createMemoryStore(options?: ResourceLimits): ExecutionStore;
export interface Runner {
    readonly ready: Promise<Readonly<Record<string, Flow>>>; readonly store: ExecutionStore; readonly namespace: string; readonly placements: readonly string[]; readonly capabilities: ExecutionStore['capabilities'];
    /** Cancellation stops pre-admission validation; work already persisted remains admitted. */
    submit(input: Submission<unknown, unknown>, context?: { source?: string; signal?: AbortSignal; report?: ReportOptions }): Promise<ExecutionReport>;
    run(input: Submission<unknown, unknown>, options?: RunOptions): Promise<ExecutionReport>;
    execute(taskId: string, options?: RunOptions): Promise<ExecutionReport>;
    tick(options?: { taskId?: string; signal?: AbortSignal }): Promise<number>;
    get(taskId: string, options?: ReportOptions): Promise<ExecutionReport | null>;
    list(query?: { after?: string; limit?: number; state?: string; flow?: string }): Promise<{ items: ExecutionReport[]; next: string | null }>;
    preview(input: Submission<unknown, unknown>, options?: { signal?: AbortSignal }): Promise<ExecutionReport & { readonly preview: true; readonly snapshot: SnapshotLease; readonly inference: false; readonly delivery: false }>;
    /** Retry a prepared payload after definite failure; planning failures require a new explicit execution. */
    retryDelivery(deliveryId: string): Promise<ExecutionReport | null>;
    resolveDelivery(deliveryId: string, resolution: { status: 'acknowledged' | 'failed'; receipt?: JsonValue }): Promise<ExecutionReport | null>;
    resolveEvaluation(taskId: string, resolution: { status: 'succeeded'; reason: string; result: unknown } | { status: 'failed'; reason: string }, options?: { signal?: AbortSignal }): Promise<ExecutionReport | null>;
    recordCompletion(deliveryId: string, completion: DownstreamCompletion & { readonly status: 'completed' | 'failed' }): Promise<ExecutionReport | null>;
    cancel(taskId: string): Promise<ExecutionReport | null>;
    recover(): Promise<number>; maintain(): Promise<{ removed: number }>; inspect(): ReturnType<ExecutionStore['inspect']>; pending(): Promise<number>;
    pause(type: 'flow' | 'destination', name: string, paused?: boolean): Promise<void>;
    reprocess(taskId: string, options?: { flow?: string; idempotencyKey?: string; signal?: AbortSignal }): Promise<ExecutionReport>;
    redeliver(taskId: string, options: { flow: string; idempotencyKey?: string; signal?: AbortSignal }): Promise<ExecutionReport>;
    reevaluate(taskId: string, options?: { flow?: string; signal?: AbortSignal }): Promise<ExecutionReport>;
    openSnapshot(taskId: string): Promise<SnapshotLease>;
    listSnapshot(snapshotId: string, query?: SnapshotQuery): Promise<SnapshotPage>;
    readSnapshot(snapshotId: string, partId: string, query?: { offset?: number; maxBytes?: number }): Promise<DataPage>;
    readValue(snapshotId: string, partId: string): Promise<ReadonlyJsonValue>;
    closeSnapshot(snapshotId: string): Promise<void>;
    describeSnapshot(snapshotId: string): Promise<SnapshotLease>;
    readExportFrames(snapshotId: string, query?: { after?: string; limit?: number }): Promise<ExportPage>;
    openImport(options: { flow: string }): Promise<ImportLease>;
    appendImport(transferId: string, frame: TransferFrame): Promise<ImportProgress>;
    finishImport(transferId: string, options?: { signal?: AbortSignal }): Promise<ExecutionReport>;
    closeImport(transferId: string): Promise<void>;
    describeImport(transferId: string): Promise<ImportLease>;
    exportTask(taskId: string, options?: { signal?: AbortSignal }): AsyncIterable<Uint8Array>;
    /** Validate the complete framed stream before admitting a new root execution. */
    importTask(chunks: AsyncIterable<Uint8Array> | Iterable<Uint8Array>, options: { flow: string; signal?: AbortSignal }): Promise<ExecutionReport>;
    reply(taskId: string, callback: (report: ExecutionReport | null, context: { signal: AbortSignal }) => unknown | Promise<unknown>, options?: { signal?: AbortSignal; timeoutMs?: number }): Promise<unknown>;
    acknowledge(taskId: string, callback: (context: { taskId: string; signal: AbortSignal }) => unknown | Promise<unknown>, options?: { signal?: AbortSignal; timeoutMs?: number }): Promise<unknown>;
    close(options?: { cancel?: boolean }): Promise<void>;
}
export interface RunnerOptions {
    /** Identity fields are validated at construction; placements are captured as an exact label set. */
    flows: Readonly<Record<string, Flow>>; report?: ReportOptions; store?: ExecutionStore; namespace?: string; workerId?: string; placements?: readonly string[]; limits?: ResourceLimits;
    evaluators?: Record<string, Evaluator<any, any>>; validators?: Record<string, ResultValidator<any>>; connectors?: Record<string, OutgoingConnector>;
    policies?: Record<string, { revision: string; execute(result: unknown, options: JsonValue, context: { signal: AbortSignal }): PolicyDecision | Promise<PolicyDecision> }>;
    transforms?: Record<string, TransformExtension>; validateOutput?: (schema: object | boolean, value: JsonValue) => boolean | void | Promise<boolean | void>;
    onEvent?: (event: Readonly<Record<string, unknown>>, context: { signal: AbortSignal }) => void | Promise<void>;
}
export function createRunner(options: RunnerOptions): Runner;
export type ExtensionKind = 'incoming' | 'outgoing' | 'evaluator' | 'validator' | 'policy' | 'transform' | 'store';
export interface Extension { apiVersion: 1; revision: string; runtime?: 'portable' | 'node'; schema: object; create(options: unknown, context?: unknown): unknown | Promise<unknown>; }
export interface Registry { register(kind: ExtensionKind, name: string, extension: Extension): Registry; get(kind: ExtensionKind, name: string): Extension; describe(): readonly unknown[]; }
export function createRegistry(): Registry;
export function createTelemetryObserver(options?: { exportEvent?: (event: Readonly<Record<string, unknown>>) => void | Promise<void>; maxSeries?: number }): { onEvent: NonNullable<RunnerOptions['onEvent']>; snapshot(): readonly unknown[] };
export interface IncomingMessage { input: Submission<unknown, unknown>; acknowledge?: (context: unknown) => unknown | Promise<unknown>; reply?: (report: unknown, context?: unknown) => unknown | Promise<unknown>; reject?: (error: ErrorDetails, context?: unknown) => unknown | Promise<unknown>; }
export interface SourceCloseOptions { signal?: AbortSignal; timeoutMs?: number; }
export interface IncomingConnector { name?: string; revision?: string; capabilities?: ConnectorCapabilities; open?(options: { signal: AbortSignal }): AsyncIterable<IncomingMessage> | Promise<AsyncIterable<IncomingMessage>>; start?(options: { runner: Runner; signal: AbortSignal; name: string; role: string; whenActive?: Promise<void> }): Promise<void>; health?(): { ready: boolean } | Promise<{ ready: boolean }>; close?(options?: SourceCloseOptions): void | Promise<void>; }
export interface ConsumableIncomingConnector extends IncomingConnector {
    name: string; revision: string; capabilities: ConnectorCapabilities;
    open(options: { signal: AbortSignal }): AsyncIterable<IncomingMessage> | Promise<AsyncIterable<IncomingMessage>>;
}
export function createIterableSource(iterable: Iterable<IncomingMessage> | AsyncIterable<IncomingMessage>, options: { name: string; revision?: string; runtime?: 'portable' | 'node' }): ConsumableIncomingConnector;
export interface ConsumeSourceOptions {
    acknowledge?: 'admitted' | 'completed' | 'none';
    concurrency?: number;
    signal?: AbortSignal;
    onOutcome?: (outcome: unknown, context: { signal: AbortSignal }) => void | Promise<void>;
    processingTimeoutMs?: number;
    sourceTimeoutMs?: number;
    acknowledgementTimeoutMs?: number;
    replyTimeoutMs?: number;
    observerTimeoutMs?: number;
    closeTimeoutMs?: number;
}
export function consumeSource(runner: Runner, source: ConsumableIncomingConnector, options?: ConsumeSourceOptions): Promise<unknown>;
export interface AggregationRule { id?: string; revision: string; sources: readonly string[]; key: string; windowMs: number; dedupeMs?: number; maxGroups?: number; maxEvents?: number; maxBytes?: number; duplicate?: 'ignore' | 'reject' | 'replace'; late?: 'reject' | 'new-window'; incomplete?: 'discard' | 'emit'; }
export function createAggregator(rule: AggregationRule): { add(event: unknown, options?: { now?: number }): unknown; flush(options?: { now?: number }): readonly unknown[]; inspect(): unknown; close(options?: { now?: number }): readonly unknown[] };
export interface DurableAggregationRule extends Omit<AggregationRule, 'id' | 'key' | 'maxBytes' | 'maxEvents' | 'late'> { id: string; flow: string; late?: 'reject'; }
export function createDurableAggregator(options: { runner: Runner; rule: DurableAggregationRule }): { add(event: { source: string; eventId: string; correlationKey: string | number; event: JsonValue }): Promise<unknown>; flush(): Promise<readonly unknown[]> };
