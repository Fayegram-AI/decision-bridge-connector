import { assert, fingerprint, fingerprintCaptured, freeze, identifier, isRecord, keysOnly, snapshot } from '../util.mjs';
import { validatePolicy } from '../policy.mjs';
import { validateProjection } from '../mapping.mjs';
import { validateTransform } from './transform.mjs';
import { validateChainRetry } from './chaining.mjs';
import { registeredEntry } from './registry.mjs';
import { captureSubmission } from './input-capture.mjs';

export const RECORD_VERSION = 3;
export const LIMITS = Object.freeze({ maxPending: 10000, maxBytes: 1048576, evaluationConcurrency: 4,
    deliveryConcurrency: 16, optionalConcurrency: 4, perDestination: 4, leaseMs: 30000,
    pageSize: 100, retentionMs: 604800000, dedupeMs: 604800000,
    maxTransfers: 16, transferIdleMs: 300000, maxTransferBytes: 1073741824 });
const conditions = ['evaluation.succeeded', 'decision.accepted', 'decision.review', 'decision.abstained', 'failure', 'always'];
const payloads = ['raw-result', 'final-decision', 'final-decision-with-result', 'failure', 'report'];

function validateSettings(settings) {
    for (const key of ['correlationId', 'causationId', 'questionRevision']) if (settings[key] !== undefined) identifier(settings[key], key);
    if (settings.deadlineAt !== undefined) assert(typeof settings.deadlineAt === 'string' && Number.isFinite(Date.parse(settings.deadlineAt)), 'Invalid deadline.');
}

/** Rate gates persist timestamps rather than timers, but their arithmetic must remain representable. */
function validateRate(rate, label) {
    if (rate === undefined) return;
    assert(Number.isFinite(rate) && rate > 0 && Number.isSafeInteger(Date.now() + Math.ceil(1000 / rate)), `Invalid ${label}.`);
}

/** A flow is an immutable authority boundary, not a caller-supplied routing hint. */
export async function compileFlows(options) {
    assert(isRecord(options.flows) && Object.keys(options.flows).length > 0, 'At least one named flow is required.');
    const result = Object.create(null);
    const registered = (kind, name) => registeredEntry(options[kind], name);
    function extension(kind, name, method) {
        identifier(name, 'Extension name');
        const entry = registered(kind, name);
        assert(typeof entry?.[method] === 'function', `Registered ${kind} extension requires ${method}().`);
        identifier(entry.revision, 'Extension revision');
        return entry;
    }
    for (const [name, input] of Object.entries(options.flows)) {
        identifier(name, 'Flow name');
        const flow = snapshot(input);
        keysOnly(flow, ['revision', 'operation', 'inputs', 'evaluator', 'validator', 'template', 'policies', 'connections',
            'defaults', 'locked', 'overrides', 'timeoutMs', 'deliveryTtlMs', 'maxAgeMs', 'stale', 'maxDepth', 'maxExecutions', 'concurrency', 'rate'], 'Flow');
        identifier(flow.revision, 'Flow revision');
        flow.operation ??= 'decision';
        assert(['decision', 'extraction', 'live'].includes(flow.operation), 'Unsupported operation.');
        flow.inputs ??= ['request', 'event', 'result'];
        assert(flow.inputs.length > 0 && flow.inputs.every(x => ['request', 'event', 'result'].includes(x)), 'Invalid input kinds.');
        const evaluator = flow.evaluator === undefined ? undefined : extension('evaluators', flow.evaluator, 'execute');
        const validator = flow.validator === undefined ? undefined : extension('validators', flow.validator, 'validate');
        for (const method of ['validate', 'validateSaved']) if (evaluator?.[method] !== undefined)
            assert(typeof evaluator[method] === 'function', `Evaluator ${method} must be callable.`);
        if (evaluator?.operation !== undefined) assert(evaluator.operation === flow.operation, 'Evaluator operation does not match its flow.');
        if (flow.inputs.some(x => x !== 'result')) assert(evaluator, 'Flow evaluator is not registered.');
        if (flow.inputs.includes('event')) validateProjection(flow.template);
        if (flow.inputs.includes('result')) assert(validator || typeof evaluator?.validateSaved === 'function',
            'Result admission requires a registered validator.');
        flow.policies ??= {};
        assert(Object.keys(flow.policies).length <= 64, 'Use at most 64 policy profiles.');
        for (const [policy, spec] of Object.entries(flow.policies)) {
            identifier(policy);
            if (spec.type === 'module') spec.revision = extension('policies', spec.name, 'execute').revision;
            else validatePolicy(spec);
        }
        flow.defaults ??= {}; flow.locked ??= {}; flow.overrides ??= [];
        const permitted = ['context', 'correlationId', 'causationId', 'questionRevision', 'deadlineAt'];
        assert(flow.overrides.every(key => permitted.includes(key)), 'Unsupported caller override.');
        for (const value of [flow.defaults, flow.locked]) { keysOnly(value, permitted, 'Flow settings'); validateSettings(value); }
        flow.timeoutMs ??= 60000; flow.deliveryTtlMs ??= 3600000; flow.maxDepth ??= 8; flow.maxExecutions ??= 64;
        flow.concurrency ??= 4; flow.stale ??= 'hold';
        validateRate(flow.rate, 'flow rate');
        for (const key of ['timeoutMs', 'deliveryTtlMs', 'maxDepth', 'maxExecutions', 'concurrency'])
            assert(Number.isSafeInteger(flow[key]) && flow[key] > 0, `Invalid ${key}.`);
        assert(flow.timeoutMs <= 2147483647, 'Flow timeout exceeds the timer limit.');
        if (flow.maxAgeMs !== undefined) assert(Number.isSafeInteger(flow.maxAgeMs) && flow.maxAgeMs > 0, 'Invalid result age.');
        assert(['hold', 'expire'].includes(flow.stale), 'Stale action must be hold or expire; route review through a policy.');
        flow.connections ??= [];
        assert(Array.isArray(flow.connections) && flow.connections.length <= 128, 'Use at most 128 connections per flow.');
        const ids = new Set();
        for (const connection of flow.connections) {
            keysOnly(connection, ['id', 'connector', 'revision', 'policy', 'when', 'payload', 'projection', 'transforms', 'encoding',
                'required', 'timeoutMs', 'retry', 'chainRetry', 'ordering', 'dependsOn', 'destination', 'placement', 'outputSchema', 'nextFlow', 'rate'], 'Connection');
            if (connection.nextFlow !== undefined || connection.chainRetry !== undefined) {
                identifier(connection.nextFlow, 'nextFlow');
                validateChainRetry(connection.chainRetry);
            }
            identifier(connection.id); assert(connection.id.length <= 128, 'Connection id exceeds 128 characters.'); assert(!ids.has(connection.id), 'Duplicate connection id.'); ids.add(connection.id);
            const connector = extension('connectors', connection.connector, 'send');
            if (connection.placement !== undefined) identifier(connection.placement, 'Connection placement');
            assert(conditions.includes(connection.when) && payloads.includes(connection.payload), 'Invalid output condition or payload.');
            if (connection.policy !== undefined || connection.when.startsWith('decision.') || connection.payload.startsWith('final-decision')) {
                identifier(connection.policy, 'Connection policy');
                assert(Object.hasOwn(flow.policies, connection.policy), 'Output requires a flow policy profile.');
            }
            if (connection.projection !== undefined) validateProjection(connection.projection);
            validateTransform(connection.transforms);
            for (const step of connection.transforms ?? []) if (step.type === 'module')
                extension('transforms', step.name, 'execute');
            if (connection.outputSchema !== undefined) {
                assert(isRecord(connection.outputSchema) || typeof connection.outputSchema === 'boolean', 'Output schema must be an object or boolean.');
                assert(typeof options.validateOutput === 'function', 'An output schema requires a configured validator.');
            }
            connection.required ??= true; connection.timeoutMs ??= 10000;
            validateRate(connection.rate, 'destination rate');
            if (connection.ordering) {
                keysOnly(connection.ordering, ['key', 'onFailure'], 'Ordering'); identifier(connection.ordering.key);
                assert(['block', 'continue'].includes(connection.ordering.onFailure ?? 'block'), 'Invalid ordering failure policy.');
            }
            assert(typeof connection.required === 'boolean' && Number.isSafeInteger(connection.timeoutMs) && connection.timeoutMs > 0 && connection.timeoutMs <= 2147483647, 'Invalid delivery bounds.');
            if (connection.retry !== undefined) keysOnly(connection.retry, ['maxAttempts', 'delayMs', 'maxDelayMs', 'ambiguous'], 'Delivery retry');
            connection.retry = { maxAttempts: 3, delayMs: 100, maxDelayMs: 30000, ambiguous: 'hold', ...connection.retry };
            assert(['hold', 'retry'].includes(connection.retry.ambiguous), 'Invalid ambiguity policy.');
            assert(connection.retry.ambiguous !== 'retry' || connector.capabilities?.idempotent, 'Ambiguous retry requires recipient idempotency.');
            for (const key of ['maxAttempts', 'delayMs', 'maxDelayMs']) assert(Number.isSafeInteger(connection.retry[key]) && connection.retry[key] > 0, 'Invalid retry bounds.');
            assert(connection.retry.maxAttempts <= 100, 'A delivery permits at most 100 attempts.');
            connection.encoding ??= 'json';
            assert(['json', 'text', 'jsonl'].includes(connection.encoding), 'Invalid output encoding.');
            assert(connection.dependsOn === undefined || Array.isArray(connection.dependsOn), 'Delivery dependencies must be an array of connection IDs.');
            connection.dependsOn ??= [];
            for (const dependency of connection.dependsOn) identifier(dependency, 'Delivery dependency');
            connection.connectorRevision = connector.revision;
            connection.destinationKey = await fingerprint({ connector: connection.connector, destination: connection.destination ?? null });
            connection.revision ??= await fingerprint(connection);
            if (connector.capabilities?.runtime === 'node' && connector.capabilities?.local === true)
                assert(connection.placement, 'Local connectors require a worker placement label.');
        }
        const done = new Set(); const visiting = new Set();
        function visit(connection) {
            assert(!visiting.has(connection.id), 'Cyclic delivery dependency.');
            if (done.has(connection.id)) return;
            visiting.add(connection.id);
            for (const dependency of connection.dependsOn) {
                const parent = flow.connections.find(x => x.id === dependency);
                assert(parent && parent.id !== connection.id, 'Unknown delivery dependency.'); visit(parent);
            }
            visiting.delete(connection.id); done.add(connection.id);
        }
        flow.connections.forEach(visit);
        flow.evaluatorRevision = evaluator?.revision ?? null;
        flow.validatorRevision = validator?.revision ?? flow.evaluatorRevision;
        flow.transformRevisions = Object.fromEntries(flow.connections.flatMap(connection => (connection.transforms ?? []).filter(step => step.type === 'module')
            .map(step => [step.name, options.transforms[step.name].revision])));
        result[name] = { ...flow, id: name };
    }
    for (const flow of Object.values(result)) for (const connection of flow.connections)
        if (connection.nextFlow) assert(result[connection.nextFlow]?.inputs.includes('event'), 'Chained flow must accept events.');
    // Hash the reachable definitions once per root, without recursively hashing revisions.
    // This pins downstream contracts while keeping cyclic chains deterministic.
    const definitions = { ...result };
    for (const [name, flow] of Object.entries(definitions)) {
        const reachable = new Set(); const queue = [name];
        while (queue.length) {
            const current = queue.pop();
            if (reachable.has(current)) continue;
            reachable.add(current);
            for (const connection of definitions[current].connections) if (connection.nextFlow) queue.push(connection.nextFlow);
        }
        const runtimeRevision = await fingerprintCaptured({ schemaVersion: RECORD_VERSION, flow: name, definitions: [...reachable].sort().map(id => definitions[id]) });
        result[name] = freeze({ ...flow, runtimeRevision });
    }
    return Object.freeze(result);
}

export function normalizeSubmission(input, flow, source = 'sdk', namespace = 'default', maxBytes = LIMITS.maxBytes, aggregateField) {
    // The envelope combines independent values; duplicating internal evidence must
    // never consume another value's node allowance.
    const data = captureSubmission(input, maxBytes, aggregateField);
    keysOnly(data, ['flow', 'kind', 'request', 'event', 'result', 'evidenceAt', 'validation', 'idempotencyKey',
        'context', 'correlationId', 'causationId', 'questionRevision', 'deadlineAt'], 'Submission');
    assert(flow && flow.inputs.includes(data.kind), 'Input kind is not permitted by this flow.');
    assert(['request', 'event', 'result'].filter(key => Object.hasOwn(data, key)).length === 1 && Object.hasOwn(data, data.kind), 'Supply exactly one input form.');
    const settings = { ...flow.defaults };
    for (const key of ['context', 'correlationId', 'causationId', 'questionRevision', 'deadlineAt']) {
        if (!Object.hasOwn(data, key)) continue;
        assert(flow.overrides.includes(key) && !Object.hasOwn(flow.locked, key), `Caller override is not permitted: ${key}`);
        settings[key] = data[key];
    }
    if (data.idempotencyKey !== undefined) identifier(data.idempotencyKey, 'Idempotency key');
    if (data.evidenceAt !== undefined) assert(typeof data.evidenceAt === 'string' && Number.isFinite(Date.parse(data.evidenceAt)), 'Invalid evidence timestamp.');
    if (data.evidenceAt !== undefined) assert(Date.parse(data.evidenceAt) <= Date.now() + 60000, 'Evidence timestamp is in the future.');
    const effective = { ...settings, ...flow.locked };
    validateSettings(effective);
    return freeze({ ...data, ...effective, source: identifier(source), namespace: identifier(namespace) });
}
