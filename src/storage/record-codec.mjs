import { assert, isRecord, snapshot } from '../util.mjs';
import { isDataReference } from './data-ref.mjs';
import { captureCompletion, captureReceiptBody } from '../engine/input-capture.mjs';
import { captureAggregateEvent, captureAggregateMembers } from '../engine/aggregation-values.mjs';
import { captureDiagnostic } from './diagnostics.mjs';

const pick = (value, names) => Object.fromEntries(names.filter(name => value[name] !== undefined).map(name => [name, value[name]]));
const planControls = ['id', 'revision', 'runtimeRevision', 'concurrency', 'rate', 'deliveryTtlMs', 'maxAgeMs', 'stale', 'maxDepth', 'maxExecutions',
    'connector', 'connectorRevision', 'destinationKey', 'nextFlow', 'when', 'payload', 'required', 'retry', 'chainRetry', 'ordering', 'dependsOn', 'timeoutMs', 'encoding'];
const inputControls = ['flow', 'kind', 'idempotencyKey', 'source', 'namespace', 'deadlineAt', 'evidenceAt', 'correlationId', 'causationId', 'questionRevision'];
export const encoded = isDataReference;

/** Split known DTO fields; opaque application values are never traversed for references. */
export async function encodeRecord(table, row, stage) {
    const out = { ...row };
    const put = async (value, kind = 'json', projection = {}) => encoded(value) ? value : stage.put(value, kind, projection);
    async function diagnostic(value) {
        if (value === null || encoded(value)) return value;
        const checked = captureDiagnostic(value);
        return put(checked, 'diagnostic', pick(checked, ['code', 'stage', 'ambiguous', 'retryable', 'optional']));
    }
    async function errors(value) {
        if (encoded(value)) return value;
        const entries = await Promise.all(value.map(diagnostic));
        return put(entries, 'index', { stats: { count: entries.length,
            evaluation: value.filter(error => error.stage === 'evaluation').length,
            requiredOther: value.filter(error => error.stage !== 'delivery' && !error.optional).length } });
    }
    async function receipt(value) {
        if (!isRecord(value)) return value;
        const result = { ...value };
        const body = value.receipt;
        if (body !== undefined) {
            if (encoded(body) || !isRecord(body) || !Object.hasOwn(body, 'response')) result.receipt = await put(body);
            else result.receipt = await evidenceBody(body, 'response', captureReceiptBody, { hasResponse: true });
        }
        if (value.completion !== undefined) result.completion = await completion(value.completion);
        if (value.error !== undefined) result.error = await diagnostic(value.error);
        return result;
    }
    async function completion(value) {
        const projection = pick(value, ['status']);
        if (encoded(value) || !isRecord(value) || !Object.hasOwn(value, 'evidence')) return put(value, 'json', projection);
        return evidenceBody(value, 'evidence', captureCompletion, projection);
    }
    async function evidenceBody(body, field, capture, projection) {
        let checked;
        try { checked = snapshot(body, { maxBytes: stage.maxValueBytes }); }
        catch (error) { if (error.code !== 'RESOURCE_LIMIT') throw error; }
        if (checked !== undefined) return put(checked, 'json', projection);
        checked = capture(body, stage.maxValueBytes);
        // Keep the existing single part for ordinary evidence. Only values that
        // need the extra wrapper level use an index of opaque field references.
        const entries = [];
        for (const [key, value] of Object.entries(checked)) {
            // Large scalar strings are evidence too and must not fill the index.
            const separate = key === field || (value !== null && typeof value === 'object') ||
                (typeof value === 'string' && value.length > 256);
            entries.push([key, separate ? await put(value) : value]);
        }
        return put(Object.fromEntries(entries), 'index', projection);
    }
    async function aggregateValue(value, membersOnly = false) {
        if (encoded(value)) return value;
        try { return await put(value); }
        catch (error) { if (error?.code !== 'RESOURCE_LIMIT') throw error; }
        const captured = (membersOnly ? captureAggregateMembers : captureAggregateEvent)(value, stage.maxValueBytes);
        const members = Object.entries(membersOnly ? captured : captured.events);
        const entries = [];
        for (const [name, member] of members) {
            assert(isRecord(member) && typeof member.eventId === 'string' && Object.hasOwn(member, 'event') &&
                Object.keys(member).length === 2,
                'Invalid aggregate member.', 'VALIDATION_ERROR');
            entries.push([name, { eventId: await put(member.eventId), event: await put(member.event) }]);
        }
        const events = Object.fromEntries(entries);
        if (membersOnly) return put(events, 'index');
        return put({ ...captured, correlationKey: await put(captured.correlationKey), events }, 'index');
    }
    if (row.plan !== undefined) out.plan = await put(row.plan, 'plan', pick(row.plan, planControls));
    for (const name of ['result', 'payload', 'event', 'context', 'seen', 'correlationKey'])
        if (Object.hasOwn(row, name)) out[name] = await put(row[name]);
    if (Object.hasOwn(row, 'request')) out.request = await (row.aggregateRequest ? aggregateValue(row.request) : put(row.request));
    if (Object.hasOwn(row, 'events')) out.events = await (table === 'aggregates' ? aggregateValue(row.events, true) : put(row.events));
    if (row.input !== undefined) {
        out.input = pick(row.input, inputControls);
        for (const [name, value] of Object.entries(row.input)) if (!inputControls.includes(name)) {
            const aggregate = name === 'event' && row.aggregateEvent || name === 'request' && row.aggregateInputRequest;
            out.input[name] = await (aggregate ? aggregateValue(value) : put(value));
        }
    }
    if (row.lineage !== undefined) out.lineage = { ...row.lineage,
        ...(row.lineage.path === undefined ? {} : { path: await put(row.lineage.path, 'plan') }) };
    if (row.errors !== undefined) out.errors = await errors(row.errors);
    if (row.error !== undefined) out.error = await diagnostic(row.error);
    if (row.policies !== undefined && !encoded(row.policies)) {
        const policies = Object.create(null); const stats = { succeeded: 0, failed: 0 };
        for (const [name, entry] of Object.entries(row.policies)) {
            policies[name] = { status: entry.status }; stats[entry.status]++;
            if (entry.decision !== undefined) policies[name].decision = await put(entry.decision);
            if (entry.error !== undefined) policies[name].error = await diagnostic(entry.error);
        }
        out.policies = await put(policies, 'index', { stats });
    }
    if (row.attempts !== undefined) out.attempts = await Promise.all(row.attempts.map(async attempt => ({ ...attempt,
        ...(attempt.error === undefined ? {} : { error: pick(await diagnostic(attempt.error), ['$data', 'ambiguous', 'retryable']) }) })));
    if (row.receipt !== undefined) out.receipt = await receipt(row.receipt);
    if (row.completion !== undefined) out.completion = await completion(row.completion);
    if (row.acceptance !== undefined) out.acceptance = { ...row.acceptance, receipt: await receipt(row.acceptance.receipt) };
    for (const name of ['reply', 'sourceAcknowledgement']) if (row[name] !== undefined) out[name] = await receipt(row[name]);
    if (row.resolution !== undefined) {
        out.resolution = { ...row.resolution };
        if (row.resolution.result !== undefined) out.resolution.result = await put(row.resolution.result);
        if (row.resolution.previousErrors !== undefined) out.resolution.previousErrors = await errors(row.resolution.previousErrors);
        if (row.resolution.reason !== undefined) out.resolution.reason = await put(row.resolution.reason);
    }
    snapshot(out, { maxBytes: 61440 });
    return out;
}
