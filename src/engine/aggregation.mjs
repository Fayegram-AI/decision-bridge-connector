import { assert, fingerprint, freeze, id, identifier, keysOnly, snapshot } from '../util.mjs';
import { ADMISSION_OWNER } from './admission-owner.mjs';
import { ADMISSION_LIFETIME } from './admission-lifetime.mjs';
import { AGGREGATE_EVENT, captureAggregateEvent, captureAggregateMembers } from './aggregation-values.mjs';

const hasEvent = (events, source) => Object.hasOwn(events, source);
// Internal host preflight metadata; custom aggregators need no additional methods.
export const AGGREGATION_RULE = Symbol('aggregation-rule');
export const AGGREGATION_GENERATION = Symbol('aggregation-generation');
const seen = (group, source, eventId) => (hasEvent(group.seen ?? {}, source) && group.seen[source].includes(eventId)) || (hasEvent(group.events, source) && group.events[source].eventId === eventId);

/** Processing-time fan-in retains groups and emission identities in the task store. */
export function createDurableAggregator({ runner, rule }) {
    rule = snapshot(rule);
    keysOnly(rule, ['id', 'revision', 'sources', 'windowMs', 'flow', 'late', 'duplicate', 'incomplete', 'dedupeMs', 'maxGroups'], 'Durable aggregation rule');
    assert(rule.id && rule.revision && Array.isArray(rule.sources) && rule.sources.length && rule.windowMs > 0 && rule.flow, 'Incomplete aggregation rule.');
    for (const value of [rule.id, rule.revision, rule.flow, ...rule.sources]) identifier(value);
    for (const key of ['windowMs', 'dedupeMs', 'maxGroups']) if (rule[key] !== undefined) assert(Number.isSafeInteger(rule[key]) && rule[key] > 0, 'Invalid aggregation bound.');
    assert(['reject', 'ignore', 'replace'].includes(rule.duplicate ?? 'reject') && ['discard', 'emit'].includes(rule.incomplete ?? 'discard'), 'Invalid aggregation disposition.');
    assert(new Set(rule.sources).size === rule.sources.length && rule.sources.length <= 64, 'Invalid aggregation membership.');
    assert(rule.late === undefined || rule.late === 'reject', 'Durable aggregation rejects late evidence; use a new correlation key for a new group.');
    const store = runner.store; const namespace = runner.namespace;
    const sourceName = `aggregate:${rule.id}`;
    async function generation() {
        const flow = (await runner.ready)[rule.flow];
        assert(flow?.inputs.includes('event'), 'Aggregation target must accept events.');
        return { runtimeRevision: await fingerprint({ rule, flow: flow.runtimeRevision }), flowRevision: flow.runtimeRevision };
    }
    const envelope = (group, status) => ({ aggregateId: group.aggregateId, status,
        correlationKey: group.correlationKey, events: group.events,
        missingSources: status === 'complete' ? [] : rule.sources.filter(source => !hasEvent(group.events, source)) });
    async function emit(group) {
        if (!['complete', 'incomplete'].includes(group.state) || group.emitted) return group;
        if (group.state === 'incomplete' && rule.incomplete !== 'emit') return group;
        await runner.submit({ flow: rule.flow, kind: 'event', event: envelope(group, group.state),
        idempotencyKey: group.aggregateId }, { source: sourceName.length <= 256 ? sourceName : `aggregate:${await fingerprint(rule.id)}`,
            [AGGREGATE_EVENT]: true,
            [ADMISSION_OWNER]: { kind: 'aggregate', key: group.key, aggregateId: group.aggregateId } });
        return store.aggregate(namespace, group.key, current => {
            assert(current?.aggregateId === group.aggregateId, 'Aggregation group changed during emission.', 'CONFLICT');
            if (current.emitted) return current;
            return { ...current, emitted: true, expiresAt: Date.now() + (rule.dedupeMs ?? 86400000) };
        });
    }
    const operations = {
        async add({ source, eventId, correlationKey, event }) {
            const { runtimeRevision, flowRevision } = await generation();
            const key = await fingerprint([rule.id, runtimeRevision, correlationKey]);
            const group = await store.aggregate(namespace, key, previous => {
                let current = previous;
                let initialized = false;
                if (!current || (current.emitted || current.state === 'discarded') && Date.now() >= current.expiresAt) { initialized = true; current = {
                    key, aggregateId: id('aggregate'), ruleId: rule.id, ruleRevision: rule.revision, revision: flowRevision, runtimeRevision, correlationKey,
                    state: 'pending', createdAt: Date.now(), expiresAt: Date.now() + rule.windowMs, events: {}, seen: {}, emitted: false }; }
                if (current.state !== 'pending') {
                    assert(seen(current, source, eventId), 'Aggregation window is closed.', 'LATE_EVENT');
                    return current;
                }
                // The first contribution establishes its window even if a short clock tick elapses.
                if (!initialized && Date.now() >= current.expiresAt) return { ...current, state: rule.incomplete === 'emit' ? 'incomplete' : 'discarded', closedAt: Date.now(),
                    ...(rule.incomplete === 'emit' ? {} : { expiresAt: Date.now() + (rule.dedupeMs ?? 86400000) }) };
                if (seen(current, source, eventId)) return current;
                if (hasEvent(current.events, source)) {
                    if (rule.duplicate === 'ignore') return current;
                    assert(rule.duplicate === 'replace', 'Aggregation member already supplied evidence.', 'DUPLICATE_MEMBER');
                }
                const events = captureAggregateMembers({ ...current.events, [source]: { eventId, event } }, store.limits.maxBytes);
                const history = { ...current.seen, [source]: [...(hasEvent(current.seen ?? {}, source) ? current.seen[source] : []), eventId] };
                const complete = rule.sources.every(name => hasEvent(events, name));
                // Reserve space for the generated envelope before retaining a member.
                captureAggregateEvent(envelope({ ...current, events }, !complete && rule.incomplete === 'emit' ? 'incomplete' : 'complete'), store.limits.maxBytes);
                return { ...current, events, seen: snapshot(history, { maxBytes: store.limits.maxBytes }), ...(complete ? { state: 'complete', closedAt: Date.now() } : {}) };
            }, rule.maxGroups ?? 1000);
            const result = await emit(group);
            assert(group.state === 'pending' || seen(group, source, eventId) || hasEvent(group.events, source) && rule.duplicate === 'ignore',
                'Aggregation window is closed.', 'LATE_EVENT');
            return result;
        },
        async flush() {
            const emitted = []; const failures = [];
            const { runtimeRevision } = await generation();
            for (const group of await store.dueAggregates(namespace, runtimeRevision)) {
                if (group.ruleId !== rule.id || group.runtimeRevision !== runtimeRevision) continue;
                try {
                    const closed = await store.aggregate(namespace, group.key, current => {
                        if (!current || current.aggregateId !== group.aggregateId || current.emitted || current.state === 'discarded' ||
                            current.state === 'pending' && current.expiresAt > Date.now()) return null;
                        return current.state === 'pending'
                            ? { ...current, state: rule.incomplete === 'emit' ? 'incomplete' : 'discarded', closedAt: Date.now(),
                                ...(rule.incomplete === 'emit' ? {} : { expiresAt: Date.now() + (rule.dedupeMs ?? 86400000) }) } : current;
                    });
                    if (closed) emitted.push(await emit(closed));
                } catch (error) { failures.push(error); }
            }
            // Preserve readiness/error reporting after independent groups have had their turn.
            if (failures.length) throw failures[0];
            return emitted;
        },
    };
    const admissions = runner[ADMISSION_LIFETIME];
    const track = operation => admissions ? admissions.track(operation) : operation();
    return Object.freeze({
        [AGGREGATION_RULE]: freeze(rule), [AGGREGATION_GENERATION]: generation,
        async add({ source, eventId, correlationKey, event }) {
            assert(rule.sources.includes(source), 'Unknown aggregation member.'); identifier(eventId);
            assert(typeof correlationKey === 'string' && correlationKey.length > 0 || typeof correlationKey === 'number' && Number.isFinite(correlationKey),
                'Correlation key must be a nonempty string or finite number.');
            const captured = snapshot(event, { maxBytes: store.limits.maxBytes });
            return track(() => operations.add({ source, eventId, correlationKey, event: captured }));
        },
        async flush() { return track(() => operations.flush()); },
    });
}
