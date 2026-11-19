import { assert, freeze, getPath, id, identifier, keysOnly, snapshot } from './util.mjs';
import { validatePath } from './mapping.mjs';

const own = (value, key) => Object.hasOwn(value, key);
const MAX_TIME = 8640000000000000;
const integer = (value, min, max) => Number.isSafeInteger(value) && value >= min && value <= max;

export function validateAggregationRule(rule) {
  const clean = snapshot(rule);
  keysOnly(clean, ['id', 'revision', 'sources', 'key', 'windowMs', 'dedupeMs', 'maxGroups', 'maxEvents', 'maxBytes', 'duplicate', 'late', 'incomplete'], 'Aggregation rule');
  if (own(clean, 'id')) identifier(clean.id, 'Aggregation id');
  identifier(clean.revision, 'Aggregation revision');
  assert(Array.isArray(clean.sources) && clean.sources.length > 0 && clean.sources.length <= 64, 'Aggregation requires 1–64 explicit sources.');
  clean.sources.forEach(source => identifier(source, 'Aggregation source'));
  assert(new Set(clean.sources).size === clean.sources.length, 'Aggregation sources must be unique.');
  validatePath(clean.key);
  assert(clean.key !== '' && clean.key !== '$', 'Aggregation requires an explicit scalar correlation path.');
  assert(integer(clean.windowMs, 1, 2147483647), 'Aggregation windowMs must be a positive 32-bit integer.');
  const configured = { dedupeMs: clean.windowMs, maxGroups: 1000, maxEvents: 10000, maxBytes: 8 * 1024 * 1024,
    duplicate: 'ignore', late: 'reject', incomplete: 'discard', ...clean };
  assert(integer(configured.dedupeMs, 1, 2147483647), 'Aggregation dedupeMs must be a positive 32-bit integer.');
  assert(integer(configured.maxGroups, 1, 100000), 'Aggregation maxGroups must be 1–100000.');
  assert(integer(configured.maxEvents, configured.sources.length, 1000000), 'Aggregation maxEvents must cover the source list and be at most 1000000.');
  assert(integer(configured.maxBytes, 256, 64 * 1024 * 1024), 'Aggregation maxBytes must be between 256 bytes and 64 MiB.');
  assert(['ignore', 'reject', 'replace'].includes(configured.duplicate), 'Unknown aggregation duplicate behavior.');
  assert(['reject', 'new-window'].includes(configured.late), 'Unknown aggregation late behavior.');
  assert(['discard', 'emit'].includes(configured.incomplete), 'Unknown aggregation incomplete behavior.');
  return freeze(configured);
}

/** Bounded, explicit processing-time fan-in. It owns no timers or persistent store. */
export function createAggregator(rule) {
  const configured = validateAggregationRule(rule);
  const membership = new Set(configured.sources);
  let groups = new Map();
  let lastNow = 0;
  let closed = false;

  function nowFrom(options = {}) {
    const call = snapshot(options);
    keysOnly(call, ['now'], 'Aggregation call options');
    const now = call.now === undefined ? Date.now() : call.now;
    assert(integer(now, 0, MAX_TIME - configured.windowMs - configured.dedupeMs), 'Aggregation now must be a valid nonnegative epoch timestamp.');
    // Backward wall-clock adjustments cannot reopen a window or extend its age.
    return Math.max(now, lastNow);
  }

  function describe(group, status = group.status, at) {
    const events = configured.sources.filter(source => group.members.has(source)).map(source => group.members.get(source));
    return freeze(snapshot({ aggregateId: group.id, ...(configured.id ? { ruleId: configured.id } : {}),
      ruleRevision: configured.revision, correlationKey: group.key, status,
      openedAt: new Date(group.openedAt).toISOString(), deadlineAt: new Date(group.deadlineAt).toISOString(),
      ...(at === undefined ? {} : { closedAt: new Date(at).toISOString() }),
      sources: configured.sources, missingSources: configured.sources.filter(source => !group.members.has(source)), events }));
  }

  function measure(group) {
    const encoded = JSON.stringify({ id: group.id, key: group.key, openedAt: group.openedAt, deadlineAt: group.deadlineAt,
      retainUntil: group.retainUntil, status: group.status, events: [...group.members.values()], seen: [...group.seen.values()] });
    assert(encoded.length <= configured.maxBytes, 'Aggregation retained JSON exceeds maxBytes.', 'AGGREGATION_CAPACITY');
    return new TextEncoder().encode(encoded).byteLength;
  }

  function tally(state) {
    let bytes = 0;
    let events = 0;
    let pending = 0;
    for (const group of state.values()) {
      bytes += group.bytes;
      events += group.seen.size;
      if (group.status === 'pending') pending++;
    }
    return { groups: state.size, pending, tombstones: state.size - pending, events, retainedJsonBytes: bytes };
  }

  function checkCapacity(state) {
    const size = tally(state);
    assert(size.groups <= configured.maxGroups && size.events <= configured.maxEvents && size.retainedJsonBytes <= configured.maxBytes,
      'Aggregation capacity exhausted; flush expired groups or apply backpressure.', 'AGGREGATION_CAPACITY');
  }

  function sealed(group, at, status) {
    const tombstone = { ...group, status, retainUntil: at + configured.dedupeMs, members: new Map() };
    tombstone.bytes = measure(tombstone);
    return tombstone;
  }

  function expire(now) {
    // Transactions use immutable group replacements; a rejected add cannot consume an emitted expiry.
    const next = new Map(groups);
    const expired = [];
    for (const [groupId, group] of next) {
      let current = group;
      if (group.status === 'pending' && group.deadlineAt <= now) {
        const status = configured.incomplete === 'emit' ? 'incomplete' : 'discarded';
        expired.push(describe(group, status, group.deadlineAt));
        current = sealed(group, group.deadlineAt, status);
        next.set(groupId, current);
      }
      if (current.status !== 'pending' && current.retainUntil <= now) next.delete(groupId);
    }
    return { next, expired };
  }

  function eventInput(envelope) {
    const clean = snapshot(envelope);
    keysOnly(clean, ['source', 'eventId', 'correlationId', 'causationId', 'occurredAt', 'event'], 'Aggregation envelope');
    let source;
    let eventId;
    if (typeof clean.source === 'string') { source = clean.source; eventId = clean.eventId; }
    else {
      keysOnly(clean.source, ['connector', 'eventId'], 'Aggregation source');
      source = clean.source.connector;
      eventId = clean.eventId ?? clean.source.eventId;
      assert(!own(clean, 'eventId') || !own(clean.source, 'eventId') || clean.eventId === clean.source.eventId, 'Aggregation event IDs disagree.');
    }
    identifier(source, 'Event source');
    identifier(eventId, 'Source event id');
    assert(membership.has(source), `Source is not an aggregation member: ${source}`, 'AGGREGATION_UNKNOWN_SOURCE');
    assert(own(clean, 'event'), 'Aggregation envelope requires an event.');
    for (const key of ['correlationId', 'causationId']) if (own(clean, key)) identifier(clean[key], key);
    if (own(clean, 'occurredAt')) assert(typeof clean.occurredAt === 'string' && Number.isFinite(Date.parse(clean.occurredAt)), 'occurredAt must be a timestamp string.');
    const key = getPath(clean, configured.key);
    assert((typeof key === 'string' && key.length > 0 && key.length <= 512) || (typeof key === 'number' && Number.isFinite(key)),
      'Aggregation correlation path must resolve to a nonempty string or finite number.', 'AGGREGATION_MISSING_KEY');
    return { key, keyId: JSON.stringify([typeof key, key]), eventKey: JSON.stringify([source, eventId]),
      item: { source, eventId, event: clean.event, ...Object.fromEntries(['correlationId', 'causationId', 'occurredAt']
        .filter(field => own(clean, field)).map(field => [field, clean[field]])) } };
  }

  return Object.freeze({
    rule: configured,
    capabilities: Object.freeze({ durable: false, window: 'processing-time', explicitMembership: true, boundedDeduplication: true }),

    add(envelope, options = {}) {
      assert(!closed, 'Aggregator is closed.', 'AGGREGATOR_CLOSED');
      const input = eventInput(envelope);
      const now = nowFrom(options);
      const { next, expired } = expire(now);
      let active;
      let terminal;
      for (const group of next.values()) {
        if (group.seen.has(input.eventKey)) {
          assert(group.keyId === input.keyId, 'A source event ID was reused under another correlation key.', 'AGGREGATION_EVENT_CONFLICT');
          assert(configured.duplicate !== 'reject', 'Duplicate source event.', 'AGGREGATION_DUPLICATE');
          groups = next; lastNow = now;
          return freeze({ status: 'duplicate', aggregateId: group.id, reason: 'event_id', expired });
        }
        if (group.keyId === input.keyId) {
          if (group.status === 'pending') active = group;
          else terminal = group;
        }
      }
      if (!active && terminal && configured.late === 'reject') {
        groups = next; lastNow = now;
        return freeze({ status: 'late', aggregateId: terminal.id, reason: 'window_closed', expired });
      }
      if (active?.members.has(input.item.source)) {
        assert(configured.duplicate !== 'reject', 'An aggregation source already supplied an event.', 'AGGREGATION_DUPLICATE');
        if (configured.duplicate === 'ignore') {
          groups = next; lastNow = now;
          return freeze({ status: 'duplicate', aggregateId: active.id, reason: 'source_already_present', expired });
        }
      }
      const group = active ? { ...active, members: new Map(active.members), seen: new Map(active.seen) }
        : { id: id('aggregate'), key: input.key, keyId: input.keyId, openedAt: now, deadlineAt: now + configured.windowMs,
          retainUntil: now + configured.windowMs + configured.dedupeMs, status: 'pending', members: new Map(), seen: new Map() };
      group.members.set(input.item.source, freeze(input.item));
      group.seen.set(input.eventKey, { source: input.item.source, eventId: input.item.eventId });
      group.bytes = measure(group);
      next.set(group.id, group);
      // A completion must obey the evidence budget before its payload becomes a tombstone.
      checkCapacity(next);
      const complete = group.members.size === configured.sources.length;
      const aggregate = describe(group, complete ? 'complete' : 'pending', complete ? now : undefined);
      if (complete) next.set(group.id, sealed(group, now, 'complete'));
      groups = next; lastNow = now;
      return freeze({ status: complete ? 'complete' : 'pending', aggregate, expired });
    },

    flush(options = {}) {
      assert(!closed, 'Aggregator is closed.', 'AGGREGATOR_CLOSED');
      const now = nowFrom(options);
      const { next, expired } = expire(now);
      groups = next; lastNow = now;
      return freeze(expired);
    },

    inspect() {
      return freeze({ closed, ...tally(groups), groupsByStatus: [...groups.values()].map(group => ({ aggregateId: group.id,
        correlationKey: group.key, status: group.status, deadlineAt: new Date(group.deadlineAt).toISOString(),
        retainUntil: new Date(group.retainUntil).toISOString() })) });
    },

    close(options = {}) {
      if (closed) return Object.freeze([]);
      const now = nowFrom(options);
      const { next, expired } = expire(now);
      for (const group of next.values()) if (group.status === 'pending') expired.push(describe(group,
        configured.incomplete === 'emit' ? 'incomplete' : 'discarded', now));
      groups = new Map(); lastNow = now; closed = true;
      return freeze(expired);
    },
  });
}
