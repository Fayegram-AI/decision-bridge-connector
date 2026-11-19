import test from 'node:test';
import assert from 'node:assert/strict';
import { createAggregator, validateAggregationRule } from '../src/aggregation.mjs';

const rule = extra => ({ id: 'evidence', revision: '1', sources: ['tests', 'logs'], key: 'correlationId', windowMs: 100, ...extra });
const event = (source, eventId, correlationId = 'task', value = eventId) => ({ source, eventId, correlationId, event: { value } });

test('explicit fan-in retains membership order and keeps correlation groups independent', () => {
  const aggregator = createAggregator(rule());
  assert.equal(aggregator.add(event('logs', 'l1', 'first'), { now: 0 }).status, 'pending');
  assert.equal(aggregator.add(event('tests', 't2', 'second'), { now: 1 }).status, 'pending');
  const completed = aggregator.add(event('tests', 't1', 'first'), { now: 2 });
  assert.equal(completed.status, 'complete');
  assert.deepEqual(completed.aggregate.events.map(item => item.source), ['tests', 'logs']);
  assert.deepEqual(completed.aggregate.events.map(item => item.eventId), ['t1', 'l1']);
  assert.deepEqual(completed.aggregate.missingSources, []);
  assert.equal(completed.aggregate.ruleRevision, '1');
  assert.equal(aggregator.inspect().pending, 1);
  assert.equal(aggregator.inspect().tombstones, 1);
});

test('typed correlation keys are distinct and membership is checked before retaining evidence', () => {
  const aggregator = createAggregator(rule({ key: 'event.key' }));
  aggregator.add({ source: { connector: 'tests', eventId: 'a' }, event: { key: 1 } }, { now: 0 });
  aggregator.add({ source: 'logs', eventId: 'b', event: { key: '1' } }, { now: 0 });
  assert.equal(aggregator.inspect().pending, 2);
  assert.throws(() => aggregator.add({ source: 'unconfigured', eventId: 'c', event: { key: 1 } }, { now: 0 }), { code: 'AGGREGATION_UNKNOWN_SOURCE' });
  assert.throws(() => aggregator.add({ source: 'logs', eventId: 'd', event: {} }, { now: 0 }), { code: 'AGGREGATION_MISSING_KEY' });
  assert.equal(aggregator.inspect().events, 2);
});

test('duplicate event IDs and repeated source contributions follow explicit rules', () => {
  const aggregator = createAggregator(rule());
  aggregator.add(event('tests', 'first'), { now: 0 });
  assert.equal(aggregator.add(event('tests', 'first'), { now: 1 }).reason, 'event_id');
  assert.equal(aggregator.add(event('tests', 'second'), { now: 2 }).reason, 'source_already_present');
  assert.throws(() => aggregator.add(event('tests', 'first', 'other'), { now: 2 }), { code: 'AGGREGATION_EVENT_CONFLICT' });
  const complete = aggregator.add(event('logs', 'log'), { now: 3 });
  assert.equal(complete.aggregate.events[0].eventId, 'first');
  assert.equal(aggregator.add(event('logs', 'log'), { now: 4 }).status, 'duplicate');

  const replacing = createAggregator(rule({ duplicate: 'replace' }));
  replacing.add(event('tests', 'old'), { now: 0 });
  replacing.add(event('tests', 'new'), { now: 1 });
  assert.equal(replacing.add(event('tests', 'old'), { now: 2 }).status, 'duplicate');
  assert.equal(replacing.add(event('logs', 'ready'), { now: 3 }).aggregate.events[0].eventId, 'new');

  const rejecting = createAggregator(rule({ duplicate: 'reject' }));
  rejecting.add(event('tests', 'first'), { now: 0 });
  assert.throws(() => rejecting.add(event('tests', 'second'), { now: 1 }), { code: 'AGGREGATION_DUPLICATE' });
  assert.equal(rejecting.inspect().events, 1);
});

test('window deadlines emit explicit incomplete outcomes; late arrivals do not silently merge', () => {
  const aggregator = createAggregator(rule({ incomplete: 'emit' }));
  aggregator.add(event('tests', 't'), { now: 0 });
  const late = aggregator.add(event('logs', 'l'), { now: 100 });
  assert.equal(late.status, 'late');
  assert.equal(late.expired.length, 1);
  assert.equal(late.expired[0].status, 'incomplete');
  assert.deepEqual(late.expired[0].missingSources, ['logs']);
  assert.equal(late.expired[0].events[0].eventId, 't');
  assert.deepEqual(aggregator.flush({ now: 101 }), []);
  assert.equal(aggregator.add(event('tests', 't'), { now: 102 }).status, 'duplicate');
  aggregator.flush({ now: 200 });
  assert.equal(aggregator.inspect().groups, 0);
  assert.equal(aggregator.add(event('tests', 't'), { now: 201 }).status, 'pending');
});

test('new-window behavior retains old duplicate tombstones for their bounded lifetime', () => {
  const aggregator = createAggregator(rule({ late: 'new-window' }));
  aggregator.add(event('tests', 't1'), { now: 0 });
  aggregator.add(event('logs', 'l1'), { now: 1 });
  assert.equal(aggregator.add(event('tests', 't2'), { now: 2 }).status, 'pending');
  assert.equal(aggregator.add(event('logs', 'l1'), { now: 3 }).status, 'duplicate');
  const second = aggregator.add(event('logs', 'l2'), { now: 4 });
  assert.equal(second.status, 'complete');
  assert.deepEqual(second.aggregate.events.map(item => item.eventId), ['t2', 'l2']);
});

test('capacity applies to evidence and retained tombstones, with atomic rejection', () => {
  const aggregator = createAggregator(rule({ maxGroups: 1, maxBytes: 1500 }));
  aggregator.add(event('tests', 't'), { now: 0 });
  assert.throws(() => aggregator.add(event('logs', 'huge', 'task', 'x'.repeat(3000)), { now: 1 }), { code: 'AGGREGATION_CAPACITY' });
  assert.equal(aggregator.inspect().events, 1);
  aggregator.add(event('logs', 'l'), { now: 2 });
  assert.throws(() => aggregator.add(event('tests', 'other', 'second'), { now: 3 }), { code: 'AGGREGATION_CAPACITY' });
  aggregator.flush({ now: 102 });
  assert.equal(aggregator.add(event('tests', 'other', 'second'), { now: 103 }).status, 'pending');
});

test('failed admission cannot lose a pending expiry outcome', () => {
  const aggregator = createAggregator(rule({ maxGroups: 1, incomplete: 'emit' }));
  aggregator.add(event('tests', 't'), { now: 0 });
  assert.throws(() => aggregator.add(event('tests', 'other', 'second'), { now: 100 }), { code: 'AGGREGATION_CAPACITY' });
  const expired = aggregator.flush({ now: 100 });
  assert.equal(expired.length, 1);
  assert.equal(expired[0].events[0].eventId, 't');
});

test('aggregation snapshots evidence and close reports unfinished groups exactly once', () => {
  const aggregator = createAggregator(rule());
  const input = event('tests', 't');
  aggregator.add(input, { now: 0 });
  input.event.value = 'mutated';
  const outcomes = aggregator.close({ now: 10 });
  assert.equal(outcomes[0].status, 'discarded');
  assert.equal(outcomes[0].events[0].event.value, 't');
  assert.equal(Object.isFrozen(outcomes[0].events[0].event), true);
  assert.equal(aggregator.inspect().events, 0);
  assert.deepEqual(aggregator.close(), []);
  assert.throws(() => aggregator.add(event('logs', 'l')), { code: 'AGGREGATOR_CLOSED' });
});

test('configuration rejects implicit membership, ambiguous keys and invalid bounded behavior', () => {
  for (const invalid of [{ ...rule(), sources: [] }, { ...rule(), sources: ['same', 'same'] },
    { ...rule(), key: '$' }, { ...rule(), windowMs: 0 }, { ...rule(), maxEvents: 1 },
    { ...rule(), late: 'silently-merge' }, { ...rule(), maxGroups: Infinity }]) {
    assert.throws(() => validateAggregationRule(invalid));
  }
});
