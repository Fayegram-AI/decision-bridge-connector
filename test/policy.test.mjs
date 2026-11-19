import test from 'node:test';
import assert from 'node:assert/strict';
import { validateResult } from '@fayegram-ai/decision-bridge';
import { applyPolicy, validatePolicy } from '../src/policy.mjs';

const result = answers => ({ model: 'fixture', answers, usage: { input_tokens: null, output_tokens: null },
  meta: { providerKind: 'decision', probabilitySource: 'native', bridgeVersion: '0.1.5', schemaVersion: 1 } });
const choice = (selected = 'action', probability = 0.9) => ({ type: 'choice', choice: selected,
  probabilities: selected === 'action' ? { action: probability, unknown: 1 - probability }
    : { action: 1 - probability, unknown: probability }, confidence: 0.1 });

test('choice policy uses probability and margin, preserves raw answers, and projects accepted outcome', () => {
  const raw = result({ route: choice() });
  const policy = { id: 'route', revision: '2', accept: { question: 'route', type: 'choice', minProbability: 0.85,
    minMargin: 0.7, abstainOptions: ['unknown'] }, otherwise: 'review', outcome: { $path: 'answers.route.choice' } };
  const accepted = applyPolicy(raw, policy);
  assert.equal(accepted.status, 'accepted');
  assert.equal(accepted.outcome, 'action');
  assert.equal(accepted.policyId, 'route');
  assert.equal(accepted.policyRevision, '2');
  assert.equal(accepted.reasons[0].selectedProbability, 0.9);
  assert.equal(raw.answers.route.confidence, 0.1);
  assert.equal(Object.isFrozen(raw), false);
  assert.equal(Object.isFrozen(accepted.reasons[0]), true);
  assert.equal(applyPolicy(result({ route: choice('action', 0.7) }), policy).status, 'review');
});

test('explicit abstention precedes review and acceptance; no rule implies acceptance', () => {
  const raw = result({ route: choice('unknown'), yes: { type: 'noul', noul: 0.9 } });
  const decision = applyPolicy(raw, { revision: '1', abstain: { question: 'route', type: 'choice', value: 'unknown' },
    review: { question: 'yes', type: 'noul', min: 0.8 }, accept: { question: 'yes', type: 'noul', min: 0.8 }, outcome: 'act' });
  assert.equal(decision.status, 'abstained');
  assert.equal(Object.hasOwn(decision, 'outcome'), false);
  assert.equal(applyPolicy(raw, { revision: '1' }).status, 'abstained');
  assert.equal(applyPolicy(raw, { revision: '1', otherwise: 'review' }).status, 'review');
});

test('typed choices retain boolean/string identity', () => {
  const raw = result({ route: { type: 'choice', choice: true,
    probabilities: [{ value: true, probability: 0.9 }, { value: 'true', probability: 0.1 }] } });
  assert.equal(applyPolicy(raw, { revision: '1', accept: { question: 'route', type: 'choice', value: true, minProbability: 0.8 } }).status, 'accepted');
  assert.equal(applyPolicy(raw, { revision: '1', accept: { question: 'route', type: 'choice', value: 'true' } }).status, 'abstained');
  assert.throws(() => applyPolicy(raw, { revision: '1', accept: { question: 'route', type: 'choice', abstainOptions: [false] } }), { code: 'POLICY_TYPE_MISMATCH' });
});

test('noul bands accept either sufficiently established answer with an intermediate review band', () => {
  const policy = { revision: '1', accept: { any: [
    { question: 'present', type: 'noul', max: 0.1 }, { question: 'present', type: 'noul', min: 0.9 },
  ] }, otherwise: 'review' };
  for (const noul of [0, 0.1, 0.9, 1]) assert.equal(applyPolicy(result({ present: { type: 'noul', noul } }), policy).status, 'accepted');
  assert.equal(applyPolicy(result({ present: { type: 'noul', noul: 0.5 } }), policy).status, 'review');
});

test('scores use rubric indices and explicit distribution mass, including labelled levels', () => {
  const raw = result({ quality: { type: 'score', score: 1.6, probabilities: { 0: 0.1, 1: 0.2, 2: 0.7 },
    legend: { 0: { label: 'poor' }, 1: { label: 'adequate' }, 2: { label: 'excellent' } } } });
  const condition = { question: 'quality', type: 'score', min: 1.5, minMass: 0.65, atLeastLevel: 'excellent' };
  assert.equal(applyPolicy(raw, { revision: '1', accept: condition }).status, 'accepted');
  assert.equal(applyPolicy(raw, { revision: '1', accept: { ...condition, minMass: 0.8 } }).status, 'abstained');
  assert.equal(applyPolicy(raw, { revision: '1', accept: { ...condition, atLeastLevel: 1, minMass: 0.85 } }).status, 'accepted');
  assert.throws(() => applyPolicy(raw, { revision: '1', accept: { ...condition, atLeastLevel: 'invented' } }), { code: 'POLICY_TYPE_MISMATCH' });
  assert.throws(() => applyPolicy(raw, { revision: '1', accept: { question: 'quality', type: 'score', min: 10 } }), { code: 'POLICY_TYPE_MISMATCH' });
});

test('box policies combine presence and geometry; mere box existence does not establish presence', () => {
  const answers = { present: { type: 'noul', noul: 0.1 }, target: { type: 'box',
    box: { top: 0.2, bottom: 0.4, left: 0.3, right: 0.5 }, confidence: 0.95 } };
  const policy = { revision: '1', accept: { all: [{ question: 'present', type: 'noul', min: 0.9 },
    { question: 'target', type: 'box', minConfidence: 0.9, region: { top: 0, bottom: 0.5, left: 0, right: 1 }, relation: 'inside' }] } };
  assert.equal(applyPolicy(result(answers), policy).status, 'abstained');
  answers.present.noul = 0.99;
  assert.equal(applyPolicy(result(answers), policy).status, 'accepted');
  delete answers.target.confidence;
  assert.throws(() => applyPolicy(result(answers), policy), { code: 'POLICY_TYPE_MISMATCH' });
});

test('all referenced questions and types are checked, including unselected branches', () => {
  const raw = result({ yes: { type: 'noul', noul: 1 } });
  assert.throws(() => applyPolicy(raw, { revision: '1', accept: { any: [
    { question: 'yes', type: 'noul', min: 0.5 }, { question: 'missing', type: 'noul', min: 0.5 },
  ] } }), { code: 'POLICY_MISSING_QUESTION' });
  assert.throws(() => applyPolicy(raw, { revision: '1', accept: { question: 'yes', type: 'choice', minProbability: 0.5 } }), { code: 'POLICY_TYPE_MISMATCH' });
  assert.throws(() => applyPolicy(raw, { revision: '1', abstain: { question: 'yes', type: 'noul', min: 0 },
    accept: { question: 'missing', type: 'noul', min: 0 } }), { code: 'POLICY_MISSING_QUESTION' });
});

test('generative provenance is preserved and can be excluded by an explicit policy', () => {
  const raw = result({ route: choice() });
  raw.meta = { providerKind: 'generative', probabilitySource: 'elicited', confidenceSource: 'unavailable' };
  assert.throws(() => applyPolicy(raw, { revision: '1' }), { code: 'POLICY_INVALID_RESULT' });
  delete raw.answers.route.confidence;
  const policy = { revision: '1', accept: { question: 'route', type: 'choice', minProbability: 0.8 }, allowedProbabilitySources: ['native'] };
  const decision = applyPolicy(raw, policy);
  assert.equal(decision.status, 'abstained');
  assert.equal(decision.reasons[0].code, 'probability_source_not_allowed');
  assert.equal(applyPolicy(raw, { ...policy, allowedProbabilitySources: ['elicited'] }).status, 'accepted');
  raw.meta.probabilitySource = 'native';
  assert.throws(() => applyPolicy(raw, policy), { code: 'POLICY_INVALID_RESULT' });
});

test('provenance exclusion abstains before unavailable native-only predicates are evaluated', () => {
  const raw = validateResult({ model: 'fixture', answers: { target: { type: 'box',
    box: { top: 0, left: 0, bottom: 1, right: 1 } } }, usage: { input_tokens: null, output_tokens: null },
  meta: { provider: 'generative-fixture', providerKind: 'generative', probabilitySource: 'elicited', confidenceSource: 'unavailable' } },
  { target: { type: 'box', instructions: 'Find the target.' } });
  const policy = { revision: '1', allowedProbabilitySources: ['native'],
    accept: { question: 'target', type: 'box', minConfidence: 0.9 } };
  const excluded = applyPolicy(raw, policy);
  assert.equal(excluded.status, 'abstained');
  assert.equal(excluded.reasons[0].code, 'probability_source_not_allowed');
  assert.equal(raw.answers.target.confidence, undefined);
  assert.throws(() => applyPolicy(raw, { ...policy, allowedProbabilitySources: ['elicited'] }), { code: 'POLICY_TYPE_MISMATCH' });
  assert.throws(() => applyPolicy(raw, { ...policy, accept: { question: 'target', type: 'box' } }), { code: 'CONFIGURATION_ERROR' });
});

test('policy output retains explicit correlation and question revision through every disposition', () => {
  const raw = result({ ready: { type: 'noul', noul: 0.99 } });
  const correlation = { eventId: 'upstream-event', source: { sessionId: 'live-session' } };
  const options = { correlation, questionRevision: 'question-4' };
  for (const policy of [
    { revision: '1', accept: { question: 'ready', type: 'noul', min: 0.9 } },
    { revision: '1', review: { question: 'ready', type: 'noul', min: 0.9 } },
    { revision: '1', allowedProbabilitySources: ['different-source'] },
  ]) {
    const decision = applyPolicy(raw, policy, options);
    assert.equal(decision.questionRevision, 'question-4');
    assert.deepEqual(decision.correlation, correlation);
    assert.equal(Object.isFrozen(decision.correlation.source), true);
    assert.notEqual(decision.correlation, correlation);
  }
});

test('operation-specific payloads use explicit paths; question predicates cannot cross operation types', () => {
  const extraction = { extraction: { amount: 12, active: false }, meta: { model: 'clairic' } };
  const policy = { revision: '1', accept: { all: [{ path: 'extraction.amount', min: 10 }, { path: 'extraction.active', equals: false }] } };
  assert.equal(applyPolicy(extraction, policy, { operation: 'extraction' }).status, 'accepted');
  assert.throws(() => applyPolicy(extraction, { revision: '1', accept: { question: 'amount', type: 'score', min: 1 } }, { operation: 'extraction' }), { code: 'POLICY_TYPE_MISMATCH' });
  assert.throws(() => applyPolicy(extraction, { revision: '1', accept: { path: 'missing', equals: null } }, { operation: 'extraction' }), { code: 'POLICY_MISSING_PATH' });
  assert.equal(applyPolicy({ event: { type: 'decision.completed' } }, { revision: '1', accept: { path: 'event.type', equals: 'decision.completed' } }, { operation: 'live' }).status, 'accepted');
});

test('optional native fields can be explicitly unmatched without hiding malformed existing values', () => {
  const policy = { revision: '1', abstain: { path: 'notification.decision.answers.0.type', equals: 'refusal' },
    accept: { all: [{ path: 'notification.decision.answers.0.type', equals: 'predicate' },
      { path: 'notification.decision.answers.0.probability', min: 0.95, missing: 'unmatched' }] }, otherwise: 'review' };
  const refusal = { notification: { decision: { answers: [{ type: 'refusal', name: 'ready' }] } } };
  const abstained = applyPolicy(refusal, policy, { operation: 'live' });
  assert.equal(abstained.status, 'abstained');
  assert.equal(abstained.reasons[1].checks[1].matched, false);
  assert.equal(abstained.reasons[1].checks[1].missing, true);
  const accepted = { notification: { decision: { answers: [{ type: 'predicate', name: 'ready', probability: 0.99 }] } } };
  assert.equal(applyPolicy(accepted, policy, { operation: 'live' }).status, 'accepted');
  for (const probability of [null, '0.99']) {
    const malformed = { notification: { decision: { answers: [{ type: 'refusal', name: 'ready', probability }] } } };
    assert.throws(() => applyPolicy(malformed, policy, { operation: 'live' }), { code: 'POLICY_TYPE_MISMATCH' });
  }
  assert.equal(applyPolicy({ extraction: {} }, { revision: '1', accept: { path: 'extraction.optional', min: 1, missing: 'unmatched' } },
    { operation: 'extraction' }).status, 'abstained');
  assert.throws(() => applyPolicy({ extraction: {} }, { revision: '1', accept: { path: 'extraction.optional', min: 1 } },
    { operation: 'extraction' }), { code: 'POLICY_MISSING_PATH' });
  for (const condition of [{ path: 'a', missing: 'unmatched' }, { path: 'a', min: 1, missing: 'accepted' },
    { question: 'route', type: 'choice', minProbability: 0.9, missing: 'unmatched' }]) {
    assert.throws(() => validatePolicy({ revision: '1', accept: condition }), { code: 'CONFIGURATION_ERROR' });
  }
});

test('already validated rounded probability distributions are not revalidated with the wrong precision', () => {
  const probabilities = Object.fromEntries(Array.from({ length: 210 }, (_, index) => [`v${index}`, 0.0048]));
  const raw = result({ category: { type: 'choice', choice: 'v0', probabilities } });
  assert.ok(Math.abs(Object.values(probabilities).reduce((sum, value) => sum + value, 0) - 1) > 0.001);
  assert.equal(applyPolicy(raw, { revision: '1', accept: { question: 'category', type: 'choice', minProbability: 0.004, minMargin: 0 } }).status, 'accepted');
  assert.equal(raw.answers.category.probabilities.v0, 0.0048);
});

test('configuration rejects empty predicates, malformed thresholds and executable data', () => {
  for (const accept of [{ question: 'x', type: 'choice' }, { question: 'x', type: 'noul' },
    { question: 'x', type: 'score', atLeastLevel: 1 }, { question: 'x', type: 'box' },
    { path: 'a' }, { all: [] }, { question: 'x', type: 'noul', min: 0.8, max: 0.2 },
    { question: 'x', type: 'choice', minProbablity: 0.9 }, { question: 'x', type: 'score', minMass: 0.9 }]) {
    assert.throws(() => validatePolicy({ revision: '1', accept }), { code: 'CONFIGURATION_ERROR' });
  }
  assert.throws(() => validatePolicy({ revision: '1', accept: () => true }), { code: 'VALIDATION_ERROR' });
  assert.throws(() => validatePolicy({ revision: '1', otherwise: 'accepted' }), { code: 'CONFIGURATION_ERROR' });
});
