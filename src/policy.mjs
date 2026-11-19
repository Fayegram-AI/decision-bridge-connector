import { assert, freeze, getPath, identifier, isRecord, keysOnly, snapshot, stableStringify } from './util.mjs';
import { project, validatePath, validateProjection } from './mapping.mjs';

const own = (value, key) => Object.hasOwn(value, key);
const probability = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1;
const finite = value => typeof value === 'number' && Number.isFinite(value);
const compare = (value, condition) => (!own(condition, 'min') || value >= condition.min) &&
  (!own(condition, 'max') || value <= condition.max);
const requireResult = (condition, message) => assert(condition, message, 'POLICY_INVALID_RESULT');

function limits(condition, { unit = false, nonnegative = false } = {}) {
  for (const key of ['min', 'max']) if (own(condition, key)) {
    assert(finite(condition[key]) && (!unit || probability(condition[key])) && (!nonnegative || condition[key] >= 0),
      `${key} must be ${unit ? 'a probability' : nonnegative ? 'a nonnegative finite number' : 'a finite number'}.`);
  }
  assert(!own(condition, 'min') || !own(condition, 'max') || condition.min <= condition.max, 'min must not exceed max.');
}

function boxCoordinates(value, label, code = 'CONFIGURATION_ERROR') {
  assert(isRecord(value) && Object.keys(value).length === 4 && ['top', 'bottom', 'left', 'right'].every(key => own(value, key)),
    `${label} requires top, bottom, left and right.`, code);
  assert(Object.values(value).every(probability) && value.top < value.bottom && value.left < value.right,
    `${label} must enclose a positive area with coordinates in [0, 1].`, code);
}

/** Conditions are a bounded data language; no functions or executable expressions. */
export function validatePolicy(policy) {
  const configured = snapshot(policy);
  keysOnly(configured, ['id', 'revision', 'accept', 'review', 'abstain', 'otherwise', 'outcome', 'allowedProbabilitySources'], 'Policy');
  if (own(configured, 'id')) identifier(configured.id, 'Policy id');
  identifier(configured.revision, 'Policy revision');
  assert(!own(configured, 'otherwise') || ['review', 'abstained'].includes(configured.otherwise), 'Policy otherwise must be review or abstained.');
  if (own(configured, 'allowedProbabilitySources')) {
    assert(Array.isArray(configured.allowedProbabilitySources) && configured.allowedProbabilitySources.length > 0 &&
      configured.allowedProbabilitySources.length <= 64, 'allowedProbabilitySources must be a nonempty bounded list.');
    configured.allowedProbabilitySources.forEach(value => identifier(value, 'Probability source'));
    assert(new Set(configured.allowedProbabilitySources).size === configured.allowedProbabilitySources.length, 'Probability sources must be unique.');
  }
  if (own(configured, 'outcome')) validateProjection(configured.outcome);
  let nodes = 0;
  function condition(value, depth) {
    assert(++nodes <= 1024 && depth <= 32, 'Policy exceeds its condition limit.', 'RESOURCE_LIMIT');
    assert(isRecord(value), 'Policy conditions must be objects.');
    for (const operator of ['all', 'any', 'not']) if (own(value, operator)) {
      keysOnly(value, [operator], 'Logical condition');
      if (operator === 'not') condition(value.not, depth + 1);
      else {
        assert(Array.isArray(value[operator]) && value[operator].length > 0, `${operator} requires at least one condition.`);
        value[operator].forEach(child => condition(child, depth + 1));
      }
      return;
    }
    if (own(value, 'path')) {
      keysOnly(value, ['path', 'equals', 'min', 'max', 'missing'], 'Path condition');
      validatePath(value.path);
      assert(['equals', 'min', 'max'].some(key => own(value, key)), 'A path condition requires an actual comparison.');
      assert(!own(value, 'missing') || value.missing === 'unmatched', 'A missing-path policy must be unmatched.');
      limits(value);
      return;
    }
    identifier(value.question, 'Policy question');
    const base = ['question', 'type'];
    if (value.type === 'choice') {
      keysOnly(value, [...base, 'value', 'minProbability', 'minMargin', 'abstainOptions'], 'Choice condition');
      assert(['value', 'minProbability', 'minMargin', 'abstainOptions'].some(key => own(value, key)), 'A choice condition requires an actual constraint.');
      if (own(value, 'value')) assert(typeof value.value === 'string' || typeof value.value === 'boolean', 'A choice value must be a string or boolean.');
      for (const key of ['minProbability', 'minMargin']) if (own(value, key)) assert(probability(value[key]), `${key} must be in [0, 1].`);
      if (own(value, 'abstainOptions')) {
        assert(Array.isArray(value.abstainOptions) && value.abstainOptions.length > 0 &&
          value.abstainOptions.every(item => typeof item === 'string' || typeof item === 'boolean'), 'abstainOptions must be a nonempty list of choice values.');
        assert(new Set(value.abstainOptions).size === value.abstainOptions.length, 'abstainOptions must be unique.');
      }
    } else if (value.type === 'noul') {
      keysOnly(value, [...base, 'min', 'max'], 'Noul condition');
      assert(own(value, 'min') || own(value, 'max'), 'A noul condition requires a bound.');
      limits(value, { unit: true });
    } else if (value.type === 'score') {
      keysOnly(value, [...base, 'min', 'max', 'minMass', 'atLeastLevel'], 'Score condition');
      assert(own(value, 'min') || own(value, 'max') || own(value, 'minMass'), 'A score condition requires a bound or probability-mass threshold.');
      limits(value, { nonnegative: true });
      assert(own(value, 'minMass') === own(value, 'atLeastLevel'), 'minMass and atLeastLevel must be supplied together.');
      if (own(value, 'minMass')) {
        assert(probability(value.minMass), 'minMass must be in [0, 1].');
        assert((Number.isSafeInteger(value.atLeastLevel) && value.atLeastLevel >= 0) ||
          (typeof value.atLeastLevel === 'string' && value.atLeastLevel.length > 0), 'atLeastLevel must be a nonnegative index or an explicit rubric label.');
      }
    } else if (value.type === 'box') {
      keysOnly(value, [...base, 'minConfidence', 'region', 'relation'], 'Box condition');
      assert(own(value, 'minConfidence') || own(value, 'region'), 'A box condition requires a confidence or geometry constraint.');
      if (own(value, 'minConfidence')) assert(probability(value.minConfidence), 'minConfidence must be in [0, 1].');
      if (own(value, 'region')) boxCoordinates(value.region, 'Policy region');
      if (own(value, 'relation')) assert(own(value, 'region') && ['inside', 'intersects'].includes(value.relation), 'A box relation requires a region and must be inside or intersects.');
    } else assert(false, 'Unknown question policy type.');
  }
  for (const rule of ['abstain', 'review', 'accept']) if (own(configured, rule)) condition(configured[rule], 0);
  return freeze(configured);
}

function readChoice(answer) {
  const entries = [];
  if (Array.isArray(answer.probabilities)) {
    for (const item of answer.probabilities) {
      requireResult(isRecord(item) && Object.keys(item).length === 2 && own(item, 'value') && own(item, 'probability'), 'Malformed typed choice probability.');
      requireResult(typeof item.value === 'string' || typeof item.value === 'boolean', 'Typed choice values must remain strings or booleans.');
      entries.push([item.value, item.probability]);
    }
  } else {
    requireResult(isRecord(answer.probabilities), 'A choice requires its complete probability distribution.');
    entries.push(...Object.entries(answer.probabilities));
  }
  const distribution = new Map(entries);
  requireResult(entries.length > 0 && entries.length === distribution.size && entries.every(([, value]) => probability(value)), 'Invalid choice probabilities.');
  requireResult(distribution.has(answer.choice), 'Selected choice is missing from the distribution.');
  const selectedProbability = distribution.get(answer.choice);
  const runnerUp = entries.reduce((highest, [value, probability]) => value === answer.choice ? highest : Math.max(highest, probability), 0);
  requireResult(selectedProbability + 1e-6 >= runnerUp, 'The selected choice must be maximal.');
  return { distribution, selectedProbability, margin: selectedProbability - runnerUp };
}

function readScore(answer) {
  requireResult(isRecord(answer.probabilities) && isRecord(answer.legend), 'A score requires its probabilities and rubric legend.');
  const count = Object.keys(answer.legend).length;
  requireResult(count >= 2 && Object.keys(answer.probabilities).length === count, 'Score probabilities and rubric differ.');
  const levels = Array.from({ length: count }, (_, index) => String(index));
  requireResult(levels.every(key => own(answer.legend, key) && own(answer.probabilities, key) && probability(answer.probabilities[key])), 'Score levels must be contiguous indices with probabilities.');
  requireResult(finite(answer.score) && answer.score >= 0 && answer.score <= count - 1, 'Score is outside its rubric range.');
  const mean = levels.reduce((sum, key) => sum + Number(key) * answer.probabilities[key], 0);
  requireResult(Math.abs(mean - answer.score) <= 0.02 * count, 'Score is inconsistent with its probability-weighted mean.');
  return { count, levels };
}

function decisionProvenance(result) {
  requireResult(isRecord(result) && isRecord(result.answers) && Object.keys(result.answers).length > 0, 'Decision policies require a complete decision result.');
  requireResult(isRecord(result.meta) && ['decision', 'generative'].includes(result.meta.providerKind), 'Decision result providerKind is missing or invalid.');
  requireResult(typeof result.meta.probabilitySource === 'string' && result.meta.probabilitySource.trim().length > 0, 'Decision probability provenance is missing.');
  if (result.meta.providerKind === 'generative') {
    requireResult(result.meta.probabilitySource === 'elicited' &&
      (result.meta.confidenceSource === undefined || result.meta.confidenceSource === 'unavailable'), 'Generative results must retain elicited probability provenance.');
    requireResult(Object.values(result.answers).every(answer => isRecord(answer) && !own(answer, 'confidence')), 'Generative answers cannot contain native confidence.');
  }
}

/** Apply application policy to a result already validated by its operation adapter. */
export function applyPolicy(result, policy, options = {}) {
  const call = snapshot(options);
  keysOnly(call, ['operation', 'correlation', 'questionRevision'], 'Policy call options');
  if (own(call, 'questionRevision')) identifier(call.questionRevision, 'Question revision');
  const operation = call.operation === undefined ? 'decision' : call.operation;
  assert(['decision', 'extraction', 'live'].includes(operation), 'Unsupported policy operation.', 'POLICY_TYPE_MISMATCH');
  const configured = validatePolicy(policy);
  const source = snapshot(result);
  requireResult(isRecord(source), 'Policy input must be an operation result object.');
  if (operation === 'decision') decisionProvenance(source);
  else assert(!own(configured, 'allowedProbabilitySources'), 'Probability-source eligibility is available only for decision results.', 'POLICY_TYPE_MISMATCH');

  const base = { ...(own(configured, 'id') ? { policyId: configured.id } : {}), policyRevision: configured.revision,
    ...(own(call, 'correlation') ? { correlation: call.correlation } : {}),
    ...(own(call, 'questionRevision') ? { questionRevision: call.questionRevision } : {}) };
  // An explicitly ineligible source cannot authorize an action. Native-only
  // predicates may require data that an excluded generative result cannot supply.
  if (configured.allowedProbabilitySources && !configured.allowedProbabilitySources.includes(source.meta.probabilitySource)) {
    return freeze({ ...base, status: 'abstained', reasons: [{ rule: 'eligibility', matched: false,
      code: 'probability_source_not_allowed', probabilitySource: source.meta.probabilitySource }] });
  }

  function evaluate(condition) {
    for (const operator of ['all', 'any', 'not']) if (own(condition, operator)) {
      // Evaluate every referenced condition: an invalid/missing answer must never hide behind short circuiting.
      const checks = operator === 'not' ? [evaluate(condition.not)] : condition[operator].map(evaluate);
      const matched = operator === 'all' ? checks.every(check => check.matched) : operator === 'any'
        ? checks.some(check => check.matched) : !checks[0].matched;
      return { type: operator, matched, checks };
    }
    if (own(condition, 'path')) {
      const value = getPath(source, condition.path);
      if (value === undefined && condition.missing === 'unmatched') {
        return { type: 'path', path: condition.path, matched: false, missing: true };
      }
      assert(value !== undefined, `Policy path is missing: ${condition.path}`, 'POLICY_MISSING_PATH');
      if (own(condition, 'min') || own(condition, 'max')) assert(finite(value), 'A numeric path predicate requires a finite number.', 'POLICY_TYPE_MISMATCH');
      return { type: 'path', path: condition.path, matched: (!own(condition, 'equals') || stableStringify(value) === stableStringify(condition.equals)) && compare(value, condition) };
    }
    assert(operation === 'decision', 'Question predicates require decision results; use explicit paths for extraction and Live.', 'POLICY_TYPE_MISMATCH');
    assert(own(source.answers, condition.question), `Policy question is missing: ${condition.question}`, 'POLICY_MISSING_QUESTION');
    const answer = source.answers[condition.question];
    assert(isRecord(answer) && answer.type === condition.type, `Policy question type does not match: ${condition.question}`, 'POLICY_TYPE_MISMATCH');
    const details = { type: condition.type, question: condition.question };
    if (answer.type === 'choice') {
      const { distribution, selectedProbability, margin } = readChoice(answer);
      if (own(condition, 'value')) assert(distribution.has(condition.value), 'Policy choice value is not an available option.', 'POLICY_TYPE_MISMATCH');
      if (condition.abstainOptions) assert(condition.abstainOptions.every(value => distribution.has(value)), 'Policy abstention option is not an available choice.', 'POLICY_TYPE_MISMATCH');
      return { ...details, matched: (!own(condition, 'value') || answer.choice === condition.value) &&
        (!own(condition, 'minProbability') || selectedProbability >= condition.minProbability) &&
        (!own(condition, 'minMargin') || margin >= condition.minMargin) &&
        (!condition.abstainOptions || !condition.abstainOptions.includes(answer.choice)),
      choice: answer.choice, selectedProbability, margin };
    }
    if (answer.type === 'noul') {
      requireResult(probability(answer.noul), 'Noul must be a probability.');
      return { ...details, matched: compare(answer.noul, condition), probability: answer.noul };
    }
    if (answer.type === 'score') {
      const { count, levels } = readScore(answer);
      for (const key of ['min', 'max']) if (own(condition, key)) assert(condition[key] <= count - 1, 'Score policy threshold exceeds the rubric range.', 'POLICY_TYPE_MISMATCH');
      let mass;
      let level;
      if (own(condition, 'atLeastLevel')) {
        if (typeof condition.atLeastLevel === 'number') level = condition.atLeastLevel;
        else {
          const matching = levels.filter(key => isRecord(answer.legend[key]) && answer.legend[key].label === condition.atLeastLevel);
          assert(matching.length === 1, 'A score policy label must identify exactly one labelled rubric level.', 'POLICY_TYPE_MISMATCH');
          level = Number(matching[0]);
        }
        assert(level < count, 'Score policy level is outside the rubric.', 'POLICY_TYPE_MISMATCH');
        mass = levels.slice(level).reduce((sum, key) => sum + answer.probabilities[key], 0);
      }
      return { ...details, matched: compare(answer.score, condition) && (mass === undefined || mass >= condition.minMass),
        score: answer.score, ...(mass === undefined ? {} : { atLeastLevel: level, mass }) };
    }
    boxCoordinates(answer.box, 'Result box', 'POLICY_INVALID_RESULT');
    if (own(answer, 'confidence')) requireResult(probability(answer.confidence), 'Box confidence must be in [0, 1].');
    if (own(condition, 'minConfidence')) assert(own(answer, 'confidence'), 'This box result does not provide the confidence required by policy.', 'POLICY_TYPE_MISMATCH');
    let spatial = true;
    if (condition.region) {
      const region = condition.region;
      const box = answer.box;
      spatial = (condition.relation ?? 'inside') === 'inside'
        ? box.top >= region.top && box.bottom <= region.bottom && box.left >= region.left && box.right <= region.right
        : Math.max(box.left, region.left) < Math.min(box.right, region.right) && Math.max(box.top, region.top) < Math.min(box.bottom, region.bottom);
    }
    return { ...details, matched: spatial && (!own(condition, 'minConfidence') || answer.confidence >= condition.minConfidence),
      ...(condition.region ? { spatial } : {}), ...(own(answer, 'confidence') ? { confidence: answer.confidence } : {}) };
  }

  // Type/schema compatibility is checked for every rule, independent of precedence.
  const rules = ['abstain', 'review', 'accept'].filter(rule => own(configured, rule));
  const evaluated = rules.map(rule => ({ rule, ...evaluate(configured[rule]) }));
  const selected = evaluated.find(rule => rule.matched);
  const status = selected ? ({ abstain: 'abstained', review: 'review', accept: 'accepted' })[selected.rule]
    : configured.otherwise ?? 'abstained';
  const reasons = [...evaluated, { rule: selected?.rule ?? 'otherwise', code: selected ? 'rule_matched' : 'no_rule_matched', status }];
  return freeze({ ...base, status, reasons,
    ...(status === 'accepted' && own(configured, 'outcome') ? { outcome: project(source, configured.outcome) } : {}) });
}
