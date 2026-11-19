import test from 'node:test';
import assert from 'node:assert/strict';
import { mapRequest, project, validateProjection, validateTemplate } from '../src/mapping.mjs';

test('projections preserve typed values, arrays, escaped pointer keys and complete payloads', () => {
  const input = { answers: { route: { choice: false } }, 'a/b': { '~name': [1, true] } };
  const result = project(input, {
    action: { $path: 'answers.route.choice' },
    values: [{ $path: '/a~1b/~0name/0' }, { $path: '/a~1b/~0name/1' }],
    retained: { $path: '$' },
  });
  assert.equal(result.action, false);
  assert.deepEqual(result.values, [1, true]);
  assert.deepEqual(result.retained, input);
  input.answers.route.choice = 'changed';
  assert.equal(result.retained.answers.route.choice, false);
  assert.equal(Object.isFrozen(result.retained.answers.route), true);
});

test('missing paths require an explicit default or optional null', () => {
  assert.throws(() => project({}, { $path: 'missing' }), { code: 'MAPPING_MISSING_PATH' });
  assert.deepEqual(project({}, {
    optional: { $path: 'missing', required: false },
    fallback: { $path: 'missing', default: { $path: 'this is literal data' } },
  }), { optional: null, fallback: { $path: 'this is literal data' } });
  assert.equal(project({ present: null }, { $path: 'present', default: 'fallback' }), null);
});

test('literal projections carry dollar-prefixed data without interpreting it', () => {
  const literal = { $path: 'not executed', $unknown: ['value'] };
  assert.deepEqual(project({}, { $literal: literal }), literal);
  assert.throws(() => validateProjection({ $eval: 'process.exit()' }), { code: 'CONFIGURATION_ERROR' });
  assert.throws(() => validateProjection({ $path: 'value', unrelated: true }), { code: 'CONFIGURATION_ERROR' });
  assert.throws(() => validateProjection({ $literal: 1, $path: 'value' }), { code: 'CONFIGURATION_ERROR' });
});

test('invalid paths are rejected before evidence lookup and accessors are never invoked', () => {
  for (const path of ['__proto__.value', '/constructor/prototype', 'a..b', '/bad~2escape']) {
    assert.throws(() => project({}, { $path: path }), { code: 'CONFIGURATION_ERROR' });
  }
  let reads = 0;
  const value = Object.defineProperty({}, 'sensitive', { enumerable: true, get() { reads++; return 'secret'; } });
  assert.throws(() => project(value, { $path: 'sensitive' }), { code: 'VALIDATION_ERROR' });
  assert.equal(reads, 0);
  assert.throws(() => project({}, () => 'run'), { code: 'VALIDATION_ERROR' });
});

test('trusted templates construct requests without adding envelope metadata', () => {
  const template = { id: 'classification', revision: '2026-10', request: {
    state: { log: { $path: 'detail.log' } },
    questions: { $literal: { routed: { type: 'noul', instructions: 'Is routing possible?' } } },
  } };
  const request = mapRequest({ taskId: 'external', detail: { log: 'error' } }, template);
  assert.deepEqual(request, { state: { log: 'error' }, questions: { routed: { type: 'noul', instructions: 'Is routing possible?' } } });
  assert.equal(Object.hasOwn(request, 'taskId'), false);
  assert.equal(Object.isFrozen(request.questions.routed), true);
  assert.throws(() => mapRequest({}, { revision: '1', request: 3 }), { code: 'MAPPING_INVALID_REQUEST' });
  assert.throws(() => validateTemplate({ request: {} }), { code: 'CONFIGURATION_ERROR' });
  assert.throws(() => validateTemplate({ revision: '1', request: {}, executable: 'x' }), { code: 'CONFIGURATION_ERROR' });
});

test('templates retain ordered media and conversation payloads as explicit JSON', () => {
  const input = [{ type: 'image', dataUrl: 'data:image/png;base64,AA==' }, { type: 'text', text: 'Assess.' }];
  const mapped = mapRequest({ input }, { revision: '1', request: {
    input: { $path: 'input' }, questions: { $literal: { present: { type: 'noul', instructions: 'Visible?' } } },
  } });
  assert.deepEqual(mapped.input, input);
  assert.equal(mapped.input[0].type, 'image');
  assert.equal(mapped.input[1].type, 'text');
});
