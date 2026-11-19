import test from 'node:test';
import assert from 'node:assert/strict';
import { snapshot, validateSignal } from '../src/util.mjs';
import { createRunner } from './helpers/runner.mjs';

test('JSON byte budgets account for actual UTF-8, escapes, surrogate pairs and JSON syntax', () => {
  const values = ['ascii', 'a"\\\n\u0000', 'é中😀', '\ud800', '\udc00',
    { nested: [true, false, null, -0, 1e21, 1e-7, '😀'] }, { 'é\n': 'value' }];
  for (const value of values) {
    const bytes = Buffer.byteLength(JSON.stringify(value));
    assert.deepEqual(snapshot(value, { maxBytes: bytes }), value);
    assert.throws(() => snapshot(value, { maxBytes: bytes - 1 }), { code: 'RESOURCE_LIMIT' });
  }
});

test('aliased strings are counted per serialized occurrence before allocating amplified JSON output', () => {
  const shared = 'x'.repeat(8192);
  assert.throws(() => snapshot(Array(10000).fill(shared)), { code: 'RESOURCE_LIMIT' });
  assert.throws(() => snapshot({ left: shared, right: shared }, { maxBytes: 9000 }), { code: 'RESOURCE_LIMIT' });
});

test('AbortSignal lookalikes are rejected before creating a persisted task', async () => {
  const lookalike = { aborted: false, addEventListener() {}, removeEventListener() {} };
  assert.throws(() => validateSignal(lookalike), { code: 'CONFIGURATION_ERROR' });
  const runner = createRunner({ evaluators: { main: { operation: 'decision', revision: '1', execute: () => ({}) } }, flows: { test: { revision: '1', inputs: ['request'], evaluator: 'main' } } });
  try {
    await assert.rejects(runner.run({ flow: 'test', kind: 'request', request: {} }, { signal: lookalike }), { code: 'CONFIGURATION_ERROR' });
    assert.deepEqual(await runner.list(), { items: [], next: null });
  } finally { await runner.close(); }
});
