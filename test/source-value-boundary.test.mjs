import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createIterableSource, consumeSource } from '../src/index.mjs';

test('iterable consumption preserves independent application-value depth limits', async () => {
    let value = 1; for (let depth = 0; depth < 100; depth++) value = { value };
    const input = { flow: 'a', kind: 'result', result: value, validation: value };
    const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
        flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
    const outcomes = [];
    try {
        assert.equal((await runner.run(input)).status, 'completed');
        const result = await consumeSource(runner, createIterableSource([{ input }], { name: 'deep-values' }),
            { onOutcome: outcome => outcomes.push(outcome) });
        assert.equal(result.admitted, 1); assert.equal(result.completed, 1);
        assert.equal(outcomes[0].error, undefined);
        const invalid = { ...input, result: { value } };
        const rejected = await consumeSource(runner, createIterableSource([{ input: invalid }], { name: 'too-deep' }));
        assert.equal(rejected.admitted, 0); assert.equal(rejected.failed, 1);
        assert.equal((await runner.list()).items.length, 2);
    } finally { await runner.close(); }
});
