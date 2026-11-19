import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createCallbackConnector } from '../src/index.mjs';

test('request and event admission require the pinned callable evaluator before validation', async () => {
    for (const kind of ['request', 'event']) {
        let validations = 0;
        const original = { revision: '1', validate(value) { validations++; return value; }, execute: value => value };
        const evaluators = Object.assign(Object.create({ e: original }), { e: original });
        const runner = createRunner({ evaluators, flows: { a: { revision: '1', inputs: [kind], evaluator: 'e',
            ...(kind === 'event' ? { template: { value: '$.value' } } : {}) } } });
        const input = { flow: 'a', kind, [kind]: { value: 1 } };
        try {
            await runner.ready;
            for (const replacement of [undefined, { ...original, revision: '2' },
                { revision: '1', validate: original.validate }, { ...original, validate: 42 }]) {
                if (replacement === undefined) delete evaluators.e;
                else evaluators.e = replacement;
                await assert.rejects(runner.preview(input), { code: 'REVISION_UNAVAILABLE' });
                await assert.rejects(runner.submit(input), { code: 'REVISION_UNAVAILABLE' });
            }
            assert.equal(validations, 0);
            assert.equal((await runner.list()).items.length, 0);
            evaluators.e = original;
            assert.equal((await runner.run(input)).status, 'completed');
            assert.equal(validations, 1);
        } finally { await runner.close(); }
    }
});

test('evaluation dispatch rejects inherited or non-callable registrations before invocation', async () => {
    for (const mode of ['inherited', 'non-callable', 'revision']) {
        let calls = 0;
        const original = { revision: '1', execute() { calls++; return 1; } };
        const evaluators = Object.assign(Object.create({ e: original }), { e: original });
        const runner = createRunner({ evaluators, flows: { a: { revision: '1', inputs: ['request'], evaluator: 'e' } } });
        try {
            const admitted = await runner.submit({ flow: 'a', kind: 'request', request: 1 });
            if (mode === 'inherited') delete evaluators.e;
            else evaluators.e = mode === 'non-callable' ? { revision: '1' } : { ...original, revision: '2' };
            const report = await runner.execute(admitted.taskId);
            assert.equal(report.status, 'failed');
            assert.equal(report.evaluation.status, 'failed');
            assert.equal(calls, 0);
            const record = await runner.store.get(admitted.taskId, runner.namespace);
            assert.equal(record.task.errors[0].code, 'REVISION_UNAVAILABLE');
        } finally { await runner.close(); }
    }
});

test('delivery dispatch holds inherited or non-callable registrations without recipient attempts', async () => {
    for (const mode of ['inherited', 'non-callable', 'revision']) {
        let calls = 0;
        const original = createCallbackConnector(async () => { calls++; return { status: 'acknowledged' }; });
        const connectors = Object.assign(Object.create({ sink: original }), { sink: original });
        const runner = createRunner({ connectors, validators: { v: { revision: '1', validate: value => value } },
            flows: { a: { revision: '1', inputs: ['result'], validator: 'v', connections: [
                { id: 'out', connector: 'sink', when: 'always', payload: 'raw-result' },
            ] } } });
        try {
            const admitted = await runner.submit({ flow: 'a', kind: 'result', result: 1 });
            if (mode === 'inherited') delete connectors.sink;
            else connectors.sink = mode === 'non-callable' ? { revision: '1' } : { ...original, revision: '2' };
            const report = await runner.execute(admitted.taskId);
            assert.equal(report.deliveryCounts.held, 1);
            assert.equal(calls, 0);
            const record = await runner.store.get(admitted.taskId, runner.namespace);
            const delivery = record.jobs.find(job => job.kind === 'delivery');
            assert.equal(delivery.error.code, 'REVISION_UNAVAILABLE');
            assert.deepEqual(delivery.attempts, []);
            connectors.sink = original;
            assert.equal((await runner.run({ flow: 'a', kind: 'result', result: 2 })).status, 'completed');
            assert.equal(calls, 1);
        } finally { await runner.close(); }
    }
});
