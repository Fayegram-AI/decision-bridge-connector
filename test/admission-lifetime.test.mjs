import test from 'node:test';
import assert from 'node:assert/strict';
import { createMemoryStore, createIterableSource, consumeSource } from '../src/index.mjs';
import { createRunner } from './helpers/runner.mjs';
import { bounded, delay } from '../src/util.mjs';

const input = { flow: 'a', kind: 'result', result: 1 };
const flows = { a: { revision: '1', inputs: ['result'], validator: 'v' } };

for (const mode of ['cancel', 'deadline']) test(`source ${mode} cancels admission validation without a late task`, async () => {
    const entered = Promise.withResolvers(); const release = Promise.withResolvers();
    const controller = new AbortController(); let validationSignal;
    const runner = createRunner({ flows, validators: { v: { revision: '1', async validate(value, { signal }) {
        validationSignal = signal; entered.resolve(); await release.promise; return value;
    } } } });
    const source = createIterableSource([{ input }], { name: 'admission-lifetime' });
    const consuming = consumeSource(runner, source, { signal: controller.signal,
        processingTimeoutMs: mode === 'deadline' ? 50 : 1000 });
    try {
        await bounded(() => entered.promise);
        if (mode === 'cancel') controller.abort();
        const result = await bounded(() => consuming, { timeoutMs: 2000 });
        assert.equal(result.stopped, mode === 'cancel' ? 'cancelled' : 'exhausted');
        assert.equal(result.admitted, 0); assert.equal(validationSignal.aborted, true);
        release.resolve(); await delay(10);
        assert.equal((await runner.list()).items.length, 0);
    } finally { controller.abort(); release.resolve(); await consuming; await runner.close({ cancel: true }); }
});

test('submission rejects invalid and already-aborted signals before validation', async () => {
    let validations = 0;
    const runner = createRunner({ flows, validators: { v: { revision: '1', validate(value) { validations++; return value; } } } });
    try {
        await assert.rejects(runner.submit(input, { signal: {} }), { code: 'CONFIGURATION_ERROR' });
        await assert.rejects(runner.submit(input, { signal: AbortSignal.abort() }), { code: 'ABORTED' });
        assert.equal(validations, 0); assert.equal((await runner.list()).items.length, 0);
    } finally { await runner.close(); }
});

test('cancellation during an in-progress storage admission preserves committed work', async () => {
    const store = createMemoryStore(); const committed = Promise.withResolvers(); const release = Promise.withResolvers();
    const controller = new AbortController();
    const runner = createRunner({ flows, validators: { v: { revision: '1', validate: value => value } },
        store: { ...store, async admit(...args) { const value = await store.admit(...args); committed.resolve(); await release.promise; return value; } } });
    const submitting = runner.submit(input, { signal: controller.signal });
    try {
        await committed.promise; controller.abort(); release.resolve();
        const admitted = await submitting;
        assert.equal((await runner.get(admitted.taskId)).settled, false);
        assert.equal((await runner.execute(admitted.taskId)).status, 'completed');
    } finally { release.resolve(); await submitting; await runner.close(); await store.close(); }
});
