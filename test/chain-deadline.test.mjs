import test from 'node:test';
import assert from 'node:assert/strict';
import { executeChain } from '../src/engine/chaining.mjs';
import { ConnectorError } from '../src/util.mjs';

function fixture(deadlineAt) {
    const writes = [];
    return { writes, task: { id: 'parent', namespace: 'deadline' },
        job: { id: 'chain', kind: 'chain', deadlineAt, admissionAttempts: 0, deliveryId: 'delivery',
            plan: { nextFlow: 'child', chainRetry: { maxAttempts: 2, delayMs: 1, maxDelayMs: 1 } } },
        store: { async findChildAdmission() { return null; },
            async commit(_job, update) { writes.push(update.job); },
            async getJob() { return { id: 'delivery', receipt: { receipt: { response: 42 } } }; } } };
}

test('an owned admission timeout settles when wall time still precedes the saved deadline', async context => {
    const realNow = Date.now; let adjustment = 0; let calls = 0; let admissionSignal;
    context.mock.method(Date, 'now', () => realNow() + adjustment);
    const { task, job, store, writes } = fixture(Date.now() + 100);
    await executeChain(task, job, { store, signal: new AbortController().signal,
        chain(_flow, _response, _parent, _owner, signal) {
            calls++; adjustment = -1000; admissionSignal = signal; return new Promise(() => {});
        } });
    assert.equal(calls, 1); assert.equal(admissionSignal.aborted, true); assert(job.deadlineAt > Date.now());
    assert.equal(writes.at(-1).state, 'expired'); assert.equal(writes.at(-1).error.code, 'CHAIN_ADMISSION_EXPIRED');
    assert.equal(writes.at(-1).token, null); assert.equal(writes.at(-1).leaseUntil, null);
});

test('an early nested admission timeout retains uncertainty for linkage recovery', async () => {
    for (const deadlineAt of [Date.now() + 10000, null]) {
        const { task, job, store, writes } = fixture(deadlineAt);
        const failure = new ConnectorError('TIMEOUT', 'Admission response was lost.');
        await assert.rejects(executeChain(task, job, { store, signal: new AbortController().signal,
            chain() { throw failure; } }), error => error === failure);
        assert.deepEqual(writes, [{ admissionAttempts: 1 }]);
    }
});
