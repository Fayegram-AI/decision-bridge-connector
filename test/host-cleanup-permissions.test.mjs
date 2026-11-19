import test from 'node:test';
import assert from 'node:assert/strict';
import { createRunner, createMemoryStore } from '../src/index.mjs';
import { createHost } from '../src/node/host.mjs';

const configuration = { validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } };

for (const cancelled of [false, true]) test(`host startup preserves ${cancelled ? 'cancellation' : 'failure'} and every cleanup failure`, async () => {
    const runner = createRunner(configuration); const controller = new AbortController();
    const primary = new Error('Fixture startup'); const cleanup = new Error('Fixture private cleanup'); let closes = 0;
    try {
        await assert.rejects(createHost({ runner, role: 'ingress', signal: controller.signal, sources: {
            first: { start() {}, close() { closes++; throw cleanup; } },
            second: { start() { if (cancelled) { controller.abort(); return new Promise(() => {}); } throw primary; }, close() { closes++; } },
        } }), error => {
            assert(error instanceof AggregateError); assert.equal(error.errors.length, 2);
            assert.equal(cancelled ? error.cause.code : error.cause, cancelled ? 'ABORTED' : primary);
            assert(error.errors[1] instanceof AggregateError); assert.deepEqual(error.errors[1].errors, [cleanup]);
            return true;
        });
        assert.equal(closes, 2);
    } finally { await runner.close(); }
});

for (const kind of ['snapshot', 'import']) test(`stopped hosted sources release ${kind} leases while new work stays fenced`, async () => {
    const store = createMemoryStore({ maxTransfers: 1 }); const runner = createRunner({ ...configuration, store });
    const task = await runner.submit({ flow: 'a', kind: 'result', result: 42 });
    const opened = Promise.withResolvers(); let guarded; let lease; let closes = 0;
    const source = { async start({ runner, whenActive }) {
        guarded = runner;
        assert.throws(() => guarded.closeSnapshot('staged'), { code: 'HOST_NOT_ACTIVE' });
        void whenActive.then(async () => {
            lease = kind === 'snapshot' ? await runner.openSnapshot(task.taskId) : await runner.openImport({ flow: 'a' });
            opened.resolve();
        }).catch(opened.reject);
    }, async close({ signal, timeoutMs }) {
        closes++; assert(signal instanceof AbortSignal); assert.equal(signal.aborted, false); assert.equal(timeoutMs, 1000);
        if (kind === 'snapshot') await guarded.closeSnapshot(lease.id);
        else await guarded.store.closeImport(guarded.namespace, lease.id);
    } };
    const host = await createHost({ runner, role: 'ingress', sources: { source }, drainMs: 1000 });
    try {
        await opened.promise; await host.stopIngress(); assert.equal(closes, 1);
        await assert.rejects(async () => guarded.submit({ flow: 'a', kind: 'result', result: 1 }), { code: 'HOST_NOT_ACTIVE' });
        await assert.rejects(async () => guarded.store.openImport(runner.namespace, 'a', 'revision'), { code: 'HOST_NOT_ACTIVE' });
        const replacement = await runner.openSnapshot(task.taskId); await runner.closeSnapshot(replacement.id);
        assert.deepEqual((await host.health()).errors, []);
    } finally { await host.close(); await store.close(); }
});

test('host shutdown reports source failures and timeouts while closing every source once', async () => {
    const runner = createRunner(configuration); const errors = []; let closed = 0;
    const host = await createHost({ runner, role: 'ingress', drainMs: 25, onError: error => { errors.push(error); }, sources: {
        failed: { async start() {}, close() { closed++; throw new Error('Private close detail'); } },
        stalled: { async start() {}, close() { closed++; return new Promise(() => {}); } },
        good: { async start() {}, close() { closed++; } },
    } });
    const results = await Promise.allSettled([host.close(), host.close()]);
    assert(results.every(result => result.status === 'rejected' && result.reason instanceof AggregateError));
    assert.equal(results[0].reason, results[1].reason);
    assert.equal(closed, 3); assert.equal(errors.length, 2);
    assert.equal(errors.find(error => error.stage === 'source-close:failed').code, 'UNEXPECTED_ERROR');
    assert.equal(errors.find(error => error.stage === 'source-close:stalled').code, 'TIMEOUT');
    assert.equal(JSON.stringify(errors).includes('Private close detail'), false);
});
